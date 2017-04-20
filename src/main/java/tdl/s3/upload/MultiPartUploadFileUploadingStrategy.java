package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import tdl.s3.util.SystemUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiPartUploadFileUploadingStrategy implements UploadingStrategy {

    //Minimum part size is 5 MB
    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;
    private static final long MAX_UPLOADING_TIME = 60;
    private final MultipartUpload upload;

    private MessageDigest md5Digest;

    private String uploadId;

    private List<PartETag> tags;

    private long uploadedSize;

    private int nextPartToUploadIndex;

    private boolean writingFinished;

    private ExecutorService executorService;

    /**
     * Creates new Multipart upload strategy
     *
     * @param upload       {@link MultipartUpload} object that represents already started uploading or null if it should be clean upload
     * @param threadsCount count of threads that should be used for uploading
     */
    public MultiPartUploadFileUploadingStrategy(MultipartUpload upload, int threadsCount) {
        if (threadsCount < 1) throw new IllegalArgumentException("Thread count should be >= 1");
        executorService = Executors.newFixedThreadPool(threadsCount);
        this.upload = upload;
        try {
            md5Digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Can't send multipart upload. Can't create MD5 digest. " + e.getMessage(), e);
        }
    }

    @Override
    public void upload(AmazonS3 s3, String bucket, File file, String newName) throws Exception {
        initStrategy(s3, bucket, file, newName, upload);
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            uploadRequiredParts(s3, bucket, newName, inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error while reading file " + file + ". " + e.getMessage(), e);
        }

    }

    private void initStrategy(AmazonS3 s3, String bucket, File file, String newName, MultipartUpload upload) {
        writingFinished = isWritingFinished(file);
        PartListing alreadyUploadedParts = getAlreadyUploadedParts(s3, bucket, newName, upload);
        boolean uploadingStarted = alreadyUploadedParts != null;
        if (!uploadingStarted) {
            uploadId = initUploading(s3, bucket, newName);
            uploadedSize = 0;
            nextPartToUploadIndex = 1;
        } else {
            uploadId = alreadyUploadedParts.getUploadId();
            uploadedSize = getUploadedSize(alreadyUploadedParts);
            nextPartToUploadIndex = getLastPartIndex(alreadyUploadedParts) + 1;
        }
        tags = getETags(alreadyUploadedParts);
        try {
            if (Files.size(file.toPath()) < uploadedSize) {
                throw new IllegalStateException("Already uploaded size of file " + file + " is greater than actual file size. " +
                        "Probably file was changed and can't be uploaded now.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't read size of file to upload, " + file + ". " + e.getMessage(), e);
        }
    }

    private boolean isWritingFinished(File file) {
        try {
            Path lockFilePath = getLockFilePath(file);
            if (Files.exists(lockFilePath)) {
                if (! writingProcessAlive(lockFilePath)) {
                    Files.delete(lockFilePath);
                    return true;
                }
                return false;
            } else {
                return true;
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't read lock file");
        }
    }

    private boolean writingProcessAlive(Path lockFilePath) throws IOException {
        try (InputStream inputStream = Files.newInputStream(lockFilePath)){
            Scanner scanner = new Scanner(inputStream);
            int pid = scanner.nextInt();
            return SystemUtil.isProcessAlive(pid);
        } catch (IllegalStateException | NoSuchElementException ignored) {
            return false;
        }
    }

    private List<PartETag> getETags(PartListing alreadyUploadedParts) {
        return Optional.ofNullable(alreadyUploadedParts)
                .map(PartListing::getParts)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .map(partSummary -> new PartETag(partSummary.getPartNumber(), partSummary.getETag()))
                .collect(Collectors.toList());
    }

    private Path getLockFilePath(File file) {
        return file.toPath()
                .toAbsolutePath()
                .normalize()
                .getParent()
                .resolve(file.getName() + ".lock");
    }

    private void uploadRequiredParts(AmazonS3 s3, String bucket, String newName, InputStream inputStream) throws IOException {
        try {
            uploadIncompleteParts(s3, bucket, newName, inputStream, writingFinished);
            if (writingFinished) {
                commit(s3, bucket, newName);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("File uploading was terminated.");
        }
    }

    private void commit(AmazonS3 s3, String bucket, String newName) {
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucket, newName, uploadId, tags);
        s3.completeMultipartUpload(request);
    }

    private void uploadIncompleteParts(AmazonS3 s3, String bucket, String newName, InputStream inputStream, boolean uploadLastPart) throws IOException, InterruptedException {
        uploadPartsConcurrent(s3, bucket, newName, inputStream, uploadLastPart).stream()
                .map(this::getUploadingResult)
                .forEach(tags::add);
    }

    private List<Future<PartETag>> uploadPartsConcurrent(AmazonS3 s3, String bucket, String newName, InputStream inputStream, boolean uploadLastPart) throws IOException, InterruptedException {
        List<Future<PartETag>> uploadingResults = new ArrayList<>();


        for (byte[] nextPart = getNextPart(uploadedSize, inputStream, uploadLastPart);
             nextPart.length > 0;
             nextPart = getNextPart(0, inputStream, uploadLastPart)) {
            try (InputStream partInputStream = getInputStream(nextPart)) {
                int partSize = nextPart.length;
                boolean isLastPart = uploadLastPart && partSize < MINIMUM_PART_SIZE;
                String digest = getDigest(nextPart);
                UploadPartRequest request = new UploadPartRequest()
                        .withBucketName(bucket)
                        .withKey(newName)
                        .withPartNumber(nextPartToUploadIndex++)
                        .withMD5Digest(digest)
                        .withLastPart(isLastPart)
                        .withPartSize(partSize)
                        .withUploadId(uploadId)
                        .withInputStream(partInputStream);

                Future<PartETag> uploadingResult = executorService.submit(() -> {
                    try {
                        UploadPartResult result = s3.uploadPart(request);
                        return result.getPartETag();
                    }catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                });
                uploadingResults.add(uploadingResult);
            }
        }

        executorService.shutdown();
        executorService.awaitTermination(MAX_UPLOADING_TIME, TimeUnit.MINUTES);

        return uploadingResults;
    }

    private PartETag getUploadingResult(Future<PartETag> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Some part uploads was unsuccessful. " + e.getMessage(), e);
        }
    }

    private byte[] truncate(byte[] nextPartBytes, int partSize) {
        if (partSize == nextPartBytes.length) return nextPartBytes;
        byte[] result = new byte[partSize];
        System.arraycopy(nextPartBytes, 0, result, 0, partSize);
        return result;
    }

    private InputStream getInputStream(byte[] nextPartBytes) {
        return new ByteArrayInputStream(nextPartBytes, 0, nextPartBytes.length);
    }

    private String getDigest(byte[] nextPartBytes) {
        return Base64.getEncoder().encodeToString(md5Digest.digest(nextPartBytes));
    }

    private byte[] getNextPart(long offset, InputStream inputStream, boolean readLastBytes) throws IOException {
        byte[] buffer = new byte[MINIMUM_PART_SIZE];
        int read = 0;
        skipAlreadyUploadedParts(offset, inputStream);
        int available = inputStream.available();
        if (available < MINIMUM_PART_SIZE && !readLastBytes) return new byte[0];
        while (available > 0) {
            int currentRed = inputStream.read(buffer, read, MINIMUM_PART_SIZE - read);
            read += currentRed;
            available = inputStream.available();
            if (read == MINIMUM_PART_SIZE) break;
        }
        return truncate(buffer, read);
    }

    private void skipAlreadyUploadedParts(long offset, InputStream inputStream) throws IOException {
        long skipped = 0;
        while (skipped < offset) {
            skipped += inputStream.skip(offset);
        }
    }

    private String initUploading(AmazonS3 s3, String bucket, String newName) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, newName);
        InitiateMultipartUploadResult result = s3.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    private long getUploadedSize(PartListing partListing) {
        return partListing.getParts().stream()
                .mapToLong(PartSummary::getSize)
                .sum();
    }

    private int getLastPartIndex(PartListing partListing) {
        return partListing.getParts().stream()
                .mapToInt(PartSummary::getPartNumber)
                .max()
                .orElse(1);
    }

    private PartListing getAlreadyUploadedParts(AmazonS3 s3, String bucket, String key, MultipartUpload upload) {
        return Optional.ofNullable(upload)
                .map(MultipartUpload::getUploadId)
                .map(id -> getPartListing(s3, bucket, key, id))
                .orElse(null);
    }

    private PartListing getPartListing(AmazonS3 s3, String bucket, String key, String uploadId) {
        ListPartsRequest request = new ListPartsRequest(bucket, key, uploadId);
        return s3.listParts(request);
    }

}
