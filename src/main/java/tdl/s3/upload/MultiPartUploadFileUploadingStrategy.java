package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import tdl.s3.helpers.ByteHelper;
import tdl.s3.helpers.FileHelper;
import tdl.s3.helpers.MD5Digest;
import tdl.s3.helpers.MultipartUploadHelper;
import tdl.s3.sync.SyncProgressListener;

public class MultiPartUploadFileUploadingStrategy implements UploadingStrategy {

    //Minimum part size is 5 MB
    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;

    private static final int DEFAULT_THREAD_COUNT = 4;

    private AmazonS3 client;

    private final MultipartUpload upload;

    private String uploadId;

    private List<PartETag> eTags;

    private long uploadedSize;

    private Set<Integer> failedMiddleParts;

    private int nextPartToUploadIndex;

    private boolean writingFinished;

    private ConcurrentMultipartUploader concurrentUploader;

    private SyncProgressListener listener;

    /**
     * Creates new Multipart upload strategy
     *
     * @param upload {@link MultipartUpload} object that represents already
     * started uploading or null if it should be clean upload
     */
    MultiPartUploadFileUploadingStrategy(MultipartUpload upload) {
        this(upload, DEFAULT_THREAD_COUNT);
    }

    /**
     * Creates new Multipart upload strategy
     *
     * @param upload {@link MultipartUpload} object that represents already
     * started uploading or null if it should be clean upload
     * @param threadsCount count of threads that should be used for uploading
     */
    MultiPartUploadFileUploadingStrategy(MultipartUpload upload, int threadsCount) {
        concurrentUploader = new ConcurrentMultipartUploader(threadsCount);
        this.upload = upload;

    }

    @Override
    public void upload(AmazonS3 s3, File file, RemoteFile remoteFile) throws Exception {
        concurrentUploader.setClient(s3); //TODO: Put this in appropriate place
        initStrategy(s3, file, remoteFile, upload);
        uploadRequiredParts(s3, file, remoteFile);
    }

    private void initStrategy(AmazonS3 s3, File file, RemoteFile remoteFile, MultipartUpload upload) {
        writingFinished = !FileHelper.lockFileExists(file);
        PartListing alreadyUploadedParts = getAlreadyUploadedParts(s3, remoteFile, upload);

        boolean uploadingStarted = alreadyUploadedParts != null;
        if (!uploadingStarted) {
            uploadId = initUploading(s3, remoteFile);
            uploadedSize = 0;
            failedMiddleParts = Collections.emptySet();
            nextPartToUploadIndex = 1;
        } else {
            uploadId = alreadyUploadedParts.getUploadId();
            uploadedSize = getUploadedSize(alreadyUploadedParts);
            failedMiddleParts = getFailedMiddlePartNumbers(alreadyUploadedParts);
            nextPartToUploadIndex = getLastPartIndex(alreadyUploadedParts) + 1;
        }
        eTags = MultipartUploadHelper.getPartETagsFromPartListing(alreadyUploadedParts);
        try {
            if (Files.size(file.toPath()) < uploadedSize) {
                throw new IllegalStateException("Already uploaded size of file " + file + " is greater than actual file size. "
                        + "Probably file was changed and can't be uploaded now.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't read size of file to upload, " + file + ". " + e.getMessage(), e);
        }
    }

    private void uploadRequiredParts(AmazonS3 s3, File file, RemoteFile remoteFile) throws IOException {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            uploadFailedParts(s3, file, remoteFile);
            uploadIncompleteParts(s3, remoteFile, inputStream, writingFinished);
            if (writingFinished) {
                commit(s3, remoteFile);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("File uploading was terminated.");
        }
    }

    private void uploadFailedParts(AmazonS3 s3, File file, RemoteFile remoteFile) {
        failedMiddleParts.stream()
                .map(partNumber -> {
                    byte[] partData = readPart(partNumber, file);
                    uploadedSize += partData.length;
                    return getUploadPartRequest(remoteFile, partData, false, partNumber);
                })
                .map(concurrentUploader::submitTaskForPartUploading)
                .map(this::getUploadingResult)
                .forEach(eTags::add);
    }

    private byte[] readPart(Integer partNumber, File file) {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            return getNextPart(MINIMUM_PART_SIZE * (partNumber - 1), inputStream, false);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe.getMessage(), ioe);
        }
    }

    private void commit(AmazonS3 s3, RemoteFile remoteFile) {
        eTags.sort(Comparator.comparing(PartETag::getPartNumber));
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(remoteFile.getBucket(), remoteFile.getFullPath(), uploadId, eTags);
        s3.completeMultipartUpload(request);
    }

    private void uploadIncompleteParts(AmazonS3 s3, RemoteFile remoteFile, InputStream inputStream, boolean uploadLastPart) throws IOException, InterruptedException {
        uploadPartsConcurrent(s3, remoteFile, inputStream, uploadLastPart).stream()
                .map(this::getUploadingResult)
                .forEach(eTags::add);
    }

    private List<Future<PartETag>> uploadPartsConcurrent(AmazonS3 s3, RemoteFile remoteFile, InputStream inputStream, boolean uploadLastPart) throws IOException, InterruptedException {
        List<Future<PartETag>> uploadingResults = new ArrayList<>();

        for (byte[] nextPart = getNextPart(uploadedSize, inputStream, uploadLastPart);
                nextPart.length > 0;
                nextPart = getNextPart(0, inputStream, uploadLastPart)) {
            int partSize = nextPart.length;
            boolean isLastPart = uploadLastPart && partSize < MINIMUM_PART_SIZE;
            UploadPartRequest request = getUploadPartRequest(remoteFile, nextPart, isLastPart, nextPartToUploadIndex++);

            Future<PartETag> uploadingResult = concurrentUploader.submitTaskForPartUploading(request);
            uploadingResults.add(uploadingResult);
        }

        concurrentUploader.shutdownAndAwaitTermination();

        return uploadingResults;
    }

    private UploadPartRequest getUploadPartRequest(RemoteFile remoteFile, byte[] nextPart, boolean isLastPart, int partNumber) {
        try (ByteArrayInputStream partInputStream = ByteHelper.createInputStream(nextPart)) {
            return new UploadPartRequest()
                    .withBucketName(remoteFile.getBucket())
                    .withKey(remoteFile.getFullPath())
                    .withPartNumber(partNumber)
                    .withMD5Digest(MD5Digest.digest(nextPart))
                    .withLastPart(isLastPart)
                    .withPartSize(nextPart.length)
                    .withUploadId(uploadId)
                    .withInputStream(partInputStream);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe.getMessage(), ioe);
        }
    }

    private PartETag getUploadingResult(Future<PartETag> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Some part uploads was unsuccessful. " + e.getMessage(), e);
        }
    }

    private byte[] getNextPart(long offset, InputStream inputStream, boolean readLastBytes) throws IOException {
        byte[] buffer = new byte[MINIMUM_PART_SIZE];
        int read = 0;
        skipAlreadyUploadedParts(offset, inputStream);
        int available = inputStream.available();
        if (available < MINIMUM_PART_SIZE && !readLastBytes) {
            return new byte[0];
        }
        while (available > 0) {
            int currentRed = inputStream.read(buffer, read, MINIMUM_PART_SIZE - read);
            read += currentRed;
            available = inputStream.available();
            if (read == MINIMUM_PART_SIZE) {
                break;
            }
        }
        return ByteHelper.truncate(buffer, read);
    }

    private void skipAlreadyUploadedParts(long offset, InputStream inputStream) throws IOException {
        long skipped = 0;
        while (skipped < offset) {
            skipped += inputStream.skip(offset);
        }
    }

    private String initUploading(AmazonS3 s3, RemoteFile remoteFile) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(remoteFile.getBucket(), remoteFile.getFullPath());
        InitiateMultipartUploadResult result = s3.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    private Set<Integer> getFailedMiddlePartNumbers(PartListing partListing) {
        AtomicInteger lastPartNumber = new AtomicInteger(0);
        Set<Integer> uploadedParts = partListing.getParts().stream()
                .map(PartSummary::getPartNumber)
                .peek(n -> {
                    if (lastPartNumber.get() < n) {
                        lastPartNumber.set(n);
                    }
                })
                .collect(Collectors.toSet());

        return IntStream.range(1, lastPartNumber.get())
                .filter(n -> !uploadedParts.contains(n))
                .boxed()
                .collect(Collectors.toSet());
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

    private PartListing getAlreadyUploadedParts(AmazonS3 s3, RemoteFile remoteFile, MultipartUpload upload) {
        return Optional.ofNullable(upload)
                .map(MultipartUpload::getUploadId)
                .map(id -> getPartListing(s3, remoteFile, id))
                .orElse(null);
    }

    private PartListing getPartListing(AmazonS3 s3, RemoteFile remoteFile, String uploadId) {
        ListPartsRequest request = new ListPartsRequest(remoteFile.getBucket(), remoteFile.getFullPath(), uploadId);
        return s3.listParts(request);
    }

    @Override
    public void setListener(SyncProgressListener listener) {
        this.listener = listener;
    }

}
