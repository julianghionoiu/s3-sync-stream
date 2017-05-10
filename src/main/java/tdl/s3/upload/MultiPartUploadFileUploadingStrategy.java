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
import java.util.stream.Stream;
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
        this.client = s3;
        initStrategy(s3, file, remoteFile, upload);
        uploadRequiredParts(s3, file, remoteFile);
    }

    private void initStrategy(AmazonS3 s3, File file, RemoteFile remoteFile, MultipartUpload upload) {
        writingFinished = !FileHelper.lockFileExists(file);
        PartListing alreadyUploadedParts = MultipartUploadHelper.getAlreadyUploadedParts(s3, remoteFile, upload);

        boolean uploadingStarted = alreadyUploadedParts != null;
        if (!uploadingStarted) {
            initAttributes(remoteFile);
        } else {
            initAttributesFromAlreadyUploadedParts(alreadyUploadedParts);
        }
        eTags = MultipartUploadHelper.getPartETagsFromPartListing(alreadyUploadedParts);
        validateUploadedFileSize(file);
    }

    private void initAttributes(RemoteFile remoteFile) {
        uploadId = initUploading(client, remoteFile);
        uploadedSize = 0;
        failedMiddleParts = Collections.emptySet();
        nextPartToUploadIndex = 1;
    }

    private void initAttributesFromAlreadyUploadedParts(PartListing partListing) {
        uploadId = partListing.getUploadId();
        uploadedSize = MultipartUploadHelper.getUploadedSize(partListing);
        failedMiddleParts = MultipartUploadHelper.getFailedMiddlePartNumbers(partListing);
        nextPartToUploadIndex = MultipartUploadHelper.getLastPartIndex(partListing) + 1;
    }

    private void validateUploadedFileSize(File file) {
        try {
            if (Files.size(file.toPath()) < uploadedSize) {
                throw new IllegalStateException(
                        "Already uploaded size of file " + file.getName()
                        + " is greater than actual file size. "
                        + "Probably file was changed and can't be uploaded now."
                );
            }
        } catch (IOException e) {
            throw new RuntimeException("Can't read size of file to upload, " + file + ". " + e.getMessage(), e);
        }
    }

    private void uploadRequiredParts(AmazonS3 s3, File file, RemoteFile remoteFile) throws IOException {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            Stream.concat(
                    streamUploadForFailedParts(file, remoteFile),
                    streamUploadForIncompleteParts(remoteFile, inputStream, writingFinished)
            )
                    .map(concurrentUploader::submitTaskForPartUploading)
                    .map(this::getUploadingResult)
                    .forEach(eTags::add);
            concurrentUploader.shutdownAndAwaitTermination();
            if (writingFinished) {
                commit(s3, remoteFile);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("File uploading was terminated.");
        }
    }

    private Stream<UploadPartRequest> streamUploadForFailedParts(File file, RemoteFile remoteFile) {
        return failedMiddleParts.stream()
                .map(partNumber -> {
                    byte[] partData = ByteHelper.readPart(partNumber, file);
                    uploadedSize += partData.length;
                    return getUploadPartRequest(remoteFile, partData, false, partNumber);
                });
    }

    private Stream<UploadPartRequest> streamUploadForIncompleteParts(RemoteFile remoteFile, InputStream inputStream, boolean writingFinished) throws IOException {
        byte[] nextPart = ByteHelper.getNextPartFromInputStream(inputStream, uploadedSize, writingFinished);
        int partSize = nextPart.length;
        List<UploadPartRequest> requests = new ArrayList<>();
        while (partSize > 0) {
            boolean isLastPart = writingFinished && partSize < MINIMUM_PART_SIZE;
            UploadPartRequest request = getUploadPartRequest(remoteFile, nextPart, isLastPart, nextPartToUploadIndex++);
            requests.add(request);
            nextPart = ByteHelper.getNextPartFromInputStream(inputStream, 0, writingFinished);
            partSize = nextPart.length;
        }
        return requests.stream();
    }

    private void commit(AmazonS3 s3, RemoteFile remoteFile) {
        eTags.sort(Comparator.comparing(PartETag::getPartNumber));
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(remoteFile.getBucket(), remoteFile.getFullPath(), uploadId, eTags);
        s3.completeMultipartUpload(request);
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

    private String initUploading(AmazonS3 s3, RemoteFile remoteFile) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(remoteFile.getBucket(), remoteFile.getFullPath());
        InitiateMultipartUploadResult result = s3.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    @Override
    public void setListener(SyncProgressListener listener) {
        this.listener = listener;
    }

}
