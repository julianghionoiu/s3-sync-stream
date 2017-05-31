package tdl.s3.upload;

import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.helpers.ByteHelper;
import tdl.s3.helpers.FileHelper;
import tdl.s3.helpers.MD5Digest;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.sync.progress.DummyProgressListener;
import tdl.s3.sync.progress.ProgressListener;

@Slf4j
public class MultipartUploadFileUploadingStrategy implements FileUploadingStrategy {

    //Minimum part size is 5 MB
    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;

    private static final int DEFAULT_THREAD_COUNT = 4;

    private Destination destination;

    private String uploadId;

    private List<PartETag> eTags;

    private long uploadedSize;

    private Set<Integer> failedMiddleParts;

    private int nextPartToUploadIndex;

    private boolean writingFinished;

    private ConcurrentMultipartUploader concurrentUploader;

    private ProgressListener listener = new DummyProgressListener();

    /**
     * Creates new Multipart upload strategy
     */
    MultipartUploadFileUploadingStrategy(Destination destination) {
        this(destination, DEFAULT_THREAD_COUNT);
    }

    /**
     * Creates new Multipart upload strategy.
     *
     * @param threadsCount count of threads that should be used for uploading
     */
    private MultipartUploadFileUploadingStrategy(Destination destination, int threadsCount) {
        this.destination = destination;
        concurrentUploader = new ConcurrentMultipartUploader(destination, threadsCount);
    }

    @Override
    public void upload(File file, String remotePath) throws DestinationOperationException, IOException {
        initStrategy(file, remotePath);
        listener.uploadFileStarted(file, uploadId);
        uploadRequiredParts(file, remotePath);
        listener.uploadFileFinished(file);
    }

    private void initStrategy(File file, String remotePath) throws DestinationOperationException, IOException {
        writingFinished = !FileHelper.lockFileExists(file);

        PartListing alreadyUploadedParts = destination.getAlreadyUploadedParts(remotePath);

        boolean uploadingStarted = alreadyUploadedParts != null;
        if (!uploadingStarted) {
            initAttributes(remotePath);
        } else {
            initAttributesFromAlreadyUploadedParts(alreadyUploadedParts);
        }
        eTags = MultipartUploadHelper.getPartETagsFromPartListing(alreadyUploadedParts);
        validateUploadedFileSize(file);
    }

    private void initAttributes(String remotePath) throws DestinationOperationException {
        uploadId = destination.initUploading(remotePath);
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

    private void validateUploadedFileSize(File file) throws IOException {
        if (Files.size(file.toPath()) < uploadedSize) {
            throw new IllegalStateException(
                    "Already uploaded size of file " + file.getName()
                    + " is greater than actual file size. "
                    + "Probably file was changed and can't be uploaded now."
            );
        }
    }

    private void uploadRequiredParts(File file, String remotePath) throws IOException, DestinationOperationException {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            submitUploadRequestStream(streamUploadForFailedParts(file, remotePath));
            submitUploadRequestStream(streamUploadForIncompleteParts(remotePath, inputStream, writingFinished));
            concurrentUploader.shutdownAndAwaitTermination();
            if (writingFinished) {
                commit(remotePath);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("File uploading was terminated.");
        }
    }

    private Stream<UploadPartRequest> streamUploadForFailedParts(File file, String remotePath) {
        return failedMiddleParts.stream()
                .map(partNumber -> {
                    try {
                        byte[] partData = ByteHelper.readPart(partNumber, file);
                        UploadPartRequest request = getUploadPartRequest(remotePath, partData, false, partNumber);
                        uploadedSize += partData.length;
                        return request;
                    } catch (IOException | DestinationOperationException ex) {
                        return null;
                    }
                }).filter(Objects::nonNull);
    }

    private Stream<UploadPartRequest> streamUploadForIncompleteParts(String remotePath, InputStream inputStream, boolean writingFinished) throws IOException, DestinationOperationException {
        byte[] nextPart = ByteHelper.getNextPartFromInputStream(inputStream, uploadedSize, writingFinished);
        int partSize = nextPart.length;
        List<UploadPartRequest> requests = new ArrayList<>();
        while (partSize > 0) {
            boolean isLastPart = writingFinished && partSize < MINIMUM_PART_SIZE;
            UploadPartRequest request = getUploadPartRequest(remotePath, nextPart, isLastPart, nextPartToUploadIndex++);
            requests.add(request);
            nextPart = ByteHelper.getNextPartFromInputStream(inputStream, 0, writingFinished);
            partSize = nextPart.length;
        }
        return requests.stream();
    }

    private void submitUploadRequestStream(Stream<UploadPartRequest> requestStream) {
        requestStream.map(concurrentUploader::submitTaskForPartUploading)
                .map(future -> {
                    try {
                        return this.getUploadingResult(future);
                    } catch (DestinationOperationException ex) {
                        log.error("Failed to upload", ex);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .map(e -> e.getResult().getPartETag())
                .forEach(eTags::add);
    }

    private void commit(String remotePath) throws DestinationOperationException {
        destination.commitMultipartUpload(remotePath, eTags, uploadId);
    }

    private UploadPartRequest getUploadPartRequest(String remotePath, byte[] nextPart, boolean isLastPart, int partNumber) throws DestinationOperationException, IOException {
        try (ByteArrayInputStream partInputStream = ByteHelper.createInputStream(nextPart)) {
            UploadPartRequest request = destination.createUploadPartRequest(remotePath)
                    .withPartNumber(partNumber)
                    .withMD5Digest(MD5Digest.digest(nextPart))
                    .withLastPart(isLastPart)
                    .withPartSize(nextPart.length)
                    .withUploadId(uploadId)
                    .withInputStream(partInputStream);

            request.setGeneralProgressListener((com.amazonaws.event.ProgressEvent pe)
                    -> listener.uploadFileProgress(request.getUploadId(), pe.getBytesTransferred()));

            return request;
        }
    }

    private MultipartUploadResult getUploadingResult(Future<MultipartUploadResult> future) throws DestinationOperationException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Some part uploads was unsuccessful. " + e.getMessage(), e);
        } catch (ExecutionException e) {
            Throwable ex = e.getCause();
            if (ex instanceof DestinationOperationException) {
                throw (DestinationOperationException) ex;
            }
            throw new RuntimeException("Some part uploads was unsuccessful. " + e.getMessage(), e);
        }
    }

    @Override
    public void setListener(ProgressListener listener) {
        this.listener = listener;
    }

    @Override
    public void setDestination(Destination destination) {
        this.destination = destination;
    }
}
