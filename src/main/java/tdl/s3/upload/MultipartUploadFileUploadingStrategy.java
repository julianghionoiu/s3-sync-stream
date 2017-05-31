package tdl.s3.upload;

import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.helpers.ByteHelper;
import tdl.s3.helpers.FileHelper;
import tdl.s3.helpers.MD5Digest;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.sync.destination.S3BucketDestination;
import tdl.s3.sync.progress.DummyProgressListener;
import tdl.s3.sync.progress.ProgressListener;

@Slf4j
public class MultipartUploadFileUploadingStrategy implements FileUploadingStrategy {

    //Minimum part size is 5 MB
    private static final int MINIMUM_PART_SIZE = 5 * 1024 * 1024;

    private static final int DEFAULT_THREAD_COUNT = 4;

    private S3BucketDestination destination;

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
    public MultipartUploadFileUploadingStrategy(S3BucketDestination destination) {
        this(destination, DEFAULT_THREAD_COUNT);
    }

    /**
     * Creates new Multipart upload strategy.
     *
     * @param threadsCount count of threads that should be used for uploading
     */
    private MultipartUploadFileUploadingStrategy(S3BucketDestination destination, int threadsCount) {
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

        PartListing alreadyUploadedParts = getAlreadyUploadedParts(remotePath);

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
        uploadId = initUploading(remotePath);
        uploadedSize = 0;
        failedMiddleParts = Collections.emptySet();
        nextPartToUploadIndex = 1;
    }

    private String initUploading(String remotePath) throws DestinationOperationException {
        String path = destination.getFullPath(remotePath);
        try {
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(destination.getBucket(), path);
            InitiateMultipartUploadResult result = destination.getAwsClient().initiateMultipartUpload(request);
            return result.getUploadId();
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Fail to initialize uploading process: " + path, ex);
        }
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
        requestStream
                .map(this::createCallableForPartUploadingAndReturnETag)
                .map(concurrentUploader::submitTaskForPartUploading)
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

    private Callable<MultipartUploadResult> createCallableForPartUploadingAndReturnETag(UploadPartRequest request) {
        return () -> {
            try {
                return uploadMultiPart(request);
            } catch (DestinationOperationException e) {
                throw e;
            }
        };
    }

    public MultipartUploadResult uploadMultiPart(UploadPartRequest request) throws DestinationOperationException {
        try {
            UploadPartResult result = destination.getAwsClient().uploadPart(request);
            return new MultipartUploadResult(request, result);
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Fail to upload multipart: " + request.getKey() + " #" + request.getPartNumber(), ex);
        }
    }

    private void commit(String remotePath) throws DestinationOperationException {
        commitMultipartUpload(remotePath, eTags, uploadId);
    }

    private UploadPartRequest getUploadPartRequest(String remotePath, byte[] nextPart, boolean isLastPart, int partNumber) throws DestinationOperationException, IOException {
        try (ByteArrayInputStream partInputStream = ByteHelper.createInputStream(nextPart)) {
            UploadPartRequest request = createUploadPartRequest(remotePath)
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

    public PartListing getAlreadyUploadedParts(String remotePath) throws DestinationOperationException {
        MultipartUpload multipartUpload = findOrNull(remotePath);

        return Optional.ofNullable(multipartUpload)
                .map(MultipartUpload::getUploadId)
                .map(id -> getPartListing(remotePath, id))
                .orElse(null);
    }

    private MultipartUpload findOrNull(String remotePath) throws DestinationOperationException {
        List<MultipartUpload> uploads = getAlreadyStartedMultipartUploads();
        return uploads.stream()
                .filter(upload -> upload.getKey().equals(destination.getFullPath(remotePath)))
                .findAny()
                .orElse(null);
    }

    private List<MultipartUpload> getAlreadyStartedMultipartUploads() throws DestinationOperationException {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        MultipartUploadListing multipartUploadListing = listMultipartUploads(uploadsRequest);

        Stream<MultipartUploadListing> stream = Stream.of(multipartUploadListing)
                .flatMap(listing -> {
                    try {
                        return this.streamNextListing(listing);
                    } catch (DestinationOperationException ex) {
                        log.error("Failed to stream next listing " + listing.getUploadIdMarker(), ex);
                        return null;
                    }
                }).filter(Objects::nonNull);

        return stream.map(MultipartUploadListing::getMultipartUploads)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Stream<MultipartUploadListing> streamNextListing(MultipartUploadListing listing) throws DestinationOperationException {
        if (!listing.isTruncated()) {
            return Stream.of(listing);
        }
        MultipartUploadListing nextListing = getNextListing(listing);

        Stream<MultipartUploadListing> head = Stream.of(listing);
        Stream<MultipartUploadListing> tail = streamNextListing(nextListing);

        return Stream.concat(head, tail);
    }

    private MultipartUploadListing getNextListing(MultipartUploadListing listing) throws DestinationOperationException {
        ListMultipartUploadsRequest uploadsRequest = createListMultipartUploadsRequest();
        uploadsRequest.setUploadIdMarker(listing.getNextUploadIdMarker());
        uploadsRequest.setKeyMarker(listing.getNextKeyMarker());
        return listMultipartUploads(uploadsRequest);
    }

    private ListMultipartUploadsRequest createListMultipartUploadsRequest() {
        ListMultipartUploadsRequest uploadsRequest = new ListMultipartUploadsRequest(destination.getBucket());
        uploadsRequest.setPrefix(destination.getBucket());
        return uploadsRequest;
    }

    private MultipartUploadListing listMultipartUploads(ListMultipartUploadsRequest request) throws DestinationOperationException {
        try {
            return destination.getAwsClient().listMultipartUploads(request);
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Failed to list upload request: " + request.getBucketName() + "/" + request.getPrefix(), ex);
        }
    }

    private PartListing getPartListing(String remotePath, String uploadId) {
        ListPartsRequest request = new ListPartsRequest(destination.getBucket(), destination.getFullPath(remotePath), uploadId);
        return listParts(request);
    }

    private PartListing listParts(ListPartsRequest request) {
        return destination.getAwsClient().listParts(request);
    }

    private void commitMultipartUpload(String remotePath, List<PartETag> eTags, String uploadId) throws DestinationOperationException {
        eTags.sort(Comparator.comparing(PartETag::getPartNumber));
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(
                destination.getBucket(),
                destination.getFullPath(remotePath),
                uploadId,
                eTags
        );
        completeMultipartUpload(request);
    }

    private void completeMultipartUpload(CompleteMultipartUploadRequest request) throws DestinationOperationException {
        try {
            destination.getAwsClient().completeMultipartUpload(request);
        } catch (AmazonS3Exception ex) {
            throw new DestinationOperationException("Failed to complete multipart request: " + request.getKey(), ex);
        }
    }

    private UploadPartRequest createUploadPartRequest(String remotePath) throws DestinationOperationException {
        return new UploadPartRequest()
                .withBucketName(destination.getBucket())
                .withKey(destination.getFullPath(remotePath));
    }
}
