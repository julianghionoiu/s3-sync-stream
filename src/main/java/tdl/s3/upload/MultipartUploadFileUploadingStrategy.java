package tdl.s3.upload;

import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import tdl.s3.helpers.ByteHelper;
import tdl.s3.helpers.ExistingMultipartUploadFinder;
import tdl.s3.helpers.FileHelper;
import tdl.s3.helpers.MD5Digest;
import tdl.s3.helpers.MultipartUploadHelper;
import tdl.s3.sync.Destination;
import tdl.s3.sync.DummyProgressListener;
import tdl.s3.sync.ProgressListener;

public class MultipartUploadFileUploadingStrategy implements UploadingStrategy {

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
     * @param threadsCount count of threads that should be used for uploading
     */
    MultipartUploadFileUploadingStrategy(Destination destination, int threadsCount) {
        this.destination = destination;
        concurrentUploader = new ConcurrentMultipartUploader(threadsCount);
    }
    
    @Override
    public void upload(File file, RemoteFile remoteFile) throws Exception {
        concurrentUploader.setClient(destination.getClient());
        initStrategy(file, remoteFile);
        listener.uploadFileStarted(file, uploadId);
        uploadRequiredParts(file, remoteFile);
    }
    
    public MultipartUpload findMultiPartUpload(RemoteFile remoteFile) {
        ExistingMultipartUploadFinder finder = new ExistingMultipartUploadFinder(destination.getClient(), remoteFile.getBucket(), remoteFile.getPrefix());
        return finder.findOrNull(remoteFile);
    }

    private void initStrategy(File file, RemoteFile remoteFile) {
        writingFinished = !FileHelper.lockFileExists(file);
        MultipartUpload multipartUpload = findMultiPartUpload(remoteFile);
        PartListing alreadyUploadedParts = MultipartUploadHelper.getAlreadyUploadedParts(destination.getClient(), remoteFile, multipartUpload);

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
        uploadId = initUploading(remoteFile);
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

    private void uploadRequiredParts(File file, RemoteFile remoteFile) throws IOException {
        try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            submitUploadRequestStream(streamUploadForFailedParts(file, remoteFile));
            submitUploadRequestStream(streamUploadForIncompleteParts(remoteFile, inputStream, writingFinished));
            concurrentUploader.shutdownAndAwaitTermination();
            if (writingFinished) {
                commit(remoteFile);
                listener.uploadFileFinished(file);
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

    private void submitUploadRequestStream(Stream<UploadPartRequest> requestStream) {
        requestStream.map(concurrentUploader::submitTaskForPartUploading)
                .map(this::getUploadingResult)
                .map(e -> e.getResult().getPartETag())
                .forEach(eTags::add);
    }

    private void commit(RemoteFile remoteFile) {
        eTags.sort(Comparator.comparing(PartETag::getPartNumber));
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(remoteFile.getBucket(), remoteFile.getFullPath(), uploadId, eTags);
        destination.getClient().completeMultipartUpload(request);
    }

    private UploadPartRequest getUploadPartRequest(RemoteFile remoteFile, byte[] nextPart, boolean isLastPart, int partNumber) {
        try (ByteArrayInputStream partInputStream = ByteHelper.createInputStream(nextPart)) {
            UploadPartRequest request = new UploadPartRequest()
                    .withBucketName(remoteFile.getBucket())
                    .withKey(remoteFile.getFullPath())
                    .withPartNumber(partNumber)
                    .withMD5Digest(MD5Digest.digest(nextPart))
                    .withLastPart(isLastPart)
                    .withPartSize(nextPart.length)
                    .withUploadId(uploadId)
                    .withInputStream(partInputStream);

            request.setGeneralProgressListener(createListenerForUploadPartRequest(request));
            return request;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe.getMessage(), ioe);
        }
    }

    private com.amazonaws.event.ProgressListener createListenerForUploadPartRequest(UploadPartRequest request) {
        return new com.amazonaws.event.ProgressListener() {
            @Override
            public void progressChanged(com.amazonaws.event.ProgressEvent pe) {
                listener.uploadFileProgress(request.getUploadId(), pe.getBytesTransferred());
            }
        };
    }

    private MultipartUploadResult getUploadingResult(Future<MultipartUploadResult> future) {
        try {
            MultipartUploadResult result = future.get();
            //listener.uploadFileProgress(result.getRequest().getUploadId(), (int) result.getRequest().getPartSize());
            return result;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Some part uploads was unsuccessful. " + e.getMessage(), e);
        }
    }

    private String initUploading(RemoteFile remoteFile) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(remoteFile.getBucket(), remoteFile.getFullPath());
        InitiateMultipartUploadResult result = destination.getClient().initiateMultipartUpload(request);
        return result.getUploadId();
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
