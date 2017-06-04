package tdl.s3.upload;

import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.sync.progress.DummyProgressListener;
import tdl.s3.sync.progress.ProgressListener;

@Slf4j
public class MultipartUploadFileUploadingStrategy implements UploadingStrategy {

    private static final int DEFAULT_THREAD_COUNT = 4;

    private Destination destination;

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
        MultipartUploadFile multipartUploadFile = new MultipartUploadFile(file, remotePath, destination);
        multipartUploadFile.validateUploadedFileSize();
        multipartUploadFile.notifyStart(listener);
        uploadRequiredParts(multipartUploadFile);
        multipartUploadFile.notifyFinish(listener);
    }

    private void uploadRequiredParts(MultipartUploadFile multipartUploadFile) throws IOException, DestinationOperationException {
        List<PartETag> eTags = multipartUploadFile.getPartETags();

        Stream<UploadPartRequest> failedPartRequestStream = multipartUploadFile
                .streamUploadPartRequestForFailedParts();
        submitUploadRequestStream(failedPartRequestStream, eTags);

        Stream<UploadPartRequest> incompletePartRequestStream = multipartUploadFile
                .streamUploadPartRequestForIncompleteParts();
        submitUploadRequestStream(incompletePartRequestStream, eTags);

        try {
            concurrentUploader.shutdownAndAwaitTermination();
        } catch (InterruptedException ex) {
            String message = "File uploading for " + multipartUploadFile.getFile().getName() + " was terminated.";
            throw new DestinationOperationException(message, ex);
        }
        multipartUploadFile.commitIfFinishedWriting();
    }

    private void submitUploadRequestStream(Stream<UploadPartRequest> requestStream, List<PartETag> partETags) {
        requestStream
                .map(this::attachListenerToRequest)
                .map(concurrentUploader::submitTaskForPartUploading)
                .map(future -> {
                    try {
                        return getUploadingResult(future);
                    } catch (DestinationOperationException ex) {
                        log.error("Failed to upload", ex);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .map(e -> e.getResult().getPartETag())
                .forEach(partETags::add);
    }

    private UploadPartRequest attachListenerToRequest(UploadPartRequest request) {
        request.setGeneralProgressListener((com.amazonaws.event.ProgressEvent pe)
                -> listener.uploadFileProgress(request.getUploadId(), pe.getBytesTransferred()));
        return request;
    }

    public static MultipartUploadResult getUploadingResult(Future<MultipartUploadResult> future) throws DestinationOperationException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new DestinationOperationException("Some part uploads was unsuccessful. " + e.getMessage(), e);
        } catch (ExecutionException e) {
            Throwable ex = e.getCause();
            if (ex instanceof DestinationOperationException) {
                throw (DestinationOperationException) ex;
            }
            throw new DestinationOperationException("Some part uploads was unsuccessful. " + e.getMessage(), e);
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
