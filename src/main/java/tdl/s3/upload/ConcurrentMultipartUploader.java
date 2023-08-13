package tdl.s3.upload;

import com.amazonaws.services.s3.model.UploadPartRequest;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

import java.util.concurrent.*;

public class ConcurrentMultipartUploader {

    private static final int DEFAULT_THREAD_COUNT = 4;

    private static final int MAX_UPLOADING_TIME = 360;

    private final Destination destination;

    private final ExecutorService executorService;

    public ConcurrentMultipartUploader(Destination destination) {
        this(destination, DEFAULT_THREAD_COUNT);
    }

    ConcurrentMultipartUploader(Destination destination, int threadCount) {
        this.destination = destination;
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count should be >= 1");
        }
        executorService = Executors.newFixedThreadPool(threadCount);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    void shutdownAndAwaitTermination() throws DestinationOperationException {
        ExecutorService service = getExecutorService();
        service.shutdown();
        try {
            service.awaitTermination(MAX_UPLOADING_TIME, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            throw new DestinationOperationException("Cannot finish uploading", ex);
        }
    }

    Future<MultipartUploadResult> submitTaskForPartUploading(UploadPartRequest request) {
        Callable<MultipartUploadResult> task = createCallableForPartUploadingAndReturnETag(request);
        return getExecutorService().submit(task);
    }

    private Callable<MultipartUploadResult> createCallableForPartUploadingAndReturnETag(UploadPartRequest request) {
        return () -> {
            try {
                return destination.uploadMultiPart(request);
            } catch (DestinationOperationException e) {
                throw e;
            }
        };
    }
}
