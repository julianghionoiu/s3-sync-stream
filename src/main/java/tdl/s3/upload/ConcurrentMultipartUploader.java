package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConcurrentMultipartUploader {

    private static final int DEFAULT_THREAD_COUNT = 4;

    private static final int MAX_UPLOADING_TIME = 360;

    private AmazonS3 client;

    private final ExecutorService executorService;

    public ConcurrentMultipartUploader() {
        this(DEFAULT_THREAD_COUNT);
    }

    public ConcurrentMultipartUploader(int threadCount) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("Thread count should be >= 1");
        }
        executorService = Executors.newFixedThreadPool(threadCount);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setClient(AmazonS3 client) {
        this.client = client;
    }

    public void shutdownAndAwaitTermination() throws InterruptedException {
        executorService.shutdown();
        executorService.awaitTermination(MAX_UPLOADING_TIME, TimeUnit.SECONDS);
    }

    public Future<PartETag> submitTaskForPartUploading(UploadPartRequest request) {
        Callable<PartETag> task = createCallableForPartUploadingAndReturnETag(request);
        return executorService.submit(task);
    }

    private Callable<PartETag> createCallableForPartUploadingAndReturnETag(UploadPartRequest request) {
        return () -> {
            try {
                UploadPartResult result = client.uploadPart(request);
                return result.getPartETag();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        };
    }
}
