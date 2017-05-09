package tdl.s3.upload;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrentMultipartUploader {

    private static final int DEFAULT_THREAD_COUNT = 4;

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
}
