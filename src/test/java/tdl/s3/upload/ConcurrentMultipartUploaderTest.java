package tdl.s3.upload;

import com.amazonaws.services.s3.model.UploadPartRequest;
import com.beust.jcommander.internal.Lists;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

public class ConcurrentMultipartUploaderTest {

    @Test(expected = IllegalArgumentException.class)
    public void constructorShouldThrowExceptionOnInvalidThreadCount() throws IllegalArgumentException {
        Destination destination = mock(Destination.class);
        ConcurrentMultipartUploader uploader = new ConcurrentMultipartUploader(destination, 0);
    }

    @Test
    public void testExecutor() {
        Destination destination = mock(Destination.class);
        ConcurrentMultipartUploader uploader = new ConcurrentMultipartUploader(destination);
        ExecutorService executorService = uploader.getExecutorService();
        assertNotNull(executorService);
    }

    @Test
    public void executionShouldHandleException() throws DestinationOperationException, InterruptedException {
        Destination destination = mock(Destination.class);
        MultipartUploadResult result = mock(MultipartUploadResult.class);
        when(destination.uploadMultiPart(any()))
                .thenReturn(result)
                .thenThrow(new DestinationOperationException(""));

        ConcurrentMultipartUploader uploader = new ConcurrentMultipartUploader(destination);

        List<UploadPartRequest> requests = Lists.newArrayList(
                mock(UploadPartRequest.class),
                mock(UploadPartRequest.class)
        );
        List<MultipartUploadResult> streamResult = requests.stream()
                .map(uploader::submitTaskForPartUploading)
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException ex) {
                        return null;
                    } catch (ExecutionException ex) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        assertEquals(streamResult.size(), 1);
        uploader.shutdownAndAwaitTermination();
    }
}
