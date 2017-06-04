package tdl.s3.upload;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Test;
import static org.mockito.Mockito.*;
import tdl.s3.sync.destination.DestinationOperationException;

public class MultipartUploadFileUploadingStrategyTest {

    @Test(expected = RuntimeException.class)
    public void getUploadingResultShouldHandleInterruptedException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        when(future.get()).thenThrow(new InterruptedException());
        MultipartUploadFileUploadingStrategy.getUploadingResult(future);
    }

    @Test(expected = DestinationOperationException.class)
    public void getUploadingResultShouldHandleDestinationOperationException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        ExecutionException ex = mock(ExecutionException.class);
        when(ex.getCause()).thenReturn(new DestinationOperationException(""));
        when(future.get()).thenThrow(ex);
        MultipartUploadFileUploadingStrategy.getUploadingResult(future);
    }
    
    @Test(expected = RuntimeException.class)
    public void getUploadingResultShouldHandleExecutionException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        ExecutionException ex = mock(ExecutionException.class);
        when(future.get()).thenThrow(ex);
        MultipartUploadFileUploadingStrategy.getUploadingResult(future);
    }
}
