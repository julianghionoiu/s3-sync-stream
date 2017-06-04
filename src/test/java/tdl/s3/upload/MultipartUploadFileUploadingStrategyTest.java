package tdl.s3.upload;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import static org.junit.Assert.*;
import org.junit.Test;
import static org.mockito.Mockito.*;
import tdl.s3.sync.destination.DestinationOperationException;

public class MultipartUploadFileUploadingStrategyTest {

    @Test
    public void getUploadingResultShouldReturnNullOnInterruptedException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        when(future.get()).thenThrow(new InterruptedException());
        assertNull(MultipartUploadFileUploadingStrategy.getUploadingResult(future));
    }

    @Test
    public void getUploadingResultShouldReturnNullOnDestinationOperationException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        ExecutionException ex = mock(ExecutionException.class);
        when(ex.getCause()).thenReturn(new DestinationOperationException(""));
        when(future.get()).thenThrow(ex);
        assertNull(MultipartUploadFileUploadingStrategy.getUploadingResult(future));
    }
    
    @Test
    public void getUploadingResultShouldReturnNullOnExecutionException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        ExecutionException ex = mock(ExecutionException.class);
        when(future.get()).thenThrow(ex);
        assertNull(MultipartUploadFileUploadingStrategy.getUploadingResult(future));
    }
}
