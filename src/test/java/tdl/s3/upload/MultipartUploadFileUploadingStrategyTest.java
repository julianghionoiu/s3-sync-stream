package tdl.s3.upload;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdl.s3.sync.destination.DestinationOperationException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultipartUploadFileUploadingStrategyTest {

    @Test
    public void getUploadingResultShouldReturnNullOnInterruptedException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        when(future.get()).thenThrow(new InterruptedException());
        Assertions.assertNull(MultipartUploadFileUploadingStrategy.getUploadingResult(future));
    }

    @Test
    public void getUploadingResultShouldReturnNullOnDestinationOperationException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        ExecutionException ex = mock(ExecutionException.class);
        when(ex.getCause()).thenReturn(new DestinationOperationException(""));
        when(future.get()).thenThrow(ex);
        Assertions.assertNull(MultipartUploadFileUploadingStrategy.getUploadingResult(future));
    }
    
    @Test
    public void getUploadingResultShouldReturnNullOnExecutionException() throws DestinationOperationException, InterruptedException, ExecutionException {
        Future future = mock(Future.class);
        ExecutionException ex = mock(ExecutionException.class);
        when(future.get()).thenThrow(ex);
        Assertions.assertNull(MultipartUploadFileUploadingStrategy.getUploadingResult(future));
    }
}
