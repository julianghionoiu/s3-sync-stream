package tdl.s3.upload;

import org.junit.Test;

import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

import static org.mockito.Mockito.*;

public class FileUploaderImplTest
{

    @Test(expected = DestinationOperationException.class)
    public void existsShouldThrowExceptionOnFailure() throws DestinationOperationException {
        Destination destination = mock(Destination.class);
        doThrow(new DestinationOperationException())
                .when(destination)
                .canUpload(any());
        FileUploader uploader = new FileUploaderImpl(destination, null);
        uploader.exists(any());
    }
}
