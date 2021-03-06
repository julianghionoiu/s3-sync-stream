package tdl.s3.upload;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;
import static org.mockito.Mockito.*;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

public class FileUploaderImplTest {

    @Test
    public void uploadShouldHandleFirstException() throws UploadingException, DestinationOperationException, IOException, URISyntaxException {
        Destination destination = mock(Destination.class);
        UploadingStrategy strategy = mock(UploadingStrategy.class);

        doThrow(new DestinationOperationException("Message"))
                .doNothing()
                .when(strategy)
                .upload(any(), anyString());

        FileUploader uploader = new FileUploaderImpl(destination, strategy);
        File file = mock(File.class);
        when(file.toURI()).thenReturn(new URI("file:///tmp/file1.txt"));
        when(file.getName()).thenReturn("path");
        uploader.upload(file);
    }

    @Test(expected = UploadingException.class)
    public void uploadShouldThrowExceptionWhenFailsToAllRetries() throws UploadingException, DestinationOperationException, IOException, URISyntaxException {
        Destination destination = mock(Destination.class);
        UploadingStrategy strategy = mock(UploadingStrategy.class);

        doThrow(new DestinationOperationException("Message"))
                .when(strategy)
                .upload(any(), anyString());

        FileUploader uploader = new FileUploaderImpl(destination, strategy);
        File file = mock(File.class);
        when(file.toURI()).thenReturn(new URI("file:///tmp/file1.txt"));
        when(file.getName()).thenReturn("path");
        uploader.upload(file);
    }
}
