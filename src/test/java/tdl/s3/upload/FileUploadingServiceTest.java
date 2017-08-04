package tdl.s3.upload;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import org.junit.Test;
import static org.mockito.Mockito.*;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

public class FileUploadingServiceTest {

    @Test
    public void uploadShouldHandleUploadException() throws DestinationOperationException, URISyntaxException {
        Destination destination = mock(Destination.class);
        doThrow(new DestinationOperationException("Message"))
                .when(destination)
                .getAlreadyUploadedParts(anyString());
        File file = mock(File.class);
        when(file.getName()).thenReturn("file.txt");
        when(file.toURI()).thenReturn(new URI("file:///tmp/file1.txt"));
        
        Path path = mock(Path.class);
        when(path.toAbsolutePath()).thenReturn(path);
        when(path.normalize()).thenReturn(path);
        when(path.getParent()).thenReturn(path);
        //doNothing().when(path).resolve(anyString());
        when(file.toPath()).thenReturn(path);
        FileUploadingService service = new FileUploadingService(destination);
        service.upload(file);
    }
}
