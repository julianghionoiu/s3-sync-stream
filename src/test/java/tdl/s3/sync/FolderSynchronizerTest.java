package tdl.s3.sync;

import java.io.File;
import java.nio.file.Path;
import org.junit.Test;
import static org.mockito.Mockito.*;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.upload.FileUploadingService;

public class FolderSynchronizerTest {

    @Test
    public void synchronizeShouldHandleEmptyStringIfExceptionThrown() throws DestinationOperationException {
        FolderScanner folderScanner = mock(FolderScanner.class);
        FileUploadingService fileUploadingService = mock(FileUploadingService.class);
        doNothing().when(fileUploadingService).upload(any(), anyString());
        
        Destination destination = mock(Destination.class);
        doThrow(new DestinationOperationException("Message"))
                .when(destination)
                .filterUploadableFiles(anyList());
        
        when(fileUploadingService.getDestination()).thenReturn(destination);
        
        FolderSynchronizer synchronizer = new FolderSynchronizer(folderScanner, fileUploadingService);
        
        Path path = mock(Path.class);
        when(path.toFile()).thenReturn(mock(File.class));
        synchronizer.synchronize(path, true);
    }
}
