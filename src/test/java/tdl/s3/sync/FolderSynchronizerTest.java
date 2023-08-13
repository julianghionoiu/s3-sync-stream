package tdl.s3.sync;

import org.junit.jupiter.api.Test;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.nio.file.Path;

import static org.mockito.Mockito.*;

public class FolderSynchronizerTest {

    @Test
    public void synchronizeShouldHandleEmptyStringIfExceptionThrown() throws DestinationOperationException {

        Source source = mock(Source.class);
        when(source.isRecursive()).thenReturn(true);

        Path path = mock(Path.class);
        when(path.toFile()).thenReturn(mock(File.class));
        when(source.getPath()).thenReturn(path);

        FileUploadingService fileUploadingService = mock(FileUploadingService.class);
        doNothing().when(fileUploadingService).upload(any(), anyString());

        Destination destination = mock(Destination.class);
        doThrow(new DestinationOperationException("Message"))
                .when(destination)
                .filterUploadableFiles(anyList());

        when(fileUploadingService.getDestination()).thenReturn(destination);

        FolderSynchronizer synchronizer = new FolderSynchronizer(source, fileUploadingService);
        synchronizer.synchronize();
    }
}
