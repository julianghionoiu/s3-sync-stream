package tdl.s3.upload;

import com.amazonaws.services.s3.model.UploadPartRequest;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import tdl.s3.sync.Filters;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

public class MultipartUploadFileTest {

    private Path testPath;

    private File mockFile;

    private Destination mockDestination;

    private String mockRemotePath;

    @Before
    public void setUp() throws Exception {
        testPath = Paths.get("src", "test", "resources", "test_dir");
        mockRemotePath = "file.txt";
        mockDestination = mock(Destination.class);
        mockFile = mock(File.class);
        when(mockFile.toPath()).thenReturn(testPath);
    }

    @Test(expected = IllegalStateException.class)
    public void validateUploadedFileSizeShouldThrowException() throws DestinationOperationException {
        when(mockFile.length()).thenReturn(new Long(-1));
        MultipartUploadFile multipartUploadFile = new MultipartUploadFile(mockFile, mockRemotePath, mockDestination);
        multipartUploadFile.getFile();
        multipartUploadFile.getUploadId();
        multipartUploadFile.validateUploadedFileSize();
    }

    @Test
    public void streamUploadPartRequestForFailedPartsShouldHandleIOException() throws DestinationOperationException, IOException {
        MultipartUploadFile multipartUploadFile = mock(MultipartUploadFile.class);
        Set<Integer> partNumbers = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));

        doReturn(partNumbers)
                .when(multipartUploadFile)
                .getFailedMiddlePartNumbers();
        doReturn(mock(UploadPartRequest.class))
                .when(multipartUploadFile)
                .getUploadPartRequestForData(any(), anyBoolean(), anyInt());
        doCallRealMethod().when(multipartUploadFile)
                .streamUploadPartRequestForFailedParts();

        Arrays.asList(1, 3, 5).stream().forEach(partNumber -> {
            try {
                when(multipartUploadFile.readPart(eq(partNumber)))
                        .thenReturn("Random".getBytes());
            } catch (IOException ex) {
                Logger.getLogger(MultipartUploadFileTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        });
        Arrays.asList(2, 4).stream().forEach(partNumber -> {
            try {
                when(multipartUploadFile.readPart(eq(partNumber)))
                        .thenThrow(new IOException());
            } catch (IOException ex) {
                Logger.getLogger(MultipartUploadFileTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        });

        List<UploadPartRequest> requests = multipartUploadFile.streamUploadPartRequestForFailedParts()
                .collect(Collectors.toList());

        assertEquals(requests.size(), 3);
    }
}
