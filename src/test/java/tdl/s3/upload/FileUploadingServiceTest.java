package tdl.s3.upload;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
@RunWith(MockitoJUnitRunner.class)
public class FileUploadingServiceTest {

    private FileUploadingService fileUploadingService;

    @Mock
    private FileUploader smallFilesUploader;

    @Mock
    private FileUploader largeFilesUploader;

    @Mock
    private File smallFile;

    @Mock
    private File largeFile;

    @Before
    public void setUp() throws Exception {
        fileUploadingService = new FileUploadingService(new HashMap<Integer, FileUploader>() {{
            put(1, smallFilesUploader);
            put(Integer.MAX_VALUE, largeFilesUploader);

        }});

        when(smallFile.length()).thenReturn(1255L);
        when(largeFile.length()).thenReturn(1255L * 1024);
    }

    @Test
    public void upload_smallFile() throws Exception {
        fileUploadingService.upload(smallFile);

        verify(smallFilesUploader, times(1)).upload(eq(smallFile), anyString());
        verify(largeFilesUploader, never()).upload(any(File.class), anyString());
    }

    @Test
    public void upload_largeFile() throws Exception {
        fileUploadingService.upload(largeFile);

        verify(largeFilesUploader, times(1)).upload(eq(largeFile), anyString());
        verify(smallFilesUploader, never()).upload(any(File.class), anyString());
    }


}