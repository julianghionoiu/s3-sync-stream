package tdl.s3.upload;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
@PrepareForTest(FileUploadingService.class)
public class FileUploadingServiceTest {

    private FileUploadingService fileUploadingService;

    @Mock
    private File smallFile;
    @Mock
    private File largeFile;
    @Mock
    private File incompleteFile;
    @Mock
    private Path incompleteFilePath;
    @Mock
    private AmazonS3 s3;
    @Mock
    private MultipartUploadListing listing;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private FileSystemProvider fsProvider;
    @Mock
    private MultipartUpload upload;
    @Mock
    private FileUploader fileUploader;

    private ArgumentMatcher<Path> lockFileMatcher = new ArgumentMatcher<Path>() {
        @Override
        public boolean matches(Object item) {
            return ((Path) item).endsWith(".lock");
        }
    };

    private ArgumentMatcher<UploadingStrategy> withClass(Class<? extends UploadingStrategy> c) {
        return new ArgumentMatcher<UploadingStrategy>() {
            @Override
            public boolean matches(Object item) {
                return c.equals(item.getClass());
            }
        };
    }

    @Before
    public void setUp() throws Exception {
        fileUploadingService = new FileUploadingService(s3, "testBucket", fileUploader);

        when(smallFile.length()).thenReturn(1255L);
        when(largeFile.length()).thenReturn(6L * 1024 * 1024);
        when(incompleteFile.length()).thenReturn(6L * 1024 * 1024);
        when(s3.listMultipartUploads(any())).thenReturn(listing);

        when(incompleteFile.getName()).thenReturn("incomplete.bin");
        when(upload.getKey()).thenReturn("incomplete.bin");

        when(incompleteFile.toPath()).thenReturn(incompleteFilePath);
        when(smallFile.toPath()).thenReturn(incompleteFilePath);
        when(largeFile.toPath()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.getParent()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.toAbsolutePath()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.normalize()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.resolve(any(Path.class))).thenReturn(incompleteFilePath);
        when(incompleteFilePath.resolve(anyString())).thenReturn(incompleteFilePath);
        when(incompleteFilePath.getFileSystem()).thenReturn(fileSystem);
        when(fileSystem.provider()).thenReturn(fsProvider);

        when(s3.initiateMultipartUpload(any())).thenReturn(mock(InitiateMultipartUploadResult.class));
    }

    @Test
    public void upload_smallFile() throws Exception {
        doThrow(IOException.class).when(fsProvider).checkAccess(any());

        fileUploadingService.upload(smallFile);

        verify(fileUploader).setStrategy(argThat(withClass(SmallFileUploadingStrategy.class)));
    }

    @Test
    public void upload_largeFile() throws Exception {
        doThrow(IOException.class).when(fsProvider).checkAccess(any());

        fileUploadingService.upload(largeFile);

        verify(fileUploader).setStrategy(argThat(withClass(LargeFileUploadingStrategy.class)));
    }

    @Test
    public void upload_incompleteFile_notStarted() throws Exception {
        when(listing.getMultipartUploads()).thenReturn(Collections.emptyList());
        when(s3.getObjectMetadata(anyString(), anyString())).thenThrow(NotFoundException.class);


        fileUploadingService.upload(largeFile);

        verify(fileUploader).setStrategy(argThat(withClass(UnfinishedUploadingFileUploadingStrategy.class)));

    }


    @Test
    public void upload_completeFile_alreadyStarted() throws Exception {
        when(listing.getMultipartUploads()).thenReturn(Collections.singletonList(upload));
        doThrow(IOException.class).when(fsProvider).checkAccess(any());
        when(s3.getObjectMetadata(anyString(), anyString())).thenThrow(NotFoundException.class);

        fileUploadingService.upload(incompleteFile);

        verify(fileUploader).setStrategy(argThat(withClass(UnfinishedUploadingFileUploadingStrategy.class)));
    }
}