package tdl.s3.upload;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import org.hamcrest.Matcher;
import org.hamcrest.beans.HasPropertyWithValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileUploadingService.class})
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
    private MultipartUploadListing secondListing;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private FileSystemProvider fsProvider;
    @Mock
    private MultipartUpload upload;
    @Mock
    private MultipartUpload secondUpload;
    @Mock
    private BasicFileAttributes fileAttributes;
    @Mock
    private SmallFileUploadingStrategy smallFileUploadingStrategy;
    @Mock
    private MultiPartUploadFileUploadingStrategy multiPartUploadFileUploadingStrategy;

    @Before
    public void setUp() throws Exception {
        fileUploadingService = new FileUploadingService(new HashMap<Integer,UploadingStrategy>() {{
            put(1, smallFileUploadingStrategy);
            put(Integer.MAX_VALUE, multiPartUploadFileUploadingStrategy);

        }}, s3, "testBucket", "testPrefix");

        when(smallFile.length()).thenReturn(1255L);
        when(largeFile.length()).thenReturn(6L * 1024 * 1024);
        when(incompleteFile.length()).thenReturn(6L * 1024 * 1024);
        when(s3.listMultipartUploads(any())).thenReturn(listing);

        when(incompleteFile.getName()).thenReturn("incomplete.bin");
        when(upload.getKey()).thenReturn("incomplete.bin");

        when(incompleteFile.toPath()).thenReturn(incompleteFilePath);
        when(incompleteFile.getPath()).thenReturn("incompleteFilePath.bin");
        when(smallFile.toPath()).thenReturn(incompleteFilePath);
        when(largeFile.toPath()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.getParent()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.toAbsolutePath()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.normalize()).thenReturn(incompleteFilePath);
        when(incompleteFilePath.resolve(any(Path.class))).thenReturn(incompleteFilePath);
        when(incompleteFilePath.resolve(anyString())).thenReturn(incompleteFilePath);
        when(incompleteFilePath.getFileSystem()).thenReturn(fileSystem);
        when(fileSystem.provider()).thenReturn(fsProvider);
        when(fsProvider.readAttributes(any(), any(Class.class), anyVararg())).thenReturn(fileAttributes);

        when(s3.initiateMultipartUpload(any())).thenReturn(mock(InitiateMultipartUploadResult.class));

        when(s3.getObjectMetadata(anyString(), anyString())).thenThrow(NotFoundException.class);

        whenNew(MultiPartUploadFileUploadingStrategy.class).withAnyArguments().thenReturn(multiPartUploadFileUploadingStrategy);
    }

    @Test
    public void upload_smallFile() throws Exception {
        doThrow(IOException.class).when(fsProvider).checkAccess(any());
        when(fileAttributes.size()).thenReturn(1024L);

        fileUploadingService.upload(smallFile);

        verify(smallFileUploadingStrategy).upload(any(), any(), any(), any(), any());
    }

    @Test
    public void upload_largeFile() throws Exception {
        doThrow(IOException.class).when(fsProvider).checkAccess(any());
        when(fileAttributes.size()).thenReturn(10 * 1024 * 1024L);

        fileUploadingService.upload(largeFile);

        verify(multiPartUploadFileUploadingStrategy).upload(any(), any(), any(), any(), any());
    }

    @Test
    public void upload_incompleteFile_notStarted() throws Exception {
        when(listing.getMultipartUploads()).thenReturn(Collections.emptyList());

        fileUploadingService.upload(incompleteFile);

        verify(multiPartUploadFileUploadingStrategy).upload(any(), any(), any(), any(), any());
    }


    @Test
    public void upload_completeFile_alreadyStarted() throws Exception {
        when(listing.getMultipartUploads()).thenReturn(Collections.singletonList(upload));
        doThrow(IOException.class).when(fsProvider).checkAccess(any());

        fileUploadingService.upload(incompleteFile);

        verify(multiPartUploadFileUploadingStrategy).upload(any(), any(), any(), any(), any());
    }

    @Test
    public void upload_lot_alreadyStartedUploads() throws Exception {
        when(listing.getMultipartUploads()).thenReturn(Collections.singletonList(upload));
        when(secondListing.getMultipartUploads()).thenReturn(Collections.singletonList(secondUpload));
        when(upload.getKey()).thenReturn("anotherFile.bin");
        when(secondUpload.getKey()).thenReturn("incomplete.bin");
        when(listing.isTruncated()).thenReturn(true);
        when(listing.getNextUploadIdMarker()).thenReturn("NEXT_ID");
        when(s3.listMultipartUploads(argThat(hasProperty("uploadIdMarker", equalTo("NEXT_ID"))))).thenReturn(secondListing);
        doThrow(IOException.class).when(fsProvider).checkAccess(any());

        fileUploadingService.upload(incompleteFile);

        verify(multiPartUploadFileUploadingStrategy).upload(any(), any(), any(), any(), any());
    }
}