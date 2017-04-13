package tdl.s3;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import tdl.s3.sync.FolderSynchronizer;
import tdl.s3.upload.FileUploadingService;

import java.nio.file.Paths;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * @author vdanyliuk
 * @version 13.04.17.
 */
@Ignore
@RunWith(PowerMockRunner.class)
@PrepareForTest({FolderSynchronizer.class, FileUploadingService.class, AmazonS3ClientBuilder.class, SyncFileApp.class})
public class SyncFileAppTest {

    private FileUploadingService fileUploadingService;

    private FolderSynchronizer folderSynchronizer;

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(FileUploadingService.class);
        PowerMockito.mockStatic(FolderSynchronizer.class);
        PowerMockito.mockStatic(AmazonS3ClientBuilder.class);

        fileUploadingService = mock(FileUploadingService.class);
        folderSynchronizer = mock(FolderSynchronizer.class);
        AmazonS3ClientBuilder builder = mock(AmazonS3ClientBuilder.class);

        whenNew(AmazonS3ClientBuilder.class).withAnyArguments().thenReturn(builder);
        when(AmazonS3ClientBuilder.standard()).thenReturn(builder);
        when(builder.withCredentials(any())).thenReturn(builder);
        when(builder.withRegion(anyString())).thenReturn(builder);
        when(builder.build()).thenReturn(null);

        whenNew(FileUploadingService.class).withAnyArguments().thenReturn(fileUploadingService);
        whenNew(FolderSynchronizer.class).withAnyArguments().thenReturn(folderSynchronizer);

    }

    @Test
    public void main_uploadCommand() throws Exception {

        String[] args = new String[]{"-a", "key", "-s", "secret", "-r", "eu-west-1", "-b", "test-bucket", "upload", "-f", "testfile.txt"};

        SyncFileApp.main(args);

        Mockito.verify(fileUploadingService).upload(any());
    }

    @Test
    public void main_syncCommand_recursive() throws Exception {

        String[] args = new String[]{"-a", "key", "-s", "secret", "-r", "eu-west-1", "-b", "test-bucket", "sync", "-d", "test_dir", "-R"};

        SyncFileApp.main(args);

        Mockito.verify(folderSynchronizer).synchronize(eq(Paths.get("test_dir")), eq(true));
    }

    @Test
    public void main_syncCommand_nonRecursive() throws Exception {

        String[] args = new String[]{"-a", "key", "-s", "secret", "-r", "eu-west-1", "-b", "test-bucket", "sync", "-d", "test_dir"};

        SyncFileApp.main(args);

        Mockito.verify(folderSynchronizer).synchronize(eq(Paths.get("test_dir")), eq(false));
    }

}