package tdl.s3.upload;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


import java.io.File;
import java.util.Collections;

import static junit.framework.TestCase.assertNotNull;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
@RunWith(MockitoJUnitRunner.class)
public class SimpleFileUploaderTest {

    @InjectMocks
    private SimpleFileUploader fileUploader;

    @Mock
    private AmazonS3 amazonS3;

    @Mock
    private ObjectListing objectListing1;

    @Mock
    private ObjectListing objectListing2;

    @Mock
    private S3ObjectSummary objectSummary1;

    @Mock
    private S3ObjectSummary objectSummary2;

    @Before
    public void setUp() throws Exception {
        when(amazonS3.listNextBatchOfObjects(objectListing1)).thenReturn(objectListing2);
        when(amazonS3.listObjects(anyString())).thenReturn(objectListing1);

        when(objectListing1.isTruncated()).thenReturn(true);
        when(objectListing2.isTruncated()).thenReturn(false);

        when(objectListing1.getObjectSummaries()).thenReturn(Collections.singletonList(objectSummary1));
        when(objectListing2.getObjectSummaries()).thenReturn(Collections.singletonList(objectSummary2));

        when(objectSummary1.getKey()).thenReturn("file1");
        when(objectSummary2.getKey()).thenReturn("file2");

    }

    @Test
    public void upload_retryAfterError() throws Exception {
        when(amazonS3.putObject(anyString(), anyString(), any(File.class))).thenThrow(SdkClientException.class);
        try {
            fileUploader.upload(new File("test.file"));
        } catch (Exception e) {
            assertNotNull(e);
        }
        verify(amazonS3, times(3)).putObject(anyString(), anyString(), any(File.class));
    }

    @Test
    public void upload_doNotUploadIfFileAlreadyExists() throws Exception {
        fileUploader.upload(new File("file1"));

        verify(amazonS3, never()).putObject(anyString(), anyString(), any(File.class));
    }

    @Test
    public void upload_notExisting() throws Exception {
        fileUploader.upload(new File("file3"));

        verify(amazonS3, times(1)).putObject(anyString(), anyString(), any(File.class));
    }

}