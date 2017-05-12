package tdl.s3.upload;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import tdl.s3.rules.TemporarySyncFolder;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static tdl.s3.rules.TemporarySyncFolder.ONE_MEGABYTE;
import static tdl.s3.rules.TemporarySyncFolder.PART_SIZE_IN_BYTES;
import tdl.s3.sync.Destination;

@RunWith(MockitoJUnitRunner.class)
public class UnfinishedWritingFileUploadingStrategyTest {

    @Rule
    public TemporarySyncFolder targetSyncFolder = new TemporarySyncFolder();

    @Mock
    private AmazonS3 amazonS3;
    @Mock
    private PartListing partListing;
    @Mock
    private PartSummary partSummary;
    @Mock
    private InitiateMultipartUploadResult initiatingResult;
    @Mock
    private UploadPartResult uploadPartResult;
    @Mock
    private PartETag partETag;
    @Mock
    private MultipartUpload multipartUpload;
    @Mock
    private Destination destination;

    @Before
    public void setUp() throws Exception {
        //noinspection unchecked
        when(amazonS3.getObjectMetadata(anyString(), anyString())).thenThrow(NotFoundException.class);
        when(multipartUpload.getKey()).thenReturn("unfinished_writing_file_to_upload.bin");
        when(multipartUpload.getUploadId()).thenReturn("id");
        when(amazonS3.listParts(any())).thenReturn(partListing);
        when(amazonS3.initiateMultipartUpload(any())).thenReturn(initiatingResult);
        when(amazonS3.uploadPart(any())).thenReturn(uploadPartResult);
        when(uploadPartResult.getPartETag()).thenReturn(partETag);

        when(partListing.getParts()).thenReturn(Collections.emptyList());

        when(partSummary.getPartNumber()).thenReturn(0);
        when(partSummary.getSize()).thenReturn(10L * 1024 * 1024);
        when(destination.getClient()).thenReturn(amazonS3);
    }

    @Test
    public void upload_newlyCreatedButIncompleteFile() throws Exception {
        RemoteFile remoteFile = mock(RemoteFile.class);
        MultipartUploadFileUploadingStrategy strategy = spy(new MultipartUploadFileUploadingStrategy(destination));
        doReturn(null).when(strategy).findMultiPartUpload(any());
        
        FileUploader fileUploader = new FileUploaderImpl(destination, strategy);

        String fileName = "unfinished_writing_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);
        fileUploader.upload(targetSyncFolder.getFilePath(fileName).toFile(), remoteFile);

        //verify that uploading started
        verify(amazonS3, times(1)).initiateMultipartUpload(any());
        //verify that existing parts uploaded (10 MB - 2 parts)
        verify(amazonS3, times(2)).uploadPart(any());

        when(partListing.getParts()).thenReturn(Collections.singletonList(partSummary));
        //write additional  data and delete lock file
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE_IN_BYTES + ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);


        MultipartUploadFileUploadingStrategy newStrategy = spy(new MultipartUploadFileUploadingStrategy(destination, 1));
        doReturn(multipartUpload).when(newStrategy).findMultiPartUpload(any());
        FileUploader newFileUploader = new FileUploaderImpl(destination, newStrategy);
        //upload the rest of the file
        newFileUploader.upload(targetSyncFolder.getFilePath(fileName).toFile(), remoteFile);

        //verify that rest part uploaded (4 times means 2 from previous session and 2 current)
        verify(amazonS3, times(4)).uploadPart(any());
        //verify uploading completed
        verify(amazonS3, times(1)).completeMultipartUpload(any());
    }
}