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
import tdl.s3.TempFileRule;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UnfinishedWritingFileUploadingStrategyTest {

    @Rule
    public TempFileRule tempFileRule = new TempFileRule();

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
    private MultipartUpload multipartUpload;

    @Before
    public void setUp() throws Exception {
        File filePrototype = new File("src/test/resources/unfinished_writing_file.bin");
        Files.write(tempFileRule.getLockFile().toPath(), new byte[]{0});
        Files.copy(filePrototype.toPath(), tempFileRule.getFileToUpload().toPath());

        when(amazonS3.getObjectMetadata(anyString(), anyString())).thenThrow(NotFoundException.class);
        when(multipartUpload.getKey()).thenReturn("unfinished_writing_file_to_upload.bin");
        when(multipartUpload.getUploadId()).thenReturn("id");
        when(amazonS3.listParts(any())).thenReturn(partListing);
        when(amazonS3.initiateMultipartUpload(any())).thenReturn(initiatingResult);
        when(amazonS3.uploadPart(any())).thenReturn(uploadPartResult);

        when(partListing.getParts()).thenReturn(Collections.emptyList());

        when(partSummary.getPartNumber()).thenReturn(0);
        when(partSummary.getSize()).thenReturn(10L * 1024 * 1024);
    }



    @Test
    public void upload_newlyCreatedButIncompleteFile() throws Exception {
        UnfinishedUploadingFileUploadingStrategy strategy = new UnfinishedUploadingFileUploadingStrategy(null);
        FileUploader fileUploader = new FileUploaderImpl(amazonS3, "bucket", strategy);

        fileUploader.upload(tempFileRule.getFileToUpload());

        //verify that uploading started
        verify(amazonS3, times(1)).initiateMultipartUpload(any());
        //verify that existing parts uploaded (10 MB - 2 parts)
        verify(amazonS3, times(2)).uploadPart(any());

        when(partListing.getParts()).thenReturn(Collections.singletonList(partSummary));
        //write additional  data and delete lock file
        tempFileRule.writeDataToFile(tempFileRule.getFileToUpload());
        Files.delete(tempFileRule.getLockFile().toPath());


        UnfinishedUploadingFileUploadingStrategy newStrategy = new UnfinishedUploadingFileUploadingStrategy(multipartUpload);
        FileUploader newFileUploader = new FileUploaderImpl(amazonS3, "bucket", newStrategy);
        //upload the rest of the file
        newFileUploader.upload(tempFileRule.getFileToUpload());

        //verify that rest part uploaded (4 times means 2 from previous session and 2 current)
        verify(amazonS3, times(4)).uploadPart(any());
        //verify uploading completed
        verify(amazonS3, times(1)).completeMultipartUpload(any());
    }
}