package tdl.s3.upload;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UnfinishedWritingFileUploadingStrategyTest {

    @Mock
    private AmazonS3 amazonS3;

    private File fileToUpload;
    private File lockFile;

    private FileUploader fileUploader;

    @Mock
    private MultipartUploadListing uploadListing;
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
        deleteFiles();
        File filePrototype = new File("src/test/resources/unfinished_writing_file.bin");
        fileToUpload = new File("src/test/resources/unfinished_writing_file_to_upload.bin");
        lockFile = new File("src/test/resources/unfinished_writing_file_to_upload.bin.lock");
        Files.write(lockFile.toPath(), new byte[]{0});
        Files.copy(filePrototype.toPath(), fileToUpload.toPath());
        UnfinishedUploadingFileUploadingStrategy strategy = new UnfinishedUploadingFileUploadingStrategy();
        fileUploader = new FileUploaderImpl(amazonS3, "bucket", strategy);
        when(amazonS3.getObjectMetadata(anyString(), anyString())).thenThrow(NotFoundException.class);
        when(amazonS3.listMultipartUploads(any())).thenReturn(uploadListing);
        when(uploadListing.getMultipartUploads()).thenReturn(Collections.emptyList());
        when(multipartUpload.getKey()).thenReturn("unfinished_writing_file_to_upload.bin");
        when(multipartUpload.getUploadId()).thenReturn("id");
        when(amazonS3.listParts(any())).thenReturn(partListing);
        when(amazonS3.initiateMultipartUpload(any())).thenReturn(initiatingResult);
        when(amazonS3.uploadPart(any())).thenReturn(uploadPartResult);

        when(partListing.getParts()).thenReturn(Collections.emptyList());

        when(partSummary.getPartNumber()).thenReturn(0);
        when(partSummary.getSize()).thenReturn(10L * 1024 * 1024);
    }

    private void writeDataToFile(File file) {
        try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
            byte[] part = new byte[6 * 1024 * 1024];
            fileOutputStream.write(part);
        } catch (IOException e) {
            throw new RuntimeException("Test interrupted.", e);
        }
    }

    @Test
    public void upload() throws Exception {
        fileUploader.upload(fileToUpload);

        //verify that uploading started
        verify(amazonS3, times(1)).initiateMultipartUpload(any());
        //verify that existing parts uploaded (10 MB - 2 parts)
        verify(amazonS3, times(2)).uploadPart(any());

        when(uploadListing.getMultipartUploads()).thenReturn(Collections.singletonList(multipartUpload));
        when(partListing.getParts()).thenReturn(Collections.singletonList(partSummary));
        //write additional  data and delete lock file
        writeDataToFile(fileToUpload);
        Files.delete(lockFile.toPath());

        //upload the rest of the file
        fileUploader.upload(fileToUpload);

        //verify that rest part uploaded (3 times means 2 from previous session and 1 current)
        verify(amazonS3, times(3)).uploadPart(any());
        //verify uploading completed
        verify(amazonS3, times(1)).completeMultipartUpload(any());
    }

    @After
    public void tearDown() throws Exception {
        deleteFiles();
    }

    private void deleteFiles() {
        try {
            Files.delete(fileToUpload.toPath());
            Files.delete(lockFile.toPath());
        } catch (Exception ignored) {
        }
    }
}