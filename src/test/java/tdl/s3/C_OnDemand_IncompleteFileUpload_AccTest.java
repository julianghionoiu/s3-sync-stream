package tdl.s3;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.*;
import org.junit.rules.ExternalResource;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class C_OnDemand_IncompleteFileUpload_AccTest {

    @ClassRule
    public static FileCheckingRule fileChecking = new FileCheckingRule();

    @ClassRule
    public static TempFileRule tempFileRule = new TempFileRule();

    /*
    Rule to delete all uploaded objects and multipart uploads
     */
    @ClassRule
    public static ExternalResource resource = new ExternalResource() {
        @Override
        protected void after() {
            fileChecking.getAmazonS3().listMultipartUploads(new ListMultipartUploadsRequest(fileChecking.getBucketName()))
                    .getMultipartUploads()
                    .forEach(upload -> {
                        AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(fileChecking.getBucketName(), upload.getKey(), upload.getUploadId());
                        fileChecking.getAmazonS3().abortMultipartUpload(request);
                    });

            fileChecking.getAmazonS3().listObjects(fileChecking.getBucketName())
                    .getObjectSummaries()
                    .forEach(s3ObjectSummary -> {
                        DeleteObjectRequest request = new DeleteObjectRequest(fileChecking.getBucketName(), s3ObjectSummary.getKey());
                        fileChecking.getAmazonS3().deleteObject(request);
                    });
        }
    };

    /**
     * NOTES
     *
     * This is where it gets interesting. :)
     * We want to start uploading the video recording as soon as it starts being generated.
     *
     * I am using https://github.com/julianghionoiu/dev-screen-record to generate the recording and
     * it is configured such that information is always appended to the end of the video and never changed.
     * Internally, the video recording is composed of small independent parts and can be played even if the
     * recording has not stopped.
     * This way we can upload chunks of video as it is being generated.
     *
     * The tricky bit is to detect when the recording has completed and the upload can be finalised.
     */


    @Test
    public void a_should_upload_incomplete_file() throws Exception {
        //Create file to upload and lock file
        File filePrototype = new File("src/test/resources/unfinished_writing_file.bin");
        Files.write(tempFileRule.getLockFile().toPath(), new byte[]{0});
        Files.copy(filePrototype.toPath(), tempFileRule.getFileToUpload().toPath());

        //Start uploading the file
        String[] uploadingArgs = (fileChecking.commonArgs() + "upload -f " + tempFileRule.getFileToUpload()).split(" ");
        SyncFileApp.main(uploadingArgs);

        //Check that the file still not exists on the server
        ObjectMetadata objectMetadata = fileChecking.getObjectMetadata(tempFileRule.getFileToUpload().getName());
        assertNull(objectMetadata);
        //Check that multipart upload started
        boolean started = fileChecking.isMultipartUploadExists(tempFileRule.getFileToUpload().getName());
        assertTrue(started);
    }


    @Test
    public void b_should_be_able_to_continue_incomplete_file_and_finalise() throws Exception {

        //write additional data and delete lock file
        tempFileRule.writeDataToFile(tempFileRule.getFileToUpload());
        Files.delete(tempFileRule.getLockFile().toPath());

        //Start uploading the rest of the file
        String[] uploadingArgs = (fileChecking.commonArgs() + "upload -f " + tempFileRule.getFileToUpload()).split(" ");
        SyncFileApp.main(uploadingArgs);

        //Check that the file exists on the server
        ObjectMetadata objectMetadata = fileChecking.getObjectMetadata(tempFileRule.getFileToUpload().getName());
        assertNotNull(objectMetadata);
        //Check that multipart upload completed and not exists anymore
        boolean exists = fileChecking.isMultipartUploadExists(tempFileRule.getFileToUpload().getName());
        assertFalse(exists);

    }

}