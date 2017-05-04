package tdl.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.RemoteTestBucket;

import java.time.Instant;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class A_OnDemand_FileUpload_AccTest {

    @Rule
    public RemoteTestBucket remoteTestBucket = new RemoteTestBucket();

    @Test
    public void should_not_upload_file_if_already_present() throws Exception {
        String[] uploadingArgs = ("-c " + ".private/aws-test-secrets" + " upload -f " + "src/test/resources/already_uploaded.txt").split(" ");

        //Upload first file just to check in test that it will not be uploaded twice
        SyncFileApp.main(uploadingArgs);
        // Sleep 2 seconds to distinguish that file uploaded_once.txt on aws was not uploaded by next call
        Thread.sleep(2000);
        Instant uploadingTime = Instant.now();
        SyncFileApp.main(uploadingArgs);

        ObjectMetadata objectMetadata = remoteTestBucket.getObjectMetadata("already_uploaded.txt");
        Instant actualLastModifiedDate = objectMetadata.getLastModified().toInstant();

        //Check that file is older than last uploading start
        assertTrue(actualLastModifiedDate.isBefore(uploadingTime));
    }

    @Test
    public void should_upload_simple_file_to_bucket() throws Exception {
        String[] uploadingArgs = ("-c " + ".private/aws-test-secrets" + "upload -f " + "src/test/resources/sample_small_file_to_upload.txt").split(" ");
        SyncFileApp.main(uploadingArgs);

        assertThat(remoteTestBucket.doesObjectExists("sample_small_file_to_upload.txt"), is(true));
    }

    @Test
    public void should_upload_large_file_to_bucket_using_multipart_upload() throws Exception {
        String[] uploadingArgs = ("-c " + ".private/aws-test-secrets" + "upload -f " + "src/test/resources/large_file.bin").split(" ");
        SyncFileApp.main(uploadingArgs);

        assertThat(remoteTestBucket.doesObjectExists("large_file.bin"), is(true));
    }

}