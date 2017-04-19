package tdl.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.*;
import org.junit.rules.ExternalResource;

import java.time.Instant;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * To run this test you need to have file ~/.aws/credentials (for *nix systems)
 * or C:\Users\USERNAME\.aws\credentials (for windows).
 * File should contains next lines
 *      [default]
 *      aws_access_key_id=your_access_key_id
 *      aws_secret_access_key=your_secret_access_key
 *      s3_region=your_s3_aws_region
 *      s3_bucket=your_test_bucket_name
 *
 * This bucket will be cleared before tests run
 *
 */
public class A_OnDemand_FileUpload_AccTest {

    @Rule
    public FileCheckingRule fileChecking = new FileCheckingRule();

    @Before
    public void setUp() throws Exception {
        fileChecking.deleteObjects("uploaded_once.txt", "new_file_name.txt", "large_file.bin");
    }

    @Rule
    public ExternalResource resource = new DeleteRemoteObjectsRule(fileChecking);

    @Test
    public void should_not_upload_file_if_already_present() throws Exception {
        String[] uploadingArgs = (fileChecking.commonArgs() + "upload -f " + "src/test/resources/already_uploaded.txt").split(" ");

        //Upload first file just to check in test that it will not be uploaded twice
        SyncFileApp.main(uploadingArgs);
        // Sleep 2 seconds to distinguish that file uploaded_once.txt on aws was not uploaded by next call
        Thread.sleep(2000);
        Instant uploadingTime = Instant.now();
        SyncFileApp.main(uploadingArgs);

        ObjectMetadata objectMetadata = fileChecking.getObjectMetadata("already_uploaded.txt");
        Instant actualLastModifiedDate = objectMetadata.getLastModified().toInstant();

        //Check that file is older than last uploading start
        assertTrue(actualLastModifiedDate.isBefore(uploadingTime));
    }

    @Test
    public void should_upload_simple_file_to_bucket() throws Exception {
        String[] uploadingArgs = (fileChecking.commonArgs() + "upload -f " + "src/test/resources/sample_small_file_to_upload.txt").split(" ");
        SyncFileApp.main(uploadingArgs);

        ObjectMetadata objectMetadata = fileChecking.getObjectMetadata("sample_small_file_to_upload.txt");
        assertNotNull(objectMetadata);
    }

    @Test
    public void should_upload_large_file_to_bucket_using_multipart_upload() throws Exception {
        String[] uploadingArgs = (fileChecking.commonArgs() + "upload -f " + "src/test/resources/large_file.bin").split(" ");
        SyncFileApp.main(uploadingArgs);

        ObjectMetadata objectMetadata = fileChecking.getObjectMetadata("large_file.bin");
        assertNotNull(objectMetadata);
    }

}