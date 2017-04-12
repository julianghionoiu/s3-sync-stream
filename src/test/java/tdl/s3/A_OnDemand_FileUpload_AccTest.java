package tdl.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Properties;

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

    private AmazonS3 amazonS3;

    private String bucketName;

    private FileUploadingService fileUploadingService;

    @Before
    public void setUp() throws Exception {
        Properties properties = loadProperties();

        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(properties.getProperty("s3_region"))
                .build();

        bucketName = properties.getProperty("s3_bucket");
        fileUploadingService = new FileUploadingService(amazonS3, bucketName);

        //Delete previously uploaded files if present
        amazonS3.deleteObject(bucketName, "uploaded_once.txt");
        amazonS3.deleteObject(bucketName, "new_file_name.txt");
        amazonS3.deleteObject(bucketName, "large_file.bin");

    }

    private Properties loadProperties() throws IOException {
        String userHome = System.getProperty("user.home");
        Path propertiesPath = Paths.get(userHome, ".aws", "credentials");
        Properties properties = new Properties();
        try (InputStream inStream = Files.newInputStream(propertiesPath)) {
            properties.load(inStream);
            return properties;
        }
    }

    @Test
    public void should_not_upload_file_if_already_present() throws Exception {
        //Upload first file just to check in test that it will not be uploaded twice
        fileUploadingService.upload(new File("src/test/resources/sample_small_file_to_upload.txt"), "uploaded_once.txt");
        // Sleep 2 seconds to distinguish that file uploaded_once.txt on aws was not uploaded by next call
        Thread.sleep(2000);
        Instant uploadingTime = Instant.now();
        fileUploadingService.upload(new File("src/test/resources/sample_small_file_to_upload.txt"), "uploaded_once.txt");

        ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(bucketName, "uploaded_once.txt");
        Instant actualLastModifiedDate = objectMetadata.getLastModified().toInstant();

        //Check that file is older than last uploading start
        assertTrue(actualLastModifiedDate.isBefore(uploadingTime));
    }

    @Test
    public void should_upload_simple_file_to_bucket() throws Exception {
        fileUploadingService.upload(new File("src/test/resources/sample_small_file_to_upload.txt"), "new_file_name.txt");

        ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(bucketName, "new_file_name.txt");
        assertNotNull(objectMetadata);
    }

    @Test
    public void should_upload_large_file_to_bucket_using_multipart_upload() throws Exception {
        fileUploadingService.upload(new File("src/test/resources/large_file.bin"));

        ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(bucketName, "large_file.bin");
        assertNotNull(objectMetadata);
    }

}