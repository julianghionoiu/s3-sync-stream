package tdl.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Before;
import tdl.s3.upload.FileUploadingService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * @author vdanyliuk
 * @version 13.04.17.
 */
public class AbstractAcceptanceTestCase {

    AmazonS3 amazonS3;

    String bucketName;

    FileUploadingService fileUploadingService;

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

        additionalSetUp();
    }

    protected void additionalSetUp() {
        //to implement in child
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

    ObjectMetadata getObjectMetadata(String key) {
        try {
            return amazonS3.getObjectMetadata(bucketName, key);
        } catch (AmazonS3Exception e) {
            //return null if not found
            return null;
        }
    }
}
