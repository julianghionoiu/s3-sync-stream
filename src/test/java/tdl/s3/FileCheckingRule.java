package tdl.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.Getter;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Getter
public class FileCheckingRule extends ExternalResource {

    private Properties properties;

    private AmazonS3 amazonS3;
    private String bucketName;
    private String access_key;
    private String secret_key;
    private String region;

    public FileCheckingRule() {

        properties = loadProperties();

        AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(properties.getProperty("s3_region"))
                .build();

         bucketName = properties.getProperty("s3_bucket");
         access_key = properties.getProperty("aws_access_key_id");
         secret_key = properties.getProperty("aws_secret_access_key");
         region = properties.getProperty("s3_region");

    }

    public ObjectMetadata getObjectMetadata(String key) {
        try {
            return amazonS3.getObjectMetadata(bucketName, key);
        } catch (AmazonS3Exception e) {
            //return null if not found
            return null;
        }
    }

    public String commonArgs() {
        return "-a " + access_key + " -s " + secret_key + " -r " + region + " -b " + bucketName + " ";
    }

    public void deleteObjects(String... objects) {
        for (String object : objects) {
            amazonS3.deleteObject(bucketName, object);
        }
    }

    private Properties loadProperties() {
        String userHome = System.getProperty("user.home");
        Path propertiesPath = Paths.get(userHome, ".aws", "credentials");
        Properties properties = new Properties();
        try (InputStream inStream = Files.newInputStream(propertiesPath)) {
            properties.load(inStream);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
