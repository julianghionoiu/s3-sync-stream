package tdl.s3.credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Read credentials and bucket information from private properties file.
 *
 * The file should contain the following keys:
 *  - aws_access_key_id
 *  - aws_secret_access_key
 *  - s3_region
 *  - s3_bucket
 */
public class AWSSecretProperties {
    private Properties privateProperties;

    private AWSSecretProperties(Properties privateProperties) {
        this.privateProperties = privateProperties;
    }

    public static AWSSecretProperties fromPlainTextFile(Path plainTextPropertyFile) {
        return new AWSSecretProperties(loadPrivateProperties(plainTextPropertyFile));
    }
    
    public static AWSSecretProperties fromProperties(Properties privateProperties) {
        return new AWSSecretProperties(privateProperties);
    }

    public AmazonS3 createClient() {
        String awsAccessKeyId = privateProperties.getProperty("aws_access_key_id");
        String awsSecretAccessKey = privateProperties.getProperty("aws_secret_access_key");
        String awsSessionToken = privateProperties.getProperty("aws_session_token");
        String s3Region = privateProperties.getProperty("s3_region");

        AWSCredentials awsCredentials;
        if (awsSessionToken != null) {
            awsCredentials = new BasicSessionCredentials(
                    awsAccessKeyId,
                    awsSecretAccessKey,
                    awsSessionToken);
        } else {
            awsCredentials = new BasicAWSCredentials(
                    awsAccessKeyId,
                    awsSecretAccessKey);
        }

        return AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(s3Region)
                .build();
    }

    public String getS3Bucket() {
        return privateProperties.getProperty("s3_bucket");
    }

    public String getS3Prefix() {
        return privateProperties.getProperty("s3_prefix");
    }

    //~~~ Util

    private static Properties loadPrivateProperties(Path privatePropertiesPath) {
        Properties properties = new Properties();
        try (InputStream inStream = Files.newInputStream(privatePropertiesPath)) {
            properties.load(inStream);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
