package tdl.s3.testframework.rules;

import tdl.s3.credentials.AWSSecretProperties;

import java.nio.file.Path;
import java.nio.file.Paths;

public class RemoteTestBucket extends TestBucket {

    public RemoteTestBucket() {
        Path privatePropertiesFile = Paths.get(".private", "aws-test-secrets");
        AWSSecretProperties secretsProvider = AWSSecretProperties.fromPlainTextFile(privatePropertiesFile);

        amazonS3 = secretsProvider.createClient();
        bucketName = secretsProvider.getS3Bucket();
        uploadPrefix = secretsProvider.getS3Prefix();
    }

}
