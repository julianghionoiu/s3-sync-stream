package tdl.s3.rules;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.junit.rules.ExternalResource;
import tdl.s3.credentials.AWSSecretsProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class RemoteTestBucket extends ExternalResource {
    private final AmazonS3 amazonS3;
    private final String bucketName;

    //~~~~ Construct

    public RemoteTestBucket() {
        Path privatePropertiesFile = Paths.get(".private", "aws-test-secrets");
        AWSSecretsProvider secretsProvider = new AWSSecretsProvider(privatePropertiesFile);

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(secretsProvider)
                .withRegion(secretsProvider.getS3Region())
                .build();

        bucketName = secretsProvider.getS3Bucket();
    }

    //~~~~ Lifecycle management

    @Override
    protected void before() {
        abortAllMultipartUploads();
        removeAllObjects();
    }

    private void removeAllObjects() {
        amazonS3.listObjects(bucketName)
                .getObjectSummaries()
                .forEach(s3ObjectSummary -> {
                    DeleteObjectRequest request = new DeleteObjectRequest(bucketName, s3ObjectSummary.getKey());
                    amazonS3.deleteObject(request);
                });
    }

    private void abortAllMultipartUploads() {
        amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(bucketName))
                .getMultipartUploads()
                .forEach(upload -> {
                    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(bucketName, upload.getKey(), upload.getUploadId());
                    amazonS3.abortMultipartUpload(request);
                });
    }

    //~~~~ Bucket actions

    public boolean doesObjectExists(String key) {
        return amazonS3.doesObjectExist(bucketName, key);
    }

    public ObjectMetadata getObjectMetadata(String key) {
        return amazonS3.getObjectMetadata(bucketName, key);
    }

    public Optional<MultipartUpload> getMultipartUploadFor(String key) {
        return amazonS3.listMultipartUploads(new ListMultipartUploadsRequest(bucketName))
                .getMultipartUploads().stream()
                .filter(upl -> upl.getKey().equals(key))
                .findAny();
    }

    public List<PartSummary> getPartsFor(MultipartUpload multipartUpload) {
        return amazonS3.listParts(new ListPartsRequest(bucketName, multipartUpload.getKey(), multipartUpload.getUploadId())).getParts();
    }

}
