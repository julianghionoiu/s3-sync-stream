package tdl.s3.rules;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import java.io.ByteArrayInputStream;
import lombok.Getter;
import org.junit.rules.ExternalResource;
import tdl.s3.credentials.AWSSecretsProvider;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import static tdl.s3.rules.TemporarySyncFolder.PART_SIZE_IN_BYTES;

@Getter
public class RemoteTestBucket extends ExternalResource {
    private final AmazonS3 amazonS3;
    private final String bucketName;
    private final String uploadPrefix;

    //~~~~ Construct

    public RemoteTestBucket() {
        Path privatePropertiesFile = Paths.get(".private", "aws-test-secrets");
        AWSSecretsProvider secretsProvider = new AWSSecretsProvider(privatePropertiesFile);

        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(secretsProvider)
                .withRegion(secretsProvider.getS3Region())
                .build();

        bucketName = secretsProvider.getS3Bucket();
        uploadPrefix = secretsProvider.getS3Prefix();
    }

    //~~~~ Lifecycle management

    @Override
    protected void before() {
        abortAllMultipartUploads();
        removeAllObjects();
    }

    private void removeAllObjects() {
        amazonS3.listObjects(bucketName, uploadPrefix)
                .getObjectSummaries()
                .forEach(s3ObjectSummary -> {
                    DeleteObjectRequest request = new DeleteObjectRequest(bucketName, s3ObjectSummary.getKey());
                    amazonS3.deleteObject(request);
                });
    }

    private void abortAllMultipartUploads() {
        ListMultipartUploadsRequest multipartUploadsRequest = new ListMultipartUploadsRequest(bucketName);
        multipartUploadsRequest.setPrefix(uploadPrefix);
        amazonS3.listMultipartUploads(multipartUploadsRequest)
                .getMultipartUploads()
                .forEach(upload -> {
                    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(bucketName, upload.getKey(), upload.getUploadId());
                    amazonS3.abortMultipartUpload(request);
                });
    }

    //~~~~ Bucket actions

    public boolean doesObjectExists(String key) {
        return amazonS3.doesObjectExist(bucketName, uploadPrefix + key);
    }

    public ObjectMetadata getObjectMetadata(String key) {
        return amazonS3.getObjectMetadata(bucketName, uploadPrefix + key);
    }

    public Optional<MultipartUpload> getMultipartUploadFor(String key) {
        ListMultipartUploadsRequest multipartUploadsRequest = new ListMultipartUploadsRequest(bucketName);
        multipartUploadsRequest.setPrefix(uploadPrefix);
        return amazonS3.listMultipartUploads(multipartUploadsRequest)
                .getMultipartUploads().stream()
                .filter(upl -> upl.getKey().equals(uploadPrefix + key))
                .findAny();
    }

    public List<PartSummary> getPartsFor(MultipartUpload multipartUpload) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName,
                multipartUpload.getKey(), multipartUpload.getUploadId());
        return amazonS3.listParts(listPartsRequest).getParts();
    }

    public void upload(String key, Path path) {
        PutObjectRequest objectRequest = new PutObjectRequest(bucketName, uploadPrefix + key, path.toFile());
        amazonS3.putObject(objectRequest);
    }

    public String initiateMultipartUpload(String name) {
        InitiateMultipartUploadResult result = amazonS3.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(bucketName, uploadPrefix + name)
        );
        return result.getUploadId();
    }

    public void uploadPart(String name, String uploadId, byte[] partData, int partNumber) throws NoSuchAlgorithmException {
        UploadPartRequest request = new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(uploadPrefix + name)
                .withPartNumber(partNumber)
                .withMD5Digest(Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(partData)))
                .withPartSize(PART_SIZE_IN_BYTES)
                .withUploadId(uploadId)
                .withInputStream(new ByteArrayInputStream(partData));
        amazonS3.uploadPart(request);
    }

}
