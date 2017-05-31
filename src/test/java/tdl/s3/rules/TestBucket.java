package tdl.s3.rules;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.junit.rules.ExternalResource;
import static tdl.s3.rules.TemporarySyncFolder.PART_SIZE_IN_BYTES;
import tdl.s3.sync.destination.DebugDestination;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.S3BucketDestination;

abstract public class TestBucket extends ExternalResource {

    protected AmazonS3 amazonS3;
    protected String bucketName;
    protected String uploadPrefix;

    //~~~~ Getters
    public Destination asDestination() {
        S3BucketDestination remoteDestination = S3BucketDestination.builder()
                .awsClient(amazonS3)
                .bucket(bucketName)
                .prefix(uploadPrefix)
                .build();
        return new DebugDestination(remoteDestination);
    }

    //~~~~ Lifecycle management
    @Override
    protected void before() {
        abortAllMultipartUploads();
        removeAllObjects();
    }

    public AmazonS3 getAmazonS3() {
        return amazonS3;
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

    public List<PartSummary> getPartsForKey(String key) {
        ListMultipartUploadsRequest multipartUploadsRequest = new ListMultipartUploadsRequest(bucketName);
        multipartUploadsRequest.setPrefix(uploadPrefix);
        MultipartUpload upload = amazonS3.listMultipartUploads(multipartUploadsRequest)
                .getMultipartUploads()
                .stream()
                .findFirst()
                .orElse(null);
        if (upload == null) {
            return null;
        } else {
            return getPartsFor(upload);
        }
    }

    public List<PartSummary> getPartsFor(MultipartUpload multipartUpload) {
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName,
                multipartUpload.getKey(), multipartUpload.getUploadId());
        return amazonS3.listParts(listPartsRequest).getParts();
    }

    @SuppressWarnings("SameParameterValue")
    public void upload(String key, Path path) {
        PutObjectRequest objectRequest = new PutObjectRequest(bucketName, uploadPrefix + key, path.toFile());
        amazonS3.putObject(objectRequest);
    }

    public void uploadFilesInsideDir(Path dir) {
        Arrays.stream(dir.toFile().listFiles())
                .filter(file -> file.isFile())
                .forEach(file -> upload(file.getName(), file.toPath()));
    }

    public String initiateMultipartUpload(String name) {
        InitiateMultipartUploadResult result = amazonS3.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(bucketName, uploadPrefix + name)
        );
        return result.getUploadId();
    }

    public String getBucketName() {
        return bucketName;
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
