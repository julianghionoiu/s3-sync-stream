package tdl.s3;

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import org.junit.rules.ExternalResource;

/**
 * @author vdanyliuk
 * @version 19.04.17.
 */
class DeleteRemoteObjectsRule extends ExternalResource {

    private final FileCheckingRule fileChecking;

    public DeleteRemoteObjectsRule(FileCheckingRule fileChecking) {
        this.fileChecking = fileChecking;
    }

    @Override
    protected void before() {
        fileChecking.getAmazonS3().listMultipartUploads(new ListMultipartUploadsRequest(fileChecking.getBucketName()))
                .getMultipartUploads()
                .forEach(upload -> {
                    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(fileChecking.getBucketName(), upload.getKey(), upload.getUploadId());
                    fileChecking.getAmazonS3().abortMultipartUpload(request);
                });

        fileChecking.getAmazonS3().listObjects(fileChecking.getBucketName())
                .getObjectSummaries()
                .forEach(s3ObjectSummary -> {
                    DeleteObjectRequest request = new DeleteObjectRequest(fileChecking.getBucketName(), s3ObjectSummary.getKey());
                    fileChecking.getAmazonS3().deleteObject(request);
                });
    }
}
