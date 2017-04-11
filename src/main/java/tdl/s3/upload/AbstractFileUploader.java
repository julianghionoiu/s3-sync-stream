package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
public abstract class AbstractFileUploader implements FileUploader {

    public static int RETRY_TIMES_COUNT = 2;

    private final AmazonS3 s3Provider;
    private final String bucket;

    public AbstractFileUploader(final AmazonS3 s3Provider, String bucket) {
        this.s3Provider = s3Provider;
        this.bucket = bucket;
    }

    @Override
    public void upload(File file) {
        upload(file, file.getName());
    }

    @Override
    public void upload(File file, String newName) {
        upload(file, newName, RETRY_TIMES_COUNT);
    }

    @Override
    public boolean exists(String bucketName, String fileKey) {
        ObjectListing objectListing = s3Provider.listObjects(bucketName);
        return Stream.of(objectListing)
                .flatMap(this::getNextListing)
                .map(ObjectListing::getObjectSummaries)
                .flatMap(Collection::stream)
                .map(S3ObjectSummary::getKey)
                .anyMatch(fileKey::equals);
    }

    private Stream<ObjectListing> getNextListing(ObjectListing objectListing) {
        if (objectListing.isTruncated()) {
            return Stream.of(objectListing);
        } else {
            ObjectListing nextListing = s3Provider.listNextBatchOfObjects(objectListing);
            return Stream.concat(Stream.of(objectListing), getNextListing(nextListing));
        }
    }

    private void upload(File file, String newName, int retry) {
        try {
            if (!exists(bucket, newName)) {
                uploadInternal(s3Provider, bucket, file, newName);
            }
        } catch (Exception e) {
            if (retry == 0) {
                throw new UploadingException("Can't upload file " + file + " due to error " + e.getMessage(), e);
            } else {
                upload(file, newName, retry - 1);
            }
        }
    }

    protected abstract void uploadInternal(AmazonS3 s3, String bucket, File file, String newName) throws InterruptedException, Exception;
}
