package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * @author vdanyliuk
 * @version 11.04.17.
 */
@Slf4j
public class FileUploaderImpl implements FileUploader {

    public static int RETRY_TIMES_COUNT = 2;

    private final AmazonS3 s3Provider;
    private final String bucket;
    private final UploadingStrategy uploadingStrategy;

    public FileUploaderImpl(final AmazonS3 s3Provider, String bucket, UploadingStrategy uploadingStrategy) {
        this.s3Provider = s3Provider;
        this.bucket = bucket;
        this.uploadingStrategy = uploadingStrategy;
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
        if (!objectListing.isTruncated()) {
            return Stream.of(objectListing);
        } else {
            ObjectListing nextListing = s3Provider.listNextBatchOfObjects(objectListing);
            return Stream.concat(Stream.of(objectListing), getNextListing(nextListing));
        }
    }

    private void upload(File file, String newName, int retry) {
        log.info("Uploading file " + file);
        try {
            if (!exists(bucket, newName)) {
                uploadInternal(s3Provider, bucket, file, newName);
            }
        } catch (Exception e) {
            if (retry == 0) {
                log.error("Error during uploading, can't upload file due to exception: " + e.getMessage());
                throw new UploadingException("Can't upload file " + file + " due to error " + e.getMessage(), e);
            } else {
                log.warn("Error during uploading : " + e.getMessage() + " Trying next time...");
                upload(file, newName, retry - 1);
            }
        }
    }

    private void uploadInternal(AmazonS3 s3, String bucket, File file, String newName) throws Exception {
        uploadingStrategy.upload(s3, bucket, file, newName);
    }
}