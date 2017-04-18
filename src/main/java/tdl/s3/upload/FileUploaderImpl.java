package tdl.s3.upload;

import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class FileUploaderImpl implements FileUploader {

    public static int RETRY_TIMES_COUNT = 2;

    private final AmazonS3 s3Provider;
    private final String bucket;
    private UploadingStrategy uploadingStrategy;

    public FileUploaderImpl(final AmazonS3 s3Provider, String bucket) {
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
        try {
            s3Provider.getObjectMetadata(bucketName, fileKey);
            return true;
        }catch (NotFoundException nfe) {
            return false;
        } catch (AmazonS3Exception nfe) {
            if (nfe.getStatusCode() == 404) {
                return false;
            } else {
                throw nfe;
            }
        }
    }

    @Override
    public void setStrategy(UploadingStrategy uploadingStrategy) {
        this.uploadingStrategy = uploadingStrategy;
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
