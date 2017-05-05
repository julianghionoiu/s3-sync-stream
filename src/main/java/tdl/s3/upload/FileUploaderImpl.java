package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

@Slf4j
public class FileUploaderImpl implements FileUploader {

    public static int RETRY_TIMES_COUNT = 2;

    private final AmazonS3 s3Provider;
    private final String bucket;
    private final String prefix;
    private final UploadingStrategy uploadingStrategy;

    public FileUploaderImpl(final AmazonS3 s3Provider, String bucket, String prefix, UploadingStrategy uploadingStrategy) {
        this.s3Provider = s3Provider;
        this.bucket = bucket;
        this.prefix = prefix;
        this.uploadingStrategy = uploadingStrategy;
    }

    @Override
    public void upload(File file) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, file.getName(), s3Provider);
        upload(file, remoteFile);
    }

    @Override
    public void upload(File file, RemoteFile remoteFile) {
        upload(file, remoteFile, RETRY_TIMES_COUNT);
    }

    @Override
    public boolean exists(RemoteFile remoteFile) {
        return remoteFile.exists();
    }

    private void upload(File file, RemoteFile remoteFile, int retry) {
        log.info("Uploading file " + file);
        try {
            if (!exists(remoteFile)) {
                uploadInternal(file, remoteFile);
            }
        } catch (Exception e) {
            if (retry == 0) {
                log.error("Error during uploading, can't upload file due to exception: " + e.getMessage());
                throw new UploadingException("Can't upload file " + file + " due to error " + e.getMessage(), e);
            } else {
                log.warn("Error during uploading : " + e.getMessage() + " Trying next time...");
                upload(file, remoteFile, retry - 1);
            }
        }
    }
    
    private void uploadInternal(File file, RemoteFile remoteFile) throws Exception {
        uploadingStrategy.upload(s3Provider, file, remoteFile);
    }

}
