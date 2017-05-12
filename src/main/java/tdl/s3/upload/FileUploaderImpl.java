package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import tdl.s3.sync.Destination;

@Slf4j
public class FileUploaderImpl implements FileUploader {

    public static int RETRY_TIMES_COUNT = 2;

    private final Destination destination;

    private final UploadingStrategy uploadingStrategy;

    public FileUploaderImpl(final Destination destination, UploadingStrategy uploadingStrategy) {
        this.destination = destination;
        this.uploadingStrategy = uploadingStrategy;
    }

    @Override
    public void upload(File file) {
        RemoteFile remoteFile = destination.createRemoteFile(file.getName());
        upload(file, remoteFile);
    }

    @Override
    public void upload(File file, RemoteFile remoteFile) {
        upload(file, remoteFile, RETRY_TIMES_COUNT);
    }

    @Override
    public boolean exists(RemoteFile remoteFile) {
        return destination.existsRemoteFile(remoteFile);
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
        uploadingStrategy.setDestination(destination);
        uploadingStrategy.upload(file, remoteFile);
    }

}
