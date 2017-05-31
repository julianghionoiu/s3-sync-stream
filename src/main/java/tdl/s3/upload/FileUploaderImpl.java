package tdl.s3.upload;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

@Slf4j
public class FileUploaderImpl implements FileUploader {

    private static int RETRY_TIMES_COUNT = 2;

    private final Destination destination;

    private final FileUploadingStrategy uploadingStrategy;

    FileUploaderImpl(final Destination destination) {
        this.destination = destination;
        this.uploadingStrategy = destination.createUploadingStrategy();
    }

    @Override
    public void upload(File file) throws UploadingException {
        upload(file, file.getName());
    }

    @Override
    public void upload(File file, String path) throws UploadingException {
        upload(file, path, RETRY_TIMES_COUNT);
    }

    private void upload(File file, String path, int retry) throws UploadingException {
        log.info("Uploading file " + file);
        try {
            uploadInternal(file, path);
        } catch (IOException | DestinationOperationException e) {
            //TODO: Might need to change to loop instead of recursive construct
            if (retry == 0) {
                log.error("Error during uploading, can't upload file due to exception: " + e.getMessage());
                throw new UploadingException("Can't upload file " + file + " due to error " + e.getMessage(), e);
            } else {
                log.warn("Error during uploading : " + e.getMessage() + " Trying next time...");
                upload(file, path, retry - 1);
            }
        } finally {
            log.info("Finished uploading file " + file);
        }
    }

    public FileUploadingStrategy getUploadingStrategy() {
        return uploadingStrategy;
    }

    public Destination getDestination() {
        return destination;
    }

    private void uploadInternal(File file, String path) throws DestinationOperationException, IOException {
        uploadingStrategy.upload(file, path);
    }

}
