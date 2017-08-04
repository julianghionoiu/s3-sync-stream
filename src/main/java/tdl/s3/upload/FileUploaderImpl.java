package tdl.s3.upload;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import tdl.s3.helpers.FileHelper;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

@Slf4j
public class FileUploaderImpl implements FileUploader {

    private static final int RETRY_TIMES_COUNT = 2;

    private final Destination destination;

    private final UploadingStrategy uploadingStrategy;

    FileUploaderImpl(final Destination destination, UploadingStrategy uploadingStrategy) {
        this.destination = destination;
        this.uploadingStrategy = uploadingStrategy;
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
        String filePath = FileHelper.getRelativeFilePathToCwd(file);
        log.info("Uploading file " + filePath);
        try {
            uploadInternal(file, path);
        } catch (IOException | DestinationOperationException e) {
            //TODO: Might need to change to loop instead of recursive construct
            if (retry == 0) {
                log.error("Error during uploading, can't upload file due to exception: " + e.getMessage());
                throw new UploadingException("Can't upload file " + filePath + " due to error " + e.getMessage(), e);
            } else {
                log.warn("Error during uploading : " + e.getMessage() + " Trying next time...");
                upload(file, path, retry - 1);
            }
        } finally {
            log.info("Finished uploading file " + filePath);
        }
    }

    private void uploadInternal(File file, String path) throws DestinationOperationException, IOException {
        uploadingStrategy.setDestination(destination);
        uploadingStrategy.upload(file, path);
    }

}
