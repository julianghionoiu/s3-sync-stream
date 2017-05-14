package tdl.s3.upload;

import java.io.File;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.DummyProgressListener;
import tdl.s3.sync.ProgressListener;

public class FileUploadingService {

    private final Destination destination;

    private ProgressListener listener = new DummyProgressListener();

    public FileUploadingService(Destination destination) {
        this.destination = destination;
    }

    public void setListener(ProgressListener listener) {
        this.listener = listener;
    }

    public void upload(File file) {
        upload(file, file.getName());
    }

    public void upload(File file, String remoteName) {
        FileUploader fileUploader = createFileUploader();
        fileUploader.upload(file, remoteName);
    }

    private FileUploader createFileUploader() {
        UploadingStrategy strategy = new MultipartUploadFileUploadingStrategy(destination);
        strategy.setListener(listener);
        return new FileUploaderImpl(destination, strategy);
    }
}
