package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUpload;

import java.io.File;
import tdl.s3.helpers.ExistingMultipartUploadFinder;
import tdl.s3.sync.Destination;
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
        RemoteFile remoteFile = this.destination.createRemoteFile(file.getName());
        upload(file, remoteFile);
    }

    public void upload(File file, String remoteName) {
        RemoteFile remoteFile = this.destination.createRemoteFile(remoteName);
        upload(file, remoteFile);
    }

    private void upload(File file, RemoteFile remoteFile) {
        FileUploader fileUploader = createFileUploader();
        fileUploader.upload(file, remoteFile);
    }

    private FileUploader createFileUploader() {
        UploadingStrategy strategy = new MultipartUploadFileUploadingStrategy(destination);
        strategy.setListener(listener);
        return new FileUploaderImpl(destination, strategy);
    }
}
