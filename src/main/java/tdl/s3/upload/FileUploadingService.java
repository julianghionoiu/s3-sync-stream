package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUpload;

import java.io.File;
import tdl.s3.helpers.ExistingMultipartUploadFinder;
import tdl.s3.sync.DummyProgressListener;
import tdl.s3.sync.ProgressListener;

public class FileUploadingService {

    private final AmazonS3 client;

    private final String bucket;

    private final String prefix;

    private ProgressListener listener = new DummyProgressListener();

    public FileUploadingService(AmazonS3 client, String bucket, String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public void setListener(ProgressListener listener) {
        this.listener = listener;
    }

    public void upload(File file) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, file.getName(), client);
        upload(file, remoteFile);
    }

    public void upload(File file, String remoteName) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, remoteName, client);
        upload(file, remoteFile);
    }

    private void upload(File file, RemoteFile remoteFile) {
        FileUploader fileUploader = createFileUploader();
        fileUploader.upload(file, remoteFile);
    }

    private FileUploader createFileUploader() {
        UploadingStrategy strategy = new MultipartUploadFileUploadingStrategy(client);
        strategy.setListener(listener);
        return new FileUploaderImpl(client, bucket, prefix, strategy);
    }
}
