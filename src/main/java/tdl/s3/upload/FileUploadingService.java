package tdl.s3.upload;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUpload;

import java.io.File;
import tdl.s3.helpers.ExistingMultipartUploadFinder;
import tdl.s3.sync.DummyProgressListener;
import tdl.s3.sync.ProgressListener;

public class FileUploadingService {

    private final AmazonS3 amazonS3;

    private final String bucket;

    private final String prefix;

    private ProgressListener listener = new DummyProgressListener();

    public FileUploadingService(AmazonS3 amazonS3, String bucket, String prefix) {
        this.amazonS3 = amazonS3;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    public void setListener(ProgressListener listener) {
        this.listener = listener;
    }

    public void upload(File file) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, file.getName(), amazonS3);
        upload(file, remoteFile);
    }

    public void upload(File file, String remoteName) {
        RemoteFile remoteFile = new RemoteFile(bucket, prefix, remoteName, amazonS3);
        upload(file, remoteFile);
    }

    private void upload(File file, RemoteFile remoteFile) {
        FileUploader fileUploader = createFileUploader(remoteFile);
        fileUploader.upload(file, remoteFile);
    }

    private FileUploader createFileUploader(RemoteFile remoteFile) {
        ExistingMultipartUploadFinder finder = new ExistingMultipartUploadFinder(amazonS3, remoteFile.getBucket(), remoteFile.getPrefix());
        MultipartUpload multipartUpload = finder.findOrNull(remoteFile);

        UploadingStrategy strategy = new MultipartUploadFileUploadingStrategy(multipartUpload);
        strategy.setListener(listener);
        return new FileUploaderImpl(amazonS3, bucket, prefix, strategy);
    }
}
