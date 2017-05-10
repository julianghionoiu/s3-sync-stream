package tdl.s3;

import com.amazonaws.services.s3.AmazonS3;
import tdl.s3.credentials.AWSSecretsProvider;
import tdl.s3.sync.Destination;
import tdl.s3.sync.Filters;
import tdl.s3.sync.FolderScannerImpl;
import tdl.s3.sync.FolderSynchronizer;
import tdl.s3.sync.Source;
import tdl.s3.upload.FileUploadingService;
import tdl.s3.sync.ProgressListener;

public class RemoteSync {

    private final Source source;

    private final Destination destination;

    private FileUploadingService fileUploadingService;

    private FolderSynchronizer folderSynchronizer;

    private ProgressListener listener;

    public RemoteSync(Source source, Destination destination) {
        this.source = source;
        if (!this.source.isValidPath()) {
            throw new RuntimeException("Source has to be a directory");
        }
        this.destination = destination;
    }

    public void setListener(ProgressListener listener) {
        this.listener = listener;
    }

    public void run() {
        buildUploadingService();
        buildFolderSynchronizer();
        folderSynchronizer.setListener(listener);
        folderSynchronizer.synchronize(source.getPath(), source.isRecursive());
    }

    private void buildUploadingService() {
        AWSSecretsProvider secret = destination.getSecret();
        AmazonS3 client = destination.buildClient();
        fileUploadingService = new FileUploadingService(
                client,
                secret.getS3Bucket(),
                secret.getS3Prefix()
        );
    }

    private void buildFolderSynchronizer() {
        Filters filters = source.getFilters();
        FolderScannerImpl folderScanner = new FolderScannerImpl(filters);
        folderSynchronizer = new FolderSynchronizer(folderScanner, fileUploadingService);
    }
}
