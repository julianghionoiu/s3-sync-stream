
package tdl.s3;

import com.amazonaws.services.s3.AmazonS3;
import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import tdl.s3.credentials.AWSSecretsProvider;
import tdl.s3.sync.Destination;
import tdl.s3.sync.FolderScannerImpl;
import tdl.s3.sync.FolderSynchronizer;
import tdl.s3.sync.Source;
import tdl.s3.upload.FileUploadingService;

public class RemoteSync {
    
    //TODO: Refactor this
    private static final List<String> FILTERED_EXTENSIONS = Collections.singletonList(".lock");

    private final Source source;
    
    private final Destination destination;
    
    private FileUploadingService fileUploadingService;
    
    private FolderSynchronizer folderSynchronizer;

    public RemoteSync(Source source, Destination destination) {
        this.source = source;
        this.destination = destination;
    }
    
    public void run() {
        buildUploadingService();
        if (source.isUpload()) {
            File file = source.getPath().toFile();
            fileUploadingService.upload(file);
        } else {
            throw new UnsupportedOperationException("Not implemented yet");
        }
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
        List<Predicate<Path>> filters = FILTERED_EXTENSIONS.stream()
                .map(ext -> (Predicate<Path>)(path -> ! path.toString().endsWith(ext)))
                .collect(Collectors.toList());
        FolderScannerImpl folderScanner = new FolderScannerImpl(filters);
        folderSynchronizer = new FolderSynchronizer(folderScanner, fileUploadingService);
    }
}
