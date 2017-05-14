package tdl.s3.sync;

import tdl.s3.sync.destination.Destination;
import tdl.s3.upload.FileUploadingService;

import java.io.IOException;

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
        this.listener = new DummyProgressListener();
    }

    public void setListener(ProgressListener listener) {
        this.listener = listener;
    }

    public void run() throws RemoteSyncException {
        buildUploadingService();
        buildFolderSynchronizer();
        folderSynchronizer.setListener(listener);
        try {
            folderSynchronizer.synchronize(source.getPath(), source.isRecursive());
        } catch (IOException e) {
            throw new RemoteSyncException("Failed to sync folder "+source.getPath(), e);
        }
    }

    private void buildUploadingService() {
        fileUploadingService = new FileUploadingService(destination);
    }

    private void buildFolderSynchronizer() {
        Filters filters = source.getFilters();
        FolderScanner folderScanner = new FolderScanner(filters);
        folderSynchronizer = new FolderSynchronizer(folderScanner, fileUploadingService);
    }
}
