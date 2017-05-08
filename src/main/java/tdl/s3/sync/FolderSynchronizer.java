package tdl.s3.sync;

import tdl.s3.upload.FileUploadingService;

import java.nio.file.Path;
import java.util.List;

public class FolderSynchronizer {

    private final FolderScanner folderScanner;

    private final FileUploadingService fileUploadingService;

    private List<SyncProgressListener> listeners;

    public FolderSynchronizer(FolderScanner folderScanner, FileUploadingService fileUploadingService) {
        this.folderScanner = folderScanner;
        this.fileUploadingService = fileUploadingService;
    }

    public void synchronize(Path folder, boolean recursive) {
        folderScanner.traverseFolder(folder, fileUploadingService::upload, recursive);
    }

    public void setListeners(List<SyncProgressListener> listeners) {
        this.listeners = listeners;
    }
}
