package tdl.s3.sync;

import tdl.s3.upload.FileUploadingService;

import java.nio.file.Path;
import java.util.List;

public class FolderSynchronizer {

    private final FolderScanner folderScanner;

    private final FileUploadingService fileUploadingService;

    public FolderSynchronizer(FolderScanner folderScanner, FileUploadingService fileUploadingService) {
        this.folderScanner = folderScanner;
        this.fileUploadingService = fileUploadingService;
    }

    public void synchronize(Path folder, boolean recursive) {
        folderScanner.traverseFolder(folder, fileUploadingService::upload, recursive);
    }

    public void setListener(SyncProgressListener listener) {
        fileUploadingService.setListener(listener);
    }
}
