package tdl.s3.sync;

import tdl.s3.upload.FileUploadingService;

import java.io.IOException;
import java.nio.file.Path;

class FolderSynchronizer {

    private final FolderScanner folderScanner;

    private final FileUploadingService fileUploadingService;

    FolderSynchronizer(FolderScanner folderScanner, FileUploadingService fileUploadingService) {
        this.folderScanner = folderScanner;
        this.fileUploadingService = fileUploadingService;
    }

    void synchronize(Path folder, boolean recursive) throws IOException {
        folderScanner.traverseFolder(folder, fileUploadingService::upload, recursive);
    }

    void setListener(ProgressListener listener) {
        fileUploadingService.setListener(listener);
    }
}
