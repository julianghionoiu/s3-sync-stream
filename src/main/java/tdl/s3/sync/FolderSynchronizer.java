package tdl.s3.sync;

import tdl.s3.upload.FileUploadingService;

import java.nio.file.Path;

/**
 * @author vdanyliuk
 * @version 13.04.17.
 */
public class FolderSynchronizer {

    private FolderScanner folderScanner;

    private FileUploadingService fileUploadingService;

    public FolderSynchronizer(FolderScanner folderScanner, FileUploadingService fileUploadingService) {
        this.folderScanner = folderScanner;
        this.fileUploadingService = fileUploadingService;
    }

    public void synchronize(Path folder, boolean recursive) {
        folderScanner.traverseFolder(folder, fileUploadingService::upload, recursive);
    }
}
