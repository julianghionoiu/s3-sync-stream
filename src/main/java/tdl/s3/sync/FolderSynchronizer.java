package tdl.s3.sync;

import java.io.File;
import tdl.s3.sync.progress.ProgressListener;
import tdl.s3.upload.FileUploadingService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.DestinationOperationException;

@Slf4j
class FolderSynchronizer {

    private final Source source;

    private final FileUploadingService fileUploadingService;

    FolderSynchronizer(Source source, FileUploadingService fileUploadingService) {
        this.source = source;
        this.fileUploadingService = fileUploadingService;
    }

    void synchronize() {
        Path folder = source.getPath();
        List<String> paths = source.getUploadFilesRelativePathList();
        Destination destination = fileUploadingService.getDestination();
        List<String> uploadable;
        try {
            uploadable = destination.filterUploadableFiles(paths);
        } catch (DestinationOperationException ex) {
            uploadable = new ArrayList<>();
        }
        if (uploadable.isEmpty()) {
            return;
        }
        uploadable.stream()
                .forEach(upload -> {
                    File uploadFile = new File(folder.toFile(), upload);
                    fileUploadingService.upload(uploadFile, upload);
                });
    }

    void setListener(ProgressListener listener) {
        fileUploadingService.setListener(listener);
    }
}
