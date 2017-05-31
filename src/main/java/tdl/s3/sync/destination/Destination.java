package tdl.s3.sync.destination;

import java.util.List;
import tdl.s3.upload.FileUploadingStrategy;

public interface Destination {

    List<String> filterUploadableFiles(List<String> paths) throws DestinationOperationException;

    FileUploadingStrategy createUploadingStrategy();
}
