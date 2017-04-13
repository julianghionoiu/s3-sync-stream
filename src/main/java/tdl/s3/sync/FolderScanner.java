package tdl.s3.sync;

import java.io.File;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * @author vdanyliuk
 * @version 13.04.17.
 */
public interface FolderScanner {

    void traverseFolder(Path folderPath, Consumer<File> fileConsumer, boolean recursive);
}
