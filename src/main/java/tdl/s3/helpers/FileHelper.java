
package tdl.s3.helpers;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHelper {

    public static boolean lockFileExists(File file) {
        Path lockFilePath = getLockFilePath(file);
        return Files.exists(lockFilePath);
    }
    
    public static Path getLockFilePath(File file) {
        String lockFileName = file.getName() + ".lock";
        Path fileDirectory = file.toPath()
                .toAbsolutePath()
                .normalize()
                .getParent();
        return fileDirectory.resolve(lockFileName);
    }
}
