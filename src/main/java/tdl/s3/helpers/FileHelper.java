package tdl.s3.helpers;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class FileHelper {

    private FileHelper() {

    }

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

    public static String getRelativeFilePathToCwd(File file) {
        URI baseUri = Paths.get(".").toUri();
        URI fileUri = file.toURI();
        URI relativeUri = baseUri.relativize(fileUri);
        return relativeUri.getPath();
    }
}
