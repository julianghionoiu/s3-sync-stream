package tdl.s3.sync;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.function.Consumer;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;

/**
 * @author vdanyliuk
 * @version 13.04.17.
 */
@Slf4j
public class FolderScannerImpl implements FolderScanner {

    @Override
    public void traverseFolder(Path folderPath, Consumer<File> fileConsumer, boolean recursive) {
        if (! Files.isDirectory(folderPath)) throw new IllegalArgumentException("Path should represent directory.");
        int scanDepth = recursive ? Integer.MAX_VALUE : 1;
        try {
            Files.walkFileTree(folderPath,
                    Collections.singleton(FileVisitOption.FOLLOW_LINKS),
                    scanDepth,
                    getVisitor(fileConsumer, recursive, folderPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private FileVisitor<? super Path> getVisitor(Consumer<File> fileConsumer, boolean recursive, Path currentDir) {
        return new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return recursive || currentDir.equals(dir) ? CONTINUE : SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file)) {
                    fileConsumer.accept(file.toFile());
                }
                return CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                log.warn("Can't read file " + file + " due to exception: " + exc.getMessage());
                return CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                return CONTINUE;
            }
        };
    }


}
