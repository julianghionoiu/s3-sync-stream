package tdl.s3.sync;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.SKIP_SUBTREE;

@Slf4j
public class FolderScannerImpl implements FolderScanner {

    private final Predicate<Path> filter;

    public FolderScannerImpl(List<Predicate<Path>> filters) {
        this.filter = filters.stream().reduce(v -> true, Predicate::and);
    }

    @Override
    public void traverseFolder(Path folderPath, BiConsumer<File, String> fileConsumer, boolean recursive) {
        if (! Files.isDirectory(folderPath)) throw new IllegalArgumentException("Path should represent directory.");
        int scanDepth = recursive ? Integer.MAX_VALUE : 1;
        try {
            Files.walkFileTree(folderPath,
                    Collections.singleton(FileVisitOption.FOLLOW_LINKS),
                    scanDepth,
                    getVisitor(fileConsumer, recursive, folderPath));
        } catch (IOException e) {
            throw new RuntimeException("Can't synchronize folder due to exception: " + e.getMessage(), e);
        }
    }

    private FileVisitor<? super Path> getVisitor(BiConsumer<File, String> fileConsumer, boolean recursive, Path currentDir) {
        return new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return recursive || currentDir.equals(dir) ? CONTINUE : SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file) && filter.test(file)) {
                    fileConsumer.accept(file.toFile(), currentDir.relativize(file).toString());
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
