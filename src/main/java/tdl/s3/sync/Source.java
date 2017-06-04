package tdl.s3.sync;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class Source {

    private Path path;

    private boolean isRecursive;

    private Filters filters;

    public static class Builder {

        private final Source source = new Source();

        public Builder(Path path) {
            source.path = path;
        }

        public Builder traverseDirectories(boolean traverse) {
            return this;
        }

        public Builder setRecursive(boolean isRecursive) {
            source.isRecursive = isRecursive;
            return this;
        }

        public Builder setFilters(Filters filters) {
            source.filters = filters;
            return this;
        }

        public Source create() {
            if (source.filters == null) {
                throw new RuntimeException("Cannot found filters.");
            }
            return source;
        }
    }

    public static Builder getBuilder(Path path) {
        return new Builder(path);
    }

    public Path getPath() {
        return path;
    }

    public Filters getFilters() {
        return filters;
    }

    public boolean isRecursive() {
        return isRecursive;
    }

    public boolean isValidPath() {
        File file = path.toFile();
        return file.isDirectory();
    }

    public List<String> getUploadFilesRelativePathList() {
        try {
            int maxDepth = isRecursive ? Integer.MAX_VALUE : 1;
            File base = path.toFile();
            BiPredicate<Path, BasicFileAttributes> matcher = (filePath, fileAttr) -> {
                return fileAttr.isRegularFile() && filters.accept(filePath);
            };
            return Files.find(path, maxDepth, matcher)
                    .map(filePath -> {
                        return base.toURI().relativize(filePath.toFile().toURI()).getPath();
                    })
                    .collect(Collectors.toList());
        } catch (IOException ex) {
            return new ArrayList<>();
        }
    }
}
