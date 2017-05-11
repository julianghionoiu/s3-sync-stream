package tdl.s3.sync;

import java.io.File;
import java.nio.file.Path;

public class Source {

    private Path path;

    private boolean isRecursive;

    private Filters filters;

    public static class Builder {

        private Source source = new Source();

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
}
