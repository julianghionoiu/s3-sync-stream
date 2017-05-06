package tdl.s3.sync;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import tdl.s3.helpers.FileFilter;

public class Source {

    private Path path;
    
    private FileFilter include;
    
    private FileFilter exclude;
    
    private boolean traverse;

    public static class Builder {
        
        private Source source = new Source();

        public Builder(Path path) {
            source.path = path;
        }
        
        public Builder traverseDirectories(boolean traverse) {
            return this;
        }
        
        public Builder setInclude(FileFilter filter) {
            source.include = filter;
            return this;
        }
        
        public Builder setExclude(FileFilter filter) {
            source.exclude = filter;
            return this;
        }
        
        public Source create() {
            return source;
        }
    }
    
    public static Builder getBuilder(Path path) {
        return new Builder(path);
    }

    public Path getPath() {
        return path;
    }
    
    //Assume sync if path is directory
    public boolean isSync() {
        return false; //TODO
    }
    
    //Assume sync if path is file
    public boolean isUpload() {
        File file = path.toFile();
        return file.isFile();
    }
}
