package tdl.s3.sync;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import tdl.s3.helpers.FileFilter;

public class SyncSource {

    private Path path;
    
    private FileFilter include;
    
    private FileFilter exclude;
    
    private boolean traverse;

    public static class Builder {
        
        private SyncSource source = new SyncSource();
        
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
        
        public SyncSource create() {
            return null;
        }
    }
    
    public static Builder getBuilder(Path path) {
        return null;
    }
}
