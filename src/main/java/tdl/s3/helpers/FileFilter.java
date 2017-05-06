
package tdl.s3.helpers;

import java.nio.file.Path;

public class FileFilter {

    public static class Builder {
        
        private final FileFilter filter = new FileFilter();
        
        public FileFilter create() {
            return filter;
        }
    }
    
    public Builder getBuilder() {
        return new Builder();
    }
    
    public boolean match(Path path) {
        return true;
    }
}
