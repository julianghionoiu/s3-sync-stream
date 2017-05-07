package tdl.s3.sync;

import java.nio.file.Path;

public interface Filter {
    
    public boolean accept(Path path);
}
