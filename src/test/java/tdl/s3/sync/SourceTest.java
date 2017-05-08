package tdl.s3.sync;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

public class SourceTest {
    
    @Test(expected = RuntimeException.class)
    public void createShouldThrowExceptionIfFilterNotSet() {
        Path path = Paths.get("src");
        Source source = Source.getBuilder(path)
                            .create();
    }
}
