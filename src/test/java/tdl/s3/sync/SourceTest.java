package tdl.s3.sync;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SourceTest {

    @Test()
    public void build() {
        Path path = Paths.get("src");
        Filters filters = mock(Filters.class);
        Source source = Source.getBuilder(path)
                .traverseDirectories(true)
                .setRecursive(true)
                .setFilters(filters)
                .create();
        assertEquals(source.getPath(), path);
        assertEquals(source.isRecursive(), true);
        assertNotNull(source.getFilters());
        assertTrue(source.isValidPath());
    }

    @Test(expected = RuntimeException.class)
    public void createShouldThrowExceptionIfFilterNotSet() {
        Path path = Paths.get("src");
        Source source = Source.getBuilder(path)
                .traverseDirectories(true)
                .create();
    }
}
