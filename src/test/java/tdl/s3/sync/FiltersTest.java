package tdl.s3.sync;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

import static org.junit.Assert.*;

public class FiltersTest {

    @Test
    public void acceptShouldUseDefaultLockFilter() {
        Path validPath = Paths.get("src/test/resources/test_filter/file1.txt");
        Path invalidPath = Paths.get("src/test/resources/file1.txt.lock");
        Filters filters = Filters.getBuilder()
                .include((Path path) -> path.toString().endsWith(".txt"))
                .create();
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath));
    }
    
    public void exclude() {
        Path invalidPath1 = Paths.get("src/test/resources/test_filter/valid_file1.mp4");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/valid_file1.txt");
        Path invalidPath3 = Paths.get("src/test/resources/valid_file1.txt.lock");
        
        Filters filters1 = Filters.getBuilder()
                .include((Path path) -> path.getFileName().toString().startsWith("valid_"))
                .create();
        
        assertTrue(filters1.accept(invalidPath1));
        assertTrue(filters1.accept(invalidPath2));
        assertFalse(filters1.accept(invalidPath3));
        
        Filters filters2 = Filters.getBuilder()
                .include((Path path) -> path.getFileName().toString().startsWith("valid_"))
                .exclude((Path path) -> path.getFileName().toString().endsWith("txt"))
                .create();
        assertTrue(filters2.accept(invalidPath1));
        assertFalse(filters2.accept(invalidPath2));
        assertFalse(filters2.accept(invalidPath3));
        
        Filters filters3 = Filters.getBuilder()
                .include((Path path) -> path.getFileName().toString().startsWith("valid_"))
                .exclude((Path path) -> path.getFileName().toString().endsWith("txt"))
                .exclude((Path path) -> path.getFileName().toString().endsWith("mp4"))
                .create();
        assertFalse(filters3.accept(invalidPath1));
        assertFalse(filters3.accept(invalidPath2));
        assertFalse(filters3.accept(invalidPath3));
    }
    
    @Test
    public void name() {
        Path validPath = Paths.get("src/test/resources/test_filter/file1.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/file2.txt");
        Filters filters = Filters.getBuilder()
                .include(Filters.name("file1.txt"))
                .create();
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
    }
    
    @Test
    public void startsWith() {
        Path validPath = Paths.get("src/test/resources/test_filter/validstart_file1.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/invalidstart_file2.txt");
        Filters filters = Filters.getBuilder()
                .include(Filters.startsWith("validstart"))
                .create();
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
    }
    
    @Test
    public void endsWith() {
        Path invalidPath1 = Paths.get("src/test/resources/test_filter/validstart_file1.txt");
        Path invalidPath2 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath3 = Paths.get("src/test/resources/test_filter/invalidstart_file2.txt");
        Path invalidPath4 = Paths.get("src/test/resources/test_filter/invalidstart_file2.mp4");
        Filters filters = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .exclude(Filters.endsWith("mp4"))
                .create();
        assertTrue(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
        assertTrue(filters.accept(invalidPath3));
        assertFalse(filters.accept(invalidPath4));
    }
    
    @Test
    public void matches() {
        Path validPath = Paths.get("src/test/resources/test_filter/01file.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/0files.txt");
        Filters filters1 = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .create();
        
        assertTrue(filters1.accept(validPath));
        assertFalse(filters1.accept(invalidPath1));
        assertTrue(filters1.accept(invalidPath2));
        
        Filters filters2 = Filters.getBuilder()
                .include(Filters.matches("^[0-9]{2}[a-z]{4}.txt$"))
                .create();
        assertTrue(filters2.accept(validPath));
        assertFalse(filters2.accept(invalidPath1));
        assertFalse(filters2.accept(invalidPath2));
    }
}
