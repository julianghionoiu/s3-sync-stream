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
        Filters filters = new Filters();
        filters.include((Path path) -> path.toString().endsWith(".txt"));
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath));
    }
    
    public void exclude() {
        Path invalidPath1 = Paths.get("src/test/resources/test_filter/valid_file1.mp4");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/valid_file1.txt");
        Path invalidPath3 = Paths.get("src/test/resources/valid_file1.txt.lock");
        Filters filters = new Filters();
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
        assertFalse(filters.accept(invalidPath3));
        
        filters.include((Path path) -> path.getFileName().toString().startsWith("valid_"));
        assertTrue(filters.accept(invalidPath1));
        assertTrue(filters.accept(invalidPath2));
        assertFalse(filters.accept(invalidPath3));
        
        filters.exclude((Path path) -> path.getFileName().toString().endsWith("txt"));
        assertTrue(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
        assertFalse(filters.accept(invalidPath3));
        
        filters.exclude((Path path) -> path.getFileName().toString().endsWith("mp4"));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
        assertFalse(filters.accept(invalidPath3));
    }
    
    @Test
    public void name() {
        Path validPath = Paths.get("src/test/resources/test_filter/file1.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/file2.txt");
        Filters filters = new Filters();
        filters.include(Filters.name("file1.txt"));
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
    }
    
    @Test
    public void startsWith() {
        Path validPath = Paths.get("src/test/resources/test_filter/validstart_file1.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/invalidstart_file2.txt");
        Filters filters = new Filters();
        filters.include(Filters.startsWith("validstart"));
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
    }
    
    @Test
    public void endsWith() {
        Path invalidPath1 = Paths.get("src/test/resources/test_filter/validstart_file1.txt");
        Path invalidPath2 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath3 = Paths.get("src/test/resources/test_filter/invalidstart_file2.txt");
        Filters filters = new Filters();
        
        filters.exclude(Filters.endsWith("txt"));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
        assertFalse(filters.accept(invalidPath3));
    }
    
    @Test
    public void matches() {
        Path validPath = Paths.get("src/test/resources/test_filter/01file.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/0files.txt");
        Filters filters = new Filters();
        
        assertFalse(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
        
        filters.include(Filters.matches("^[0-9]{2}[a-z]{4}.txt$"));
        assertTrue(filters.accept(validPath));
        assertFalse(filters.accept(invalidPath1));
        assertFalse(filters.accept(invalidPath2));
    }
}
