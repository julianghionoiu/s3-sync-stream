package tdl.s3.sync;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FiltersTest {
    
    @Test
    public void builderShouldThrowException() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            Filters.getBuilder().create();
        });
    }

    @Test
    public void acceptShouldUseDefaultLockFilter() {
        Path validPath = Paths.get("src/test/resources/test_filter/file1.txt");
        Path invalidPath = Paths.get("src/test/resources/file1.txt.lock");
        Filters filters = Filters.getBuilder()
                .include((Path path) -> path.toString().endsWith(".txt"))
                .create();
        Assertions.assertTrue(filters.accept(validPath));
        Assertions.assertFalse(filters.accept(invalidPath));
    }

    public void exclude() {
        Path invalidPath1 = Paths.get("src/test/resources/test_filter/valid_file1.mp4");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/valid_file1.txt");
        Path invalidPath3 = Paths.get("src/test/resources/valid_file1.txt.lock");
        
        Filters filters1 = Filters.getBuilder()
                .include((Path path) -> path.getFileName().toString().startsWith("valid_"))
                .create();
        
        Assertions.assertTrue(filters1.accept(invalidPath1));
        Assertions.assertTrue(filters1.accept(invalidPath2));
        Assertions.assertFalse(filters1.accept(invalidPath3));
        
        Filters filters2 = Filters.getBuilder()
                .include((Path path) -> path.getFileName().toString().startsWith("valid_"))
                .exclude((Path path) -> path.getFileName().toString().endsWith("txt"))
                .create();
        Assertions.assertTrue(filters2.accept(invalidPath1));
        Assertions.assertFalse(filters2.accept(invalidPath2));
        Assertions.assertFalse(filters2.accept(invalidPath3));
        
        Filters filters3 = Filters.getBuilder()
                .include((Path path) -> path.getFileName().toString().startsWith("valid_"))
                .exclude((Path path) -> path.getFileName().toString().endsWith("txt"))
                .exclude((Path path) -> path.getFileName().toString().endsWith("mp4"))
                .create();
        Assertions.assertFalse(filters3.accept(invalidPath1));
        Assertions.assertFalse(filters3.accept(invalidPath2));
        Assertions.assertFalse(filters3.accept(invalidPath3));
    }
    
    @Test
    public void name() {
        Path validPath = Paths.get("src/test/resources/test_filter/file1.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/file2.txt");
        Filters filters = Filters.getBuilder()
                .include(Filters.name("file1.txt"))
                .create();
        Assertions.assertTrue(filters.accept(validPath));
        Assertions.assertFalse(filters.accept(invalidPath1));
        Assertions.assertFalse(filters.accept(invalidPath2));
    }
    
    @Test
    public void startsWith() {
        Path validPath = Paths.get("src/test/resources/test_filter/validstart_file1.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/test_filter/invalidstart_file2.txt");
        Filters filters = Filters.getBuilder()
                .include(Filters.startsWith("validstart"))
                .create();
        Assertions.assertTrue(filters.accept(validPath));
        Assertions.assertFalse(filters.accept(invalidPath1));
        Assertions.assertFalse(filters.accept(invalidPath2));
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
        Assertions.assertTrue(filters.accept(invalidPath1));
        Assertions.assertFalse(filters.accept(invalidPath2));
        Assertions.assertTrue(filters.accept(invalidPath3));
        Assertions.assertFalse(filters.accept(invalidPath4));
    }
    
    @Test
    public void matches() {
        Path validPath = Paths.get("src/test/resources/test_filter/01file.txt");
        Path invalidPath1 = Paths.get("src/test/resources/file1.txt.lock");
        Path invalidPath2 = Paths.get("src/test/resources/0files.txt");
        Filters filters1 = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .create();
        
        Assertions.assertTrue(filters1.accept(validPath));
        Assertions.assertFalse(filters1.accept(invalidPath1));
        Assertions.assertTrue(filters1.accept(invalidPath2));
        
        Filters filters2 = Filters.getBuilder()
                .include(Filters.matches("^[0-9]{2}[a-z]{4}.txt$"))
                .create();
        Assertions.assertTrue(filters2.accept(validPath));
        Assertions.assertFalse(filters2.accept(invalidPath1));
        Assertions.assertFalse(filters2.accept(invalidPath2));
    }
}
