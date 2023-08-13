package tdl.s3.sync;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SourceTest {

    private Path emptyDirPath;
    private Path notEmptyDirPath;
    private Filters filters;

    @BeforeEach
    public void setUp() throws Exception {

        filters = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .create();

        //create empty directory if not exists
        emptyDirPath = Paths.get("build", "empty_dir");
        if (!Files.exists(emptyDirPath)) {
            Files.createDirectory(emptyDirPath);
        }

        notEmptyDirPath = Paths.get("src", "test", "resources", "test_dir");
    }

    @Test()
    public void build() {
        Path path = Paths.get("src");
        Filters filters = mock(Filters.class);
        Source source = Source.getBuilder(path)
                .traverseDirectories(true)
                .setRecursive(true)
                .setFilters(filters)
                .create();
        Assertions.assertEquals(source.getPath(), path);
        Assertions.assertTrue(source.isRecursive());
        Assertions.assertNotNull(source.getFilters());
        Assertions.assertTrue(source.isValidPath());
    }

    @Test
    public void createShouldThrowExceptionIfFilterNotSet() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            Path path = Paths.get("src");
            Source source = Source.getBuilder(path)
                    .traverseDirectories(true)
                    .create();
        });
    }

    @Test
    public void getFilesToUploadShouldReturnEmptyList() {
        Source source = Source.getBuilder(emptyDirPath)
                .setRecursive(true)
                .setFilters(filters)
                .create();
        List<String> pathList = source.getFilesToUpload();
        Assertions.assertTrue(pathList.isEmpty());
    }

    @Test
    public void getFilesToUploadShouldReturnNotEmptyList() {
        Source source = Source.getBuilder(notEmptyDirPath)
                .setRecursive(true)
                .setFilters(filters)
                .create();
        List<String> pathList = source.getFilesToUpload();
        List<String> expected = Arrays.asList(
                "subdir/sub_test_file_1.txt",
                "test_file_1.txt",
                "test_file_2.txt"
        );
        Collections.sort(pathList);
        Collections.sort(expected);
        Assertions.assertEquals(pathList, expected);
    }

    @Test
    public void getFilesToUploadShouldReturnNonRecursive() {
        Source source = Source.getBuilder(notEmptyDirPath)
                .setRecursive(false)
                .setFilters(filters)
                .create();
        List<String> pathList = source.getFilesToUpload();
        List<String> expected = Arrays.asList(
                "test_file_1.txt",
                "test_file_2.txt"
        );
        Collections.sort(pathList);
        Collections.sort(expected);
        Assertions.assertEquals(pathList, expected);
    }

    @Test
    public void getFilesToUploadShouldReturnEmptyListOnIOException() {
        Path nonExistentPath = Paths.get("src/directory_that_doesnot_exist");
        Source source = Source.getBuilder(nonExistentPath)
                .setRecursive(false)
                .setFilters(filters)
                .create();
        List<String> pathList = source.getFilesToUpload();
        Assertions.assertTrue(pathList.isEmpty());
    }
}
