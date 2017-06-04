package tdl.s3.sync;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import static org.mockito.Mockito.mock;

public class SourceTest {

    private Path emptyDirPath;
    private Path notEmptyDirPath;
    private Filters filters;

    @Before
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

    @Test
    public void getUploadFilesRelativePathListShouldReturnEmptyList() {
        Source source = Source.getBuilder(emptyDirPath)
                .setRecursive(true)
                .setFilters(filters)
                .create();
        List<String> pathList = source.getFilesToUpload();
        assertTrue(pathList.isEmpty());
    }

    @Test
    public void getUploadFilesRelativePathListShouldReturnNotEmptyList() {
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
        assertEquals(pathList, expected);
    }

    @Test
    public void getUploadFilesRelativePathListShouldReturnNonRecursive() {
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
        assertEquals(pathList, expected);
    }
}
