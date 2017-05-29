package tdl.s3.sync;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class FolderScannerTest {

    private Path emptyDirPath;
    private Path notEmptyDirPath;

    @Mock
    private BiConsumer<File, String> fileConsumer;

    private FolderScanner folderScanner;

    @Before
    public void setUp() throws Exception {

        Filters filters = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .create();
        folderScanner = new FolderScanner(filters);

        //create empty directory if not exists
        emptyDirPath = Paths.get("build", "empty_dir");
        if (!Files.exists(emptyDirPath)) {
            Files.createDirectory(emptyDirPath);
        }

        notEmptyDirPath = Paths.get("src", "test", "resources", "test_dir");
    }

    @Test
    public void traverseFolder_emptyFolder() throws Exception {
        folderScanner.traverseFolder(emptyDirPath, fileConsumer, false);

        verify(fileConsumer, never()).accept(any(), anyString());
    }

    @Test
    public void traverseFolder_notEmptyFolder_nonRecursive() throws Exception {
        folderScanner.traverseFolder(notEmptyDirPath, fileConsumer, false);

        verify(fileConsumer, times(2)).accept(any(), anyString());
    }

    @Test
    public void traverseFolder_notEmptyFolder_recursive() throws Exception {
        folderScanner.traverseFolder(notEmptyDirPath, fileConsumer, true);

        verify(fileConsumer, times(3)).accept(any(), anyString());
    }

    @Test
    public void traverseFolder_notEmptyFolder_recursive_correctNameWithSubFolder() throws Exception {
        folderScanner.traverseFolder(notEmptyDirPath, fileConsumer, true);

        verify(fileConsumer, times(3)).accept(any(), anyString());

        verify(fileConsumer, times(1)).accept(any(), startsWith("subdir"));
    }

    @Test
    public void getUploadFilesRelativePathListShouldReturnEmptyList() {
        List<String> pathList = folderScanner.getUploadFilesRelativePathList(emptyDirPath);
        assertTrue(pathList.isEmpty());
    }

    @Test
    public void getUploadFilesRelativePathListShouldReturnNotEmptyList() {
        List<String> pathList = folderScanner.getUploadFilesRelativePathList(notEmptyDirPath);
        List<String> expected = Arrays.asList(
                "subdir/sub_test_file_1.txt",
                "test_file_1.txt",
                "test_file_2.txt"
        );
        Collections.sort(pathList);
        Collections.sort(expected);
        assertEquals(pathList, expected);
    }
}
