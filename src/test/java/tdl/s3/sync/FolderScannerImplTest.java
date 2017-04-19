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
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class FolderScannerImplTest {

    private Path emptyDirPath;
    private Path notEmptyDirPath;

    @Mock
    private BiConsumer<File, String> fileConsumer;

    private FolderScanner folderScanner;

    @Before
    public void setUp() throws Exception {

        folderScanner = new FolderScannerImpl(Collections.singletonList(path -> !path.toString().endsWith(".lock")));

        //create empty directory if not exists
        emptyDirPath = Paths.get("empty_dir");
        if (! Files.exists(emptyDirPath)) {
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
    public void traverseFolder_notEmptyFolder_recursive_withoutFilter() throws Exception {
        folderScanner = new FolderScannerImpl(Collections.emptyList());

        folderScanner.traverseFolder(notEmptyDirPath, fileConsumer, true);

        verify(fileConsumer, times(4)).accept(any(), anyString());
    }

}