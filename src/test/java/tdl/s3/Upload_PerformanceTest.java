package tdl.s3;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdl.s3.sync.Filters;
import tdl.s3.sync.RemoteSync;
import tdl.s3.sync.Source;
import tdl.s3.sync.destination.PerformanceMeasureDestination;
import tdl.s3.testframework.rules.LocalTestBucket;
import tdl.s3.testframework.rules.TemporarySyncFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static tdl.s3.testframework.rules.TemporarySyncFolder.ONE_MEGABYTE;

public class Upload_PerformanceTest {

    private static int PART_SIZE = 5 * 1024 * 1024;

    private PerformanceMeasureDestination destination;

    private Filters defaultFilters;

    public TemporarySyncFolder targetSyncFolder;

    public LocalTestBucket testBucket;

    @BeforeEach
    void setUp() throws Throwable {
        targetSyncFolder = new TemporarySyncFolder();
        targetSyncFolder.beforeEach();
        testBucket = new LocalTestBucket();
        testBucket.beforeEach();
        destination = new PerformanceMeasureDestination(testBucket.asDestination());
        defaultFilters = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .include(Filters.endsWith("bin"))
                .create();
    }

    @AfterEach
    void tearDown() {
        targetSyncFolder.afterEach();
    }


    @Test
    public void uploadAlreadyUploadedFiles() {
        //8 files inside
        Path path = Paths.get("src/test/resources/performance_test/already_uploaded/");
        testBucket.uploadFilesInsideDir(path);

        Source source = Source.getBuilder(path)
                .setFilters(defaultFilters)
                .create();
        RemoteSync sync = new RemoteSync(source, destination);
        sync.run();
        Assertions.assertEquals(destination.getPerformanceScore(), 1); //only call filterUploadable
    }
    
    @Test
    public void uploadPartialLargeMultipartFile() throws IOException {
        Path path = Paths.get("src/test/resources/performance_test/multipart_partial");
        File file = createRandomFile(path, PART_SIZE * 4);
        String fileName = file.getName();
        targetSyncFolder.addFile(file.getAbsolutePath());
        targetSyncFolder.lock(fileName);
        Path directoryPath = targetSyncFolder.getFolderPath();
        Source directorySource = Source.getBuilder(directoryPath)
                .setFilters(defaultFilters)
                .setRecursive(true)
                .create();
        
        RemoteSync directoryFirstSync = new RemoteSync(directorySource, destination);
        directoryFirstSync.run();
        Assertions.assertEquals(4003, destination.getPerformanceScore());
        
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE + ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);
        
        RemoteSync directorySecondSync = new RemoteSync(directorySource, destination);
        directorySecondSync.run();
        
        //+ 1 canUpload + 1 initUpload + 2000 uploadMultipart + 1 commit
        Assertions.assertEquals(6006, destination.getPerformanceScore());
        Files.delete(file.toPath());
    }

    private File createRandomFile(Path path, int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        File tmpFile = Paths.get(path.toString() + "/random-file.txt").toFile();
        FileUtils.deleteQuietly(tmpFile);
        FileUtils.touch(tmpFile);
        FileUtils.writeByteArrayToFile(tmpFile, b, false);
        return tmpFile;
    }
}
