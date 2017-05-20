package tdl.s3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.RemoteTestBucket;
import tdl.s3.sync.Filters;
import tdl.s3.sync.RemoteSync;
import tdl.s3.sync.RemoteSyncException;
import tdl.s3.sync.Source;
import tdl.s3.sync.destination.DebugDestination;
import tdl.s3.sync.destination.Destination;
import tdl.s3.sync.destination.S3BucketDestination;

import static org.junit.Assert.*;
import tdl.s3.helpers.FileHelper;
import tdl.s3.rules.TemporarySyncFolder;
import static tdl.s3.rules.TemporarySyncFolder.ONE_MEGABYTE;
import static tdl.s3.rules.TemporarySyncFolder.PART_SIZE_IN_BYTES;

public class PerformanceTest {

    private static int PART_SIZE = 5 * 1024 * 1024;

    private DebugDestination destination;
    
    private Filters defaultFilters;

    @Rule
    public RemoteTestBucket remoteTestBucket = new RemoteTestBucket();
    
    @Rule
    public TemporarySyncFolder targetSyncFolder = new TemporarySyncFolder();

    @Before
    public void setUp() {
        destination = new DebugDestination((Destination) S3BucketDestination.createDefaultDestination());
        defaultFilters = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .include(Filters.endsWith("bin"))
                .create();
    }

    @Test
    public void uploadAlreadyUploadedFiles() throws RemoteSyncException {
        //8 files inside
        Path path = Paths.get("src/test/resources/performance_test/already_uploaded/");
        remoteTestBucket.uploadFilesInsideDir(path);

        Source source = Source.getBuilder(path)
                .setFilters(defaultFilters)
                .create();
        RemoteSync sync = new RemoteSync(source, destination);
        sync.run();
        assertEquals(destination.getCount(), 8); //only call canUpload
    }

    @Test
    public void uploadLargeMultipartFile() throws RemoteSyncException, FileNotFoundException, IOException {
        Path path = Paths.get("src/test/resources/performance_test/multipart");
        createRandomFile(path, PART_SIZE * 4);
        
        Source source = Source.getBuilder(path)
                .setFilters(defaultFilters)
                .create();
        RemoteSync sync = new RemoteSync(source, destination);
        sync.run();
        //4 x upload multipart, 4x greetings, tec.
        assertEquals(destination.getCount(), 4004);
    }
    
    @Test
    public void uploadPartialLargeMultipartFile() throws RemoteSyncException, FileNotFoundException, IOException {
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
        assertEquals(destination.getCount(), 4003);
        
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE + ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);
        
        RemoteSync directorySecondSync = new RemoteSync(directorySource, destination);
        directorySecondSync.run();
        
        //+ 1 canUpload + 1 initUpload + 2000 uploadMultipart + 1 commit
        assertEquals(destination.getCount(), 6006);
        Files.delete(file.toPath());
    }

    private File createRandomFile(Path path, int size) throws FileNotFoundException, IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        File tmpFile = Paths.get(path.toString() + "/random-file.txt").toFile();
        FileUtils.deleteQuietly(tmpFile);
        FileUtils.touch(tmpFile);
        FileUtils.writeByteArrayToFile(tmpFile, b, false);
        return tmpFile;
    }
}
