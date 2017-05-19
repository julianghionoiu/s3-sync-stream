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

public class PerformanceTest {

    private static int PART_SIZE = 5 * 1024 * 1024;

    private DebugDestination destination;

    @Rule
    public RemoteTestBucket remoteTestBucket = new RemoteTestBucket();

    @Before
    public void setUp() {
        destination = new DebugDestination((Destination) S3BucketDestination.createDefaultDestination());
    }

    @Test
    public void uploadAlreadyUploadedFiles() throws RemoteSyncException {
        //8 files inside
        Path path = Paths.get("src/test/resources/performance_test/already_uploaded/");
        remoteTestBucket.uploadFilesInsideDir(path);

        Filters filters = Filters.getBuilder().include(Filters.endsWith("txt")).create();
        Source source = Source.getBuilder(path)
                .setFilters(filters)
                .create();
        RemoteSync sync = new RemoteSync(source, destination);
        sync.run();
        assertEquals(destination.getCount(), 8); //only call canUpload
    }

    @Test
    public void uploadLargeMultipartFile() throws RemoteSyncException, FileNotFoundException, IOException {
        Path path = Paths.get("src/test/resources/performance_test/multipart");
        createRandomFile(path, PART_SIZE * 4);
        
        Filters filters = Filters.getBuilder().include(Filters.endsWith("txt")).create();
        Source source = Source.getBuilder(path)
                .setFilters(filters)
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
        remoteTestBucket.upload(file.getName(), file.toPath());
        
        byte[] b = new byte[PART_SIZE];
        new Random().nextBytes(b);
        FileUtils.writeByteArrayToFile(file, b, true);
        assertEquals(file.length(), PART_SIZE * 5);
        Path lockFile = FileHelper.getLockFilePath(file);
        Files.write(lockFile, new byte[] {0});
        
        Filters filters = Filters.getBuilder().include(Filters.endsWith("txt")).create();
        Source source = Source.getBuilder(path)
                .setFilters(filters)
                .create();
        RemoteSync sync = new RemoteSync(source, destination);
        sync.run();
        
        assertEquals(destination.getCount(), 1001);
        Files.delete(lockFile);
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
