package tdl.s3;

import com.amazonaws.services.s3.model.*;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.testframework.rules.LocalTestBucket;
import tdl.s3.testframework.rules.TemporarySyncFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import org.junit.Before;

import static tdl.s3.testframework.rules.TemporarySyncFolder.ONE_MEGABYTE;
import static tdl.s3.testframework.rules.TemporarySyncFolder.PART_SIZE_IN_BYTES;

import tdl.s3.sync.RemoteSync;
import tdl.s3.sync.Filters;
import tdl.s3.sync.Source;

@Slf4j
public class IncompleteFileUpload_AcceptanceTest {

    private Filters defaultFilters;

    @Rule
    public LocalTestBucket testBucket = new LocalTestBucket();

    @Rule
    public TemporarySyncFolder targetSyncFolder = new TemporarySyncFolder();
    
    @Before
    public void setUp() {
        defaultFilters = Filters.getBuilder()
                .include(Filters.endsWith("txt"))
                .include(Filters.endsWith("bin"))
                .create();
    }

    @Test
    public void should_upload_incomplete_file() throws Exception {
        String fileName = "unfinished_writing_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);

        //synchronize folder
        Path directoryPath = targetSyncFolder.getFolderPath();
        Source directorySource = Source.getBuilder(directoryPath)
                .setFilters(defaultFilters)
                .setRecursive(true)
                .create();
        
        RemoteSync directorySync = new RemoteSync(directorySource, testBucket.asDestination());
        directorySync.run();

        //Check that the file still not exists on the server
        assertThat(testBucket.doesObjectExists(fileName), is(false));

        //Check multipart upload exists
        MultipartUpload multipartUpload = testBucket.getMultipartUploadFor(fileName)
                .orElseThrow(() -> new AssertionError("Found no multipart upload for: "+fileName));

        //and the parts have the expected ETag
        Map<Integer, String> hashes = targetSyncFolder.getPartsHashes(fileName);
        testBucket.getPartsFor(multipartUpload).forEach(partSummary -> comparePart(partSummary, hashes));
    }

    private void comparePart(PartSummary partSummary, Map<Integer, String> hashes) {
        int partNumber = partSummary.getPartNumber();
        assertEquals(hashes.get(partNumber), partSummary.getETag());
    }

    @Test
    public void should_be_able_to_upload_failed_parts() throws Exception {
        String fileName = "unfinished_writing_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);

        String bucket = testBucket.getBucketName();
        String uploadId = testBucket.initiateMultipartUpload(fileName);
        
        //write third part of data
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE_IN_BYTES);

        //upload first and third part
        byte[] fileContent = Files.readAllBytes(Paths.get(targetSyncFolder.getFolderPath() + "/" + fileName));
        byte[] firstPart = new byte[PART_SIZE_IN_BYTES];
        byte[] thirdPart = new byte[PART_SIZE_IN_BYTES];
        System.arraycopy(fileContent, 0, firstPart, 0, PART_SIZE_IN_BYTES);
        System.arraycopy(fileContent, PART_SIZE_IN_BYTES * 2, thirdPart, 0, PART_SIZE_IN_BYTES);
        testBucket.uploadPart(fileName, uploadId, firstPart, 1);
        testBucket.uploadPart(fileName, uploadId, thirdPart, 3);

        //write additional data and delete lock file
        targetSyncFolder.writeBytesToFile(fileName, ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);

        //synchronize folder
        Path directoryPath = targetSyncFolder.getFolderPath();
        Source directorySource = Source.getBuilder(directoryPath)
                .setFilters(defaultFilters)
                .setRecursive(true)
                .create();
        
        RemoteSync directorySync = new RemoteSync(directorySource, testBucket.asDestination());
        directorySync.run();

        //Check that the file exists on the server
        assertThat(testBucket.doesObjectExists(fileName), is(true));

        //Check that multipart upload completed and not exists anymore
        assertThat(testBucket.getMultipartUploadFor(fileName), is(Optional.empty()));

        //check complete file hash. ETag of complete file consists from complete file MD5 hash and parts count after "-" sign
        assertTrue(testBucket.getObjectMetadata(fileName)
                .getETag().startsWith(targetSyncFolder.getCompleteFileMD5(fileName)));
    }



    @Test
    public void should_be_able_upload_empty_file_continue_incomplete_file_and_finalise() throws Exception {
        String fileName = "empty_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);

        //Upload empty file
        Path directoryPath = targetSyncFolder.getFolderPath();
        Source directorySource = Source.getBuilder(directoryPath)
                .setFilters(defaultFilters)
                .setRecursive(true)
                .create();
        
        RemoteSync directorySync = new RemoteSync(directorySource, testBucket.asDestination());
        directorySync.run();
        
        assertThat(testBucket.doesObjectExists(fileName), is(false));
        List<PartSummary> list1 = testBucket.getPartsForKey(fileName);
        assertNotNull(list1);
        assertTrue(list1.isEmpty());
        
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE_IN_BYTES + ONE_MEGABYTE);
        
        //Upload incomplete file file
        directorySync.run();
        
        assertThat(testBucket.doesObjectExists(fileName), is(false));
        List<PartSummary> list2 = testBucket.getPartsForKey(fileName);
        assertNotNull(list2);
        assertFalse(list2.isEmpty());
        
        targetSyncFolder.writeBytesToFile(fileName, 3 * PART_SIZE_IN_BYTES + ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);
        
        //Finalize
        directorySync.run();
        
        assertThat(testBucket.doesObjectExists(fileName), is(true));
        
        assertThat(testBucket.getMultipartUploadFor(fileName), is(Optional.empty()));
        
        assertTrue(testBucket.getObjectMetadata(fileName)
                .getETag().startsWith(targetSyncFolder.getCompleteFileMD5(fileName)));
    }
}