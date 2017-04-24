package tdl.s3;

import com.amazonaws.services.s3.model.*;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.RemoteTestBucket;
import tdl.s3.rules.TemporarySyncFolder;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static tdl.s3.rules.TemporarySyncFolder.ONE_MEGABYTE;
import static tdl.s3.rules.TemporarySyncFolder.PART_SIZE_IN_BYTES;

public class C_OnDemand_IncompleteFileUpload_AccTest {

    @Rule
    public RemoteTestBucket remoteTestBucket = new RemoteTestBucket();

    @Rule
    public TemporarySyncFolder targetSyncFolder = new TemporarySyncFolder();

    @Test
    public void should_upload_incomplete_file() throws Exception {
        String fileName = "unfinished_writing_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);

        //synchronize folder
        String[] syncArgs = ("sync -d " + targetSyncFolder.getFolderPath()+ " -R").split(" ");
        SyncFileApp.main(syncArgs);

        //Check that the file still not exists on the server
        assertThat(remoteTestBucket.doesObjectExists(fileName), is(false));

        //Check multipart upload exists
        MultipartUpload multipartUpload = remoteTestBucket.getMultipartUploadFor(fileName)
                .orElseThrow(() -> new AssertionError("Found no multipart upload for: "+fileName));

        //and the parts have the expected ETag
        Map<Integer, String> hashes = targetSyncFolder.getPartsHashes(fileName);
        remoteTestBucket.getPartsFor(multipartUpload).forEach(partSummary -> comparePart(partSummary, hashes));
    }

    private void comparePart(PartSummary partSummary, Map<Integer, String> hashes) {
        int partNumber = partSummary.getPartNumber();
        assertEquals(hashes.get(partNumber), partSummary.getETag());
    }


    @Test
    public void should_be_able_to_continue_incomplete_file_and_finalise() throws Exception {
        String fileName = "unfinished_writing_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);

        //synchronize folder
        String[] syncArgs = ("sync -d " + targetSyncFolder.getFolderPath()+ " -R").split(" ");
        SyncFileApp.main(syncArgs);

        //write additional data and delete lock file
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE_IN_BYTES + ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);

        //synchronize folder
        SyncFileApp.main(syncArgs);

        //Check that the file exists on the server
        assertThat(remoteTestBucket.doesObjectExists(fileName), is(true));

        //Check that multipart upload completed and not exists anymore
        assertThat(remoteTestBucket.getMultipartUploadFor(fileName), is(Optional.empty()));

        //check complete file hash. ETag of complete file consists from complete file MD5 hash and some part after "-" sign(probably file version number)
        assertTrue(remoteTestBucket.getObjectMetadata(fileName)
                .getETag().startsWith(targetSyncFolder.getCompleteFileMD5(fileName)));
    }

    @Test
    public void should_be_able_to_upload_failed_parts() throws Exception {
        String fileName = "unfinished_writing_file.bin";
        targetSyncFolder.addFileFromResources(fileName);
        targetSyncFolder.lock(fileName);

        String bucket = remoteTestBucket.getBucketName();
        InitiateMultipartUploadResult result = remoteTestBucket.getAmazonS3().initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, fileName));
        //write third part of data
        targetSyncFolder.writeBytesToFile(fileName, PART_SIZE_IN_BYTES);

        //upload first and third part
        byte[] fileContent = Files.readAllBytes(Paths.get(targetSyncFolder.getFolderPath() + "/" + fileName));
        byte[] firstPart = new byte[PART_SIZE_IN_BYTES];
        byte[] thirdPart = new byte[PART_SIZE_IN_BYTES];
        System.arraycopy(fileContent, 0, firstPart, 0, PART_SIZE_IN_BYTES);
        System.arraycopy(fileContent, PART_SIZE_IN_BYTES * 2, thirdPart, 0, PART_SIZE_IN_BYTES);
        uploadPart(fileName, bucket, result, firstPart, 1);
        uploadPart(fileName, bucket, result, thirdPart, 3);

        //write additional data and delete lock file
        targetSyncFolder.writeBytesToFile(fileName, ONE_MEGABYTE);
        targetSyncFolder.unlock(fileName);

        //synchronize folder
        String[] syncArgs = ("sync -d " + targetSyncFolder.getFolderPath()+ " -R").split(" ");
        SyncFileApp.main(syncArgs);

        //Check that the file exists on the server
        assertThat(remoteTestBucket.doesObjectExists(fileName), is(true));

        //Check that multipart upload completed and not exists anymore
        assertThat(remoteTestBucket.getMultipartUploadFor(fileName), is(Optional.empty()));

        //check complete file hash. ETag of complete file consists from complete file MD5 hash and parts count after "-" sign
        assertTrue(remoteTestBucket.getObjectMetadata(fileName)
                .getETag().startsWith(targetSyncFolder.getCompleteFileMD5(fileName)));
    }

    private void uploadPart(String fileName, String bucket, InitiateMultipartUploadResult result, byte[] firstPart, int partNumber) throws NoSuchAlgorithmException {
        UploadPartRequest request = new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(fileName)
                .withPartNumber(partNumber)
                .withMD5Digest(Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(firstPart)))
                .withPartSize(PART_SIZE_IN_BYTES)
                .withUploadId(result.getUploadId())
                .withInputStream(new ByteArrayInputStream(firstPart));
        remoteTestBucket.getAmazonS3().uploadPart(request);
    }

}