package tdl.s3;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Test;
import tdl.s3.sync.FolderScannerImpl;
import tdl.s3.sync.FolderSynchronizer;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class B_OnDemand_FolderSync_AccTest extends AbstractAcceptanceTestCase {

    private FolderSynchronizer folderSynchronizer;

    @Override
    protected void additionalSetUp() {
        //Delete previously uploaded files if present
        amazonS3.deleteObject(bucketName, "test_file_1.txt");
        amazonS3.deleteObject(bucketName, "test_file_2.txt");
        amazonS3.deleteObject(bucketName, "subdir/sub_test_file_1.txt");

        folderSynchronizer = new FolderSynchronizer(new FolderScannerImpl(), fileUploadingService);
    }

    @Test
    public void should_upload_all_new_files_from_folder() throws Exception {
        //upload first file
        fileUploadingService.upload(new File("src/test/resources/test_dir/test_file_1.txt"), "test_file_1.txt");

        //assert that this file exists
        ObjectMetadata existing = getObjectMetadata("test_file_1.txt");
        assertNotNull(existing);

        //assert that other files doesn't exist
        ObjectMetadata other_1 = getObjectMetadata("test_file_2.txt");
        assertNull(other_1);
        ObjectMetadata other_2 = getObjectMetadata("subdir/sub_test_file_1.txt");
        assertNull(other_2);

        //synchronize folder
        Path testFolderPath = Paths.get("src", "test", "resources", "test_dir");
        folderSynchronizer.synchronize(testFolderPath, true);

        //assert that all files exist
        existing = getObjectMetadata("test_file_1.txt");
        assertNotNull(existing);
        other_1 = getObjectMetadata("test_file_2.txt");
        assertNotNull(other_1);
        other_2 = getObjectMetadata("subdir/sub_test_file_1.txt");
        assertNotNull(other_2);
    }

}