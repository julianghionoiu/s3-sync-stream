package tdl.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class B_OnDemand_FolderSync_AccTest {

    @Rule
    public FileCheckingRule fileChecking = new FileCheckingRule();

    @Before
    public void additionalSetUp() {
        //Delete previously uploaded files if present
        fileChecking.deleteObjects("test_file_1.txt", "test_file_2.txt", "subdir/sub_test_file_1.txt");
    }

    @Test
    public void should_upload_all_new_files_from_folder() throws Exception {
        String[] uploadingArgs = (fileChecking.commonArgs() + "upload -f " + "src/test/resources/test_dir/test_file_1.txt").split(" ");
        //upload first file
        SyncFileApp.main(uploadingArgs);

        //assert that this file exists
        ObjectMetadata existing = fileChecking.getObjectMetadata("test_file_1.txt");
        assertNotNull(existing);

        //assert that other files doesn't exist
        ObjectMetadata other_1 = fileChecking.getObjectMetadata("test_file_2.txt");
        assertNull(other_1);
        ObjectMetadata other_2 = fileChecking.getObjectMetadata("subdir/sub_test_file_1.txt");
        assertNull(other_2);

        //synchronize folder
        String[] syncArgs = (fileChecking.commonArgs() + "sync -d " + "src/test/resources/test_dir -R").split(" ");
        SyncFileApp.main(syncArgs);

        //assert that all files exist
        existing = fileChecking.getObjectMetadata("test_file_1.txt");
        assertNotNull(existing);
        other_1 = fileChecking.getObjectMetadata("test_file_2.txt");
        assertNotNull(other_1);
        other_2 = fileChecking.getObjectMetadata("subdir/sub_test_file_1.txt");
        assertNotNull(other_2);
    }

}