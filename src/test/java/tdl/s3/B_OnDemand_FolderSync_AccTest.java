package tdl.s3;

import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.RemoteTestBucketRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class B_OnDemand_FolderSync_AccTest {

    @Rule
    public RemoteTestBucketRule remoteTestBucket = new RemoteTestBucketRule();

    @Test
    public void should_upload_all_new_files_from_folder() throws Exception {
        String[] uploadingArgs = ("upload -f " + "src/test/resources/test_dir/test_file_1.txt").split(" ");
        //upload first file
        SyncFileApp.main(uploadingArgs);

        //state before first upload
        assertThat(remoteTestBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("test_file_2.txt"), is(false));
        assertThat(remoteTestBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(false));

        //synchronize folder
        String[] syncArgs = ("sync -d " + "src/test/resources/test_dir -R").split(" ");
        SyncFileApp.main(syncArgs);

        //state after sync
        assertThat(remoteTestBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("test_file_2.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(true));
    }

}