package tdl.s3;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.RemoteTestBucket;

public class SyncFileAppTest {

    @Rule
    public RemoteTestBucket remoteTestBucket = new RemoteTestBucket();

    @Test
    public void shouldUploadAllNewFilesFromFolder() throws Exception {

        assertThat(remoteTestBucket.doesObjectExists("test_file_1.txt"), is(false));
        assertThat(remoteTestBucket.doesObjectExists("test_file_2.txt"), is(false));
        assertThat(remoteTestBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(false));

        //synchronize folder
        String[] args = ("-c .private/aws-test-secrets -d src/test/resources/test_dir/ --filter \"^[0-9a-zA-Z\\_]+\\.txt$\" -R").split(" ");
        SyncFileApp.main(args);

        //state after sync
        assertThat(remoteTestBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("test_file_2.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(true));
    }
}
