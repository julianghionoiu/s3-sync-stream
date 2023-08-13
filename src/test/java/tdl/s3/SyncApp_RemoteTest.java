package tdl.s3;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdl.s3.testframework.rules.RemoteTestBucket;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SyncApp_RemoteTest {

    public RemoteTestBucket testBucket;

    @BeforeEach
    void setUp() {
        testBucket = new RemoteTestBucket();
        testBucket.beforeEach();
    }

    @Test
    public void shouldUploadAllNewFilesFromFolder() throws Exception {

        //synchronize folder
        String[] args = ("-c .private/aws-test-secrets -d src/test/resources/test_dir/ --filter \"^[0-9a-zA-Z\\_]+\\.txt$\" -R").split(" ");
        SyncFileApp.main(args);

        //state after sync
        MatcherAssert.assertThat(testBucket.doesObjectExists("test_file_1.txt"), is(true));
        MatcherAssert.assertThat(testBucket.doesObjectExists("test_file_2.txt"), is(true));
        MatcherAssert.assertThat(testBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(true));
    }
}
