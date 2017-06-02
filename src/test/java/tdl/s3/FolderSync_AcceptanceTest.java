package tdl.s3;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.testframework.rules.LocalTestBucket;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import tdl.s3.sync.RemoteSync;
import tdl.s3.sync.Filters;
import tdl.s3.sync.Source;

public class FolderSync_AcceptanceTest {
    
    @Rule
    public LocalTestBucket testBucket = new LocalTestBucket();

    @Test
    public void should_upload_all_new_files_from_folder() throws Exception {
        //state before first upload
        Path filePath = Paths.get("src/test/resources/test_dir/test_file_1.txt");
        testBucket.upload("test_file_1.txt", filePath);

        assertThat(testBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(testBucket.doesObjectExists("test_file_2.txt"), is(false));
        assertThat(testBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(false));

        //synchronize folder
        Path directoryPath = Paths.get("src/test/resources/test_dir");
        Filters filters = Filters.getBuilder().include(Filters.endsWith("txt")).create();
        Source directorySource = Source.getBuilder(directoryPath)
                .setFilters(filters)
                .setRecursive(true)
                .create();

        RemoteSync directorySync = new RemoteSync(directorySource, testBucket.asDestination());
        directorySync.run();

        //state after sync
        assertThat(testBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(testBucket.doesObjectExists("test_file_2.txt"), is(true));
        assertThat(testBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(true));
    }

}
