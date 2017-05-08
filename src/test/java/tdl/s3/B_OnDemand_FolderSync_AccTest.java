package tdl.s3;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import tdl.s3.rules.RemoteTestBucket;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import tdl.s3.sync.Destination;
import tdl.s3.sync.Filters;
import tdl.s3.sync.Source;

public class B_OnDemand_FolderSync_AccTest {

    private Destination destination;
    
    @Rule
    public RemoteTestBucket remoteTestBucket = new RemoteTestBucket();
    
    @Before
    public void setUp() {
        destination = Destination.createDefaultDestination();
    }

    @Test
    public void should_upload_all_new_files_from_folder() throws Exception {
        
        Path filePath = Paths.get("src/test/resources/test_dir/test_file_1.txt");
        Filters filters = Filters.getBuilder().include(Filters.endsWith("txt")).create();
        Source fileSource = Source.getBuilder(filePath)
                .setFilters(filters)
                .create();
        
        //upload first file
        RemoteSync fileSync = new RemoteSync(fileSource, destination);
        fileSync.run();

        //state before first upload
        assertThat(remoteTestBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("test_file_2.txt"), is(false));
        assertThat(remoteTestBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(false));

        //synchronize folder
        Path directoryPath = Paths.get("src/test/resources/test_dir");
        Source directorySource = Source.getBuilder(directoryPath)
                .setFilters(filters)
                .setRecursive(true)
                .create();
        
        RemoteSync directorySync = new RemoteSync(directorySource, destination);
        directorySync.run();

        //state after sync
        assertThat(remoteTestBucket.doesObjectExists("test_file_1.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("test_file_2.txt"), is(true));
        assertThat(remoteTestBucket.doesObjectExists("subdir/sub_test_file_1.txt"), is(true));
    }

}