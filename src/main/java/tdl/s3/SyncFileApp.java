package tdl.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import tdl.s3.sync.FolderScannerImpl;
import tdl.s3.sync.FolderSynchronizer;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.nio.file.Paths;
import java.util.Scanner;

public class SyncFileApp {

    private static final String UPLOAD_COMMAND = "upload";
    private static final String SYNC_COMMAND = "sync";

    @Parameter(names = {"--access-key", "-a"}, required = true)
    private String accessKey;

    @Parameter(names = {"--secret-key", "-s"}, required = true)
    private String secretKey;

    @Parameter(names = {"--region", "-r"}, required = true)
    private String region;

    @Parameter(names = {"--bucket", "-b"}, required = true)
    private String bucket;

    @Parameters(commandDescription = "Upload single file", commandNames = UPLOAD_COMMAND)
    private class UploadCommand {
        @Parameter(names = {"--file", "-f"}, required = true)
        private String filePath;
    }

    @Parameters(commandDescription = "Sync folder content", commandNames = SYNC_COMMAND)
    private class SyncCommand {
        @Parameter(names = {"--dir", "-d"}, required = true)
        private String dirPath;

        @Parameter(names = {"--recursive", "-R"})
        private boolean recursive;
    }

    private UploadCommand uploadCommand = new UploadCommand();

    private SyncCommand syncCommand = new SyncCommand();

    private FileUploadingService fileUploadingService;

    private FolderSynchronizer folderSynchronizer;


    public static void main(String[] args) {
        SyncFileApp main = new SyncFileApp();

        JCommander jCommander = new JCommander(main);
        jCommander.addCommand(main.uploadCommand);
        jCommander.addCommand(main.syncCommand);
        jCommander.parse(args);

        String command = jCommander.getParsedCommand();

        main.run(command);
    }

    public void run(String command) {
        prepare();
        switch (command) {
            case UPLOAD_COMMAND: upload(); break;
            case SYNC_COMMAND: sync(); break;
        }
    }

    private void prepare() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(region)
                .build();

        fileUploadingService = new FileUploadingService(amazonS3, bucket);

        folderSynchronizer = new FolderSynchronizer(new FolderScannerImpl(), fileUploadingService);
    }

    private void upload() {
        File file = new File(uploadCommand.filePath);
        fileUploadingService.upload(file);
    }

    private void sync() {
        folderSynchronizer.synchronize(Paths.get(syncCommand.dirPath), syncCommand.recursive);
    }

}
