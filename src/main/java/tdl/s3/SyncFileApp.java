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
import tdl.s3.cli.CLIParams;
import tdl.s3.sync.FolderScannerImpl;
import tdl.s3.sync.FolderSynchronizer;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;

import static tdl.s3.cli.CLIParams.SYNC_COMMAND;
import static tdl.s3.cli.CLIParams.UPLOAD_COMMAND;

public class SyncFileApp {

    private FileUploadingService fileUploadingService;

    private FolderSynchronizer folderSynchronizer;

    public SyncFileApp(FileUploadingService fileUploadingService, FolderSynchronizer folderSynchronizer) {
        this.fileUploadingService = fileUploadingService;
        this.folderSynchronizer = folderSynchronizer;
    }

    public static void main(String[] args) {
        CLIParams cliParams = new CLIParams();

        java.util.Map<String, Object> commands = new HashMap<String, Object>(){{
            put(UPLOAD_COMMAND, new CLIParams.UploadCommand());
            put(SYNC_COMMAND, new CLIParams.SyncCommand());
        }};

        JCommander jCommander = new JCommander(cliParams);
        jCommander.addCommand(commands.get(UPLOAD_COMMAND));
        jCommander.addCommand(commands.get(SYNC_COMMAND));
        jCommander.parse(args);

        String command = jCommander.getParsedCommand();
        cliParams.setCommand(commands.get(command));

        SyncFileApp main = prepare(cliParams);

        main.run(command, cliParams);
    }

    private static SyncFileApp prepare(CLIParams cliParams) {
        AWSCredentials credentials = new BasicAWSCredentials(cliParams.getAccessKey(), cliParams.getSecretKey());
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(cliParams.getRegion())
                .build();

        FileUploadingService fileUploadingService = new FileUploadingService(amazonS3, cliParams.getBucket());

        FolderSynchronizer folderSynchronizer = new FolderSynchronizer(new FolderScannerImpl(), fileUploadingService);

        return new SyncFileApp(fileUploadingService, folderSynchronizer);
    }

    private void run(String command, CLIParams cliParams) {
        prepare(cliParams);
        switch (command) {
            case UPLOAD_COMMAND: {
                CLIParams.UploadCommand uploadCommand = (CLIParams.UploadCommand) cliParams.getCommand();
                upload(uploadCommand.getFilePath());
                break;
            }
            case SYNC_COMMAND: {
                CLIParams.SyncCommand syncCommand = (CLIParams.SyncCommand) cliParams.getCommand();
                sync(syncCommand.getDirPath(), syncCommand.isRecursive());
                break;
            }
        }
    }

    private void upload(String filePath) {
        File file = new File(filePath);
        fileUploadingService.upload(file);
    }

    private void sync(String dirPath, boolean recursive) {
        folderSynchronizer.synchronize(Paths.get(dirPath), recursive);
    }

}
