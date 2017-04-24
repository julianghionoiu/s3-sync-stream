package tdl.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.beust.jcommander.JCommander;
import tdl.s3.cli.CLIParams;
import tdl.s3.credentials.AWSSecretsProvider;
import tdl.s3.sync.FolderScannerImpl;
import tdl.s3.sync.FolderSynchronizer;
import tdl.s3.upload.FileUploadingService;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static tdl.s3.cli.CLIParams.SYNC_COMMAND;
import static tdl.s3.cli.CLIParams.UPLOAD_COMMAND;

public class SyncFileApp {

    private static final List<String> FILTERED_EXTENSIONS = Collections.singletonList(".lock");

    private FileUploadingService fileUploadingService;

    private FolderSynchronizer folderSynchronizer;

    private SyncFileApp(FileUploadingService fileUploadingService, FolderSynchronizer folderSynchronizer) {
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

        SyncFileApp main = prepare();

        main.run(command, cliParams);
    }

    private static SyncFileApp prepare() {
        Path privatePropertiesFile = Paths.get(".private", "aws-test-secrets");
        AWSSecretsProvider secretsProvider = new AWSSecretsProvider(privatePropertiesFile);


        AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(secretsProvider)
                .withRegion(secretsProvider.getS3Region())
                .build();

        FileUploadingService fileUploadingService = new FileUploadingService(amazonS3, secretsProvider.getS3Bucket());

        List<Predicate<Path>> filters = FILTERED_EXTENSIONS.stream()
                .map(ext -> (Predicate<Path>)(path -> ! path.toString().endsWith(ext)))
                .collect(Collectors.toList());

        FolderScannerImpl folderScanner = new FolderScannerImpl(filters);
        FolderSynchronizer folderSynchronizer = new FolderSynchronizer(folderScanner, fileUploadingService);

        return new SyncFileApp(fileUploadingService, folderSynchronizer);
    }

    private void run(String command, CLIParams cliParams) {
        prepare();
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
