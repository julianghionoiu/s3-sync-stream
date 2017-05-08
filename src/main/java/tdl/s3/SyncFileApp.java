package tdl.s3;

import com.beust.jcommander.JCommander;
import tdl.s3.cli.CLIParams;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static tdl.s3.cli.CLIParams.SYNC_COMMAND;
import static tdl.s3.cli.CLIParams.UPLOAD_COMMAND;
import tdl.s3.sync.Destination;
import tdl.s3.sync.Source;

public class SyncFileApp {
    
    private final String command;
    
    private final CLIParams params;
    
    private final Destination destination = Destination.createDefaultDestination();

    private SyncFileApp(String command, CLIParams params) {
        this.command = command;
        this.params = params;
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

        SyncFileApp main = new SyncFileApp(command, cliParams);

        main.run();
    }

    private void run() {
        RemoteSync remoteSync = null;
        switch (command) {
            case UPLOAD_COMMAND:
                remoteSync = upload();
                break;
            case SYNC_COMMAND:
                remoteSync = sync();
                break;
            default:
                throw new UnsupportedOperationException("Unknown command : " + command);
        }
        remoteSync.run();
    }

    private RemoteSync upload() {
        CLIParams.UploadCommand uploadCommand = (CLIParams.UploadCommand) params.getCommand();
        Path path = Paths.get(uploadCommand.getFilePath());
        Source source = Source.getBuilder(path).create();
        RemoteSync sync = new RemoteSync(source, destination);
        return sync;
    }

    private RemoteSync sync() {
        CLIParams.SyncCommand syncCommand = (CLIParams.SyncCommand) params.getCommand();
        Path path = Paths.get(syncCommand.getDirPath());
        Source source = Source.getBuilder(path)
                .setRecursive(syncCommand.isRecursive())
                .create();
        RemoteSync sync = new RemoteSync(source, destination);
        return sync;
    }

}
