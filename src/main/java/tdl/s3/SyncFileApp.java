package tdl.s3;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.beust.jcommander.JCommander;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import tdl.s3.cli.CLIParams;

import static tdl.s3.cli.CLIParams.SYNC_COMMAND;
import static tdl.s3.cli.CLIParams.UPLOAD_COMMAND;
import tdl.s3.credentials.AWSSecretsProvider;

public class SyncFileApp {

    private final String command;

    private final CLIParams params;

    private final Properties properties;

    private SyncFileApp(String command, CLIParams params) {
        this.command = command;
        this.params = params;
        this.properties = loadProperties();
        run();
    }

    public static void main(String[] args) {
        SyncFileApp app = createAppByArgs(args);
    }

    private static SyncFileApp createAppByArgs(String[] args) {
        CLIParams cliParams = new CLIParams();

        java.util.Map<String, Object> commands = new HashMap<String, Object>() {
            {
                put(UPLOAD_COMMAND, new CLIParams.UploadCommand());
                put(SYNC_COMMAND, new CLIParams.SyncCommand());
            }
        };

        JCommander jCommander = new JCommander(cliParams);
        jCommander.addCommand(commands.get(UPLOAD_COMMAND));
        jCommander.addCommand(commands.get(SYNC_COMMAND));
        jCommander.parse(args);

        String command = jCommander.getParsedCommand();

        SyncFileApp app = new SyncFileApp(command, cliParams);
        return app;
    }

    private Properties loadProperties() {
        Path propertiesPath = Paths.get(params.getConfigurationPath());
        Properties prop = new Properties();
        try (InputStream inStream = Files.newInputStream(propertiesPath)) {
            prop.load(inStream);   
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return prop;
    }
    
    private AmazonS3 buildS3Client() {
        AWSSecretsProvider credentialsProvider = new AWSSecretsProvider(properties);
        return AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(credentialsProvider.getS3Region())
                .build();
    }

    private void run() {
        switch (command) {
            case UPLOAD_COMMAND:

                break;
            case SYNC_COMMAND:

                break;
        }
    }
}
