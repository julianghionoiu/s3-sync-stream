package tdl.s3.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Data;

/**
 * @author vdanyliuk
 * @version 13.04.17.
 */
@Data
public class CLIParams {

    public static final String UPLOAD_COMMAND = "upload";
    public static final String SYNC_COMMAND = "sync";

    @Parameter(names = {"--access-key", "-a"}, required = true)
    private String accessKey;

    @Parameter(names = {"--secret-key", "-s"}, required = true)
    private String secretKey;

    @Parameter(names = {"--region", "-r"}, required = true)
    private String region;

    @Parameter(names = {"--bucket", "-b"}, required = true)
    private String bucket;

    private Object command;

    @Data
    @Parameters(commandDescription = "Upload single file", commandNames = UPLOAD_COMMAND)
    public static class UploadCommand {
        @Parameter(names = {"--file", "-f"}, required = true)
        private String filePath;
    }

    @Data
    @Parameters(commandDescription = "Sync folder content", commandNames = SYNC_COMMAND)
    public static class SyncCommand {
        @Parameter(names = {"--dir", "-d"}, required = true)
        private String dirPath;

        @Parameter(names = {"--recursive", "-R"})
        private boolean recursive;
    }

}
