package tdl.s3;


import lombok.Getter;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.nio.file.Files;

@Getter
public class TempFileRule extends ExternalResource {

    private File fileToUpload;
    private File lockFile;

    @Override
    protected void before() throws Throwable {
        fileToUpload = new File("src/test/resources/unfinished_writing_file_to_upload.bin");
        lockFile = new File("src/test/resources/unfinished_writing_file_to_upload.bin.lock");
        deleteFiles();
    }

    @Override
    protected void after() {
        deleteFiles();
    }

    private void deleteFiles() {
        try {
            Files.delete(fileToUpload.toPath());
            Files.delete(lockFile.toPath());
        } catch (Exception ignored) {
        }
    }
}
