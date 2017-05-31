package tdl.s3.rules;

import io.findify.s3mock.S3Mock;
import lombok.extern.slf4j.Slf4j;
import org.junit.rules.ExternalResource;

/**
 * This class has to be executed as @ClassRule
 */
@Slf4j
public class LocalS3Server extends ExternalResource {

    private final S3Mock api;

    public LocalS3Server() {
        api = new S3Mock.Builder()
                .withPort(8001)
                .withInMemoryBackend()
                .build();
    }

    @Override
    protected void before() throws Throwable {
        log.debug("Starting server");
        api.start();
    }

    @Override
    protected void after() {
        log.debug("Stopping server");
        api.stop();
    }

}
