package tdl.s3.rules;

import io.findify.s3mock.S3Mock;
import org.junit.rules.ExternalResource;

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
        api.start();
    }

    @Override
    protected void after() {
        api.stop();
    }

}
