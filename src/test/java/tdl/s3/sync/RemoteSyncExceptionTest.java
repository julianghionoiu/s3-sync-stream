package tdl.s3.sync;

import org.junit.Test;

public class RemoteSyncExceptionTest {

    @Test(expected = RemoteSyncException.class)
    public void test() throws Exception {
        throw new RemoteSyncException("Message", new Exception("Cause"));
    }
}
