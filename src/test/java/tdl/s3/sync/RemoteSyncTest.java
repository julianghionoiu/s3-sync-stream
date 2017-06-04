package tdl.s3.sync;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import tdl.s3.sync.destination.Destination;

public class RemoteSyncTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void constructorShouldThrowExceptionIfSourceNotValidPath() {
        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("Source has to be a directory");
        Destination destination = mock(Destination.class);
        Source source = mock(Source.class);
        when(source.isValidPath()).thenReturn(false);
        new RemoteSync(source, destination);
    }

}
