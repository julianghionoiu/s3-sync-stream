package tdl.s3.sync;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tdl.s3.sync.destination.Destination;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteSyncTest {

    @Test
    public void constructorShouldThrowExceptionIfSourceNotValidPath() {
        RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, () -> {
            Destination destination = mock(Destination.class);
            Source source = mock(Source.class);
            when(source.isValidPath()).thenReturn(false);
            new RemoteSync(source, destination);
        });
        MatcherAssert.assertThat(runtimeException.getMessage(), containsString("Source has to be a directory"));

    }

}
