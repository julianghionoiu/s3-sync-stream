
package tdl.s3;

import tdl.s3.sync.SyncDestination;
import tdl.s3.sync.SyncSource;

public class RemoteSync {

    private final SyncSource source;
    
    private final SyncDestination destination;

    public RemoteSync(SyncSource source, SyncDestination destination) {
        this.source = source;
        this.destination = destination;
    }
    
    public void run() {
        
    }
}
