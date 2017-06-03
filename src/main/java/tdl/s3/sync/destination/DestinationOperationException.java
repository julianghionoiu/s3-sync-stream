package tdl.s3.sync.destination;

public class DestinationOperationException extends Exception {

    public DestinationOperationException(String message) {
        super(message);
    }

    public DestinationOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
