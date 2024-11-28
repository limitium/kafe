package art.limitium.kafe.sequencer;

public class ClockWentBackException extends RuntimeException{
    public ClockWentBackException(String message) {
        super(message);
    }
}
