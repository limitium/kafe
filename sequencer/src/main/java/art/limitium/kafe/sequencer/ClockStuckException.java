package art.limitium.kafe.sequencer;

public class ClockStuckException extends RuntimeException{
    public ClockStuckException(String message) {
        super(message);
    }
}
