package demo.idempotentconsumer.exception;

public class SimulatedFailureException extends RuntimeException  {
    public SimulatedFailureException(final String message) {
        super("Simulated Failure: " + message);
    }
}