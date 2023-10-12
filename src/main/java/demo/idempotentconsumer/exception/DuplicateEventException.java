package demo.idempotentconsumer.exception;

import java.util.UUID;

public class DuplicateEventException extends RuntimeException  {
    public DuplicateEventException(final UUID eventId) {
        super("Duplicate event Id: "+ eventId.toString());
    }
}
