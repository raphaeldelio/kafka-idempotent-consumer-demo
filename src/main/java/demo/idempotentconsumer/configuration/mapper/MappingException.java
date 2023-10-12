package demo.idempotentconsumer.configuration.mapper;

public class MappingException extends RuntimeException {

    public MappingException(Throwable t) {
        super(t);
    }
}
