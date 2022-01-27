package dev.vality.kafka.common.exception;

public class TransportException extends RuntimeException {

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public TransportException(String message) {
        super(message);
    }

    public TransportException(Throwable cause) {
        super(cause);
    }

}
