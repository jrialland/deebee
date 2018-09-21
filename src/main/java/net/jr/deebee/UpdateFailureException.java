package net.jr.deebee;

public class UpdateFailureException extends RuntimeException {

    public UpdateFailureException(String message) {
        super(message);
    }

    public UpdateFailureException(Exception cause) {
        super(cause);
    }

}
