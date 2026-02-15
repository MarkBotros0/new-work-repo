package it.deloitte.postrxade.exception;

/**
 * Exception thrown when the authority code of a Legal Entity is not in a valid form
 */
public class ActionNotPermittedException extends PosAppException {

    private static final long serialVersionUID = 1L;
    public ActionNotPermittedException(String message) {
        super(message);
    }
    public ActionNotPermittedException(String message, Throwable cause) {
        super(message, cause);
    }

}