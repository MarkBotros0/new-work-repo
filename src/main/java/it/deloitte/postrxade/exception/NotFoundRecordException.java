package it.deloitte.postrxade.exception;

/**
 * Exception thrown when a record can not be found on the database
 */
public class NotFoundRecordException extends PosAppException {
	private static final long serialVersionUID = 1L;

	public NotFoundRecordException(String message) {
		super(message);
	}

	public NotFoundRecordException(String message, Throwable cause) {
		super(message, cause);
	}
}
