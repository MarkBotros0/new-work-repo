package it.deloitte.postrxade.exception;

/**
 * Exception thrown when a service can not find the current user 
 */
public class UserNotValidException extends PosAppException {

	private static final long serialVersionUID = 1L;

	public UserNotValidException(String message) {
		super(message);
	}

	public UserNotValidException(String message, Throwable cause) {
		super(message, cause);
	}

}