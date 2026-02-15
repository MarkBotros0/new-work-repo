package it.deloitte.postrxade.exception;

/**
 * Generic custom Exception associated to the project
 */
public class PosAppException extends Exception {

	private static final long serialVersionUID = 1L;

	public PosAppException(String message) {
		super(message);
	}

	public PosAppException(Throwable cause) {
		super(cause);
	}

	public PosAppException(String message, Throwable cause) {
		super(message, cause);
	}

}
