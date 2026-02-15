package it.deloitte.postrxade.exception;

/**
 * Generic runtime custom Exception associated to the project
 */
public class PosAppRuntimeException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public PosAppRuntimeException(String message) {
		super(message);
	}

	public PosAppRuntimeException(Throwable cause) {
		super(cause);
	}

	public PosAppRuntimeException(final String message, final Throwable cause) {
		super(message, cause);
	}

}
