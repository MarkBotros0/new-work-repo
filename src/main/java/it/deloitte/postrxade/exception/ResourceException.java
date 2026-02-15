package it.deloitte.postrxade.exception;

/**
 * Eccezione relativa ad una resource.
 */
public class ResourceException
extends PosAppRuntimeException {

	private static final long serialVersionUID = 1L;

	public ResourceException(String message) {
		super(message);
	}

	public ResourceException(Throwable cause) {
		super(cause);
	}

	public ResourceException(String message, Throwable cause) {
		super(message, cause);
	}

}
