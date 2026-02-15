package it.deloitte.postrxade.exception;

/**
 * Eccezione relativa ad una resource.
 */
public class ResourceNotFoundException
extends PosAppRuntimeException {

	private static final long serialVersionUID = 1L;

	public ResourceNotFoundException(String message) {
		super(message);
	}

	public ResourceNotFoundException(Throwable cause) {
		super(cause);
	}

	public ResourceNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

}
