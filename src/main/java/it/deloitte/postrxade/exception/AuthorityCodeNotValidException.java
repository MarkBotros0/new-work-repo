package it.deloitte.postrxade.exception;

/**
 * Exception thrown when the authority code of a Legal Entity is not in a valid form
 */
public class AuthorityCodeNotValidException extends PosAppException {

	private static final long serialVersionUID = 1L;

	public AuthorityCodeNotValidException(String message) {
		super(message);
	}

	public AuthorityCodeNotValidException(String message, Throwable cause) {
		super(message, cause);
	}

}