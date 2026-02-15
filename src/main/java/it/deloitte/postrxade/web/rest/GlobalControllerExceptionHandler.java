package it.deloitte.postrxade.web.rest;

import it.deloitte.postrxade.exception.*;
import it.deloitte.postrxade.utils.RestBodyResponseUtil;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.HandlerMapping;

/**
 * Global Exception Handler for the REST API.
 * <p>
 * This class intercepts exceptions thrown by any controller across the application
 * and transforms them into a standard JSON response structure defined by {@link RestBodyResponseUtil}.
 * <p>
 * It ensures that clients always receive a consistent error format:
 * <code>{ "timestamp": "...", "status": 4xx/5xx, "error": "Title", "message": "Detail", "path": "/api/..." }</code>
 */
@RestControllerAdvice
public class GlobalControllerExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalControllerExceptionHandler.class);
    private static final String LOGGER_MSG_BEGIN = "Inizio [hashcode={}]";
    private static final String LOGGER_MSG_END = "Fine";
    private static final String LOGGER_MSG_EXCEPTION = "Handling [Exception:class={}, message={}]";
    private static final String LOGGER_MSG_EXCEPTION_NULL = "Exception Message is null";

    /**
     * Handles exceptions related to invalid requests or business rule violations.
     * Maps to HTTP 400 (Bad Request).
     *
     * @param request The original HTTP request.
     * @param ex      The intercepted exception (e.g., {@link PosAppException}, {@link ResourceException}).
     * @return A {@link ResponseEntity} containing the standardized error details.
     */
    @ExceptionHandler({PosAppException.class, ResourceException.class, PosAppRuntimeException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<RestBodyResponseUtil> handleBadRequest(HttpServletRequest request, Exception ex) {

        LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
        String message = ex.getMessage() != null ? ex.getMessage() : LOGGER_MSG_EXCEPTION_NULL;
        LOGGER.debug(LOGGER_MSG_EXCEPTION, ex.getClass(), message);

        RestBodyResponseUtil response = new RestBodyResponseUtil(HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                message,
                (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE));

        LOGGER.debug(LOGGER_MSG_END);

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
    }

    /**
     * Handles security-related exceptions where the user is not authenticated or authorized.
     * Maps to HTTP 401 (Unauthorized).
     *
     * @param request The original HTTP request.
     * @param ex      The security exception (e.g., {@link AuthorityCodeNotValidException}).
     * @return A {@link ResponseEntity} with status 401.
     */
    @ExceptionHandler({AuthorityCodeNotValidException.class, InsufficientAuthenticationException.class})
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    public ResponseEntity<RestBodyResponseUtil> handleUnauthorized(HttpServletRequest request, Exception ex) {

        LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
        String message = ex.getMessage() != null ? ex.getMessage() : LOGGER_MSG_EXCEPTION_NULL;
        LOGGER.debug(LOGGER_MSG_EXCEPTION, ex.getClass(), message);

        RestBodyResponseUtil response = new RestBodyResponseUtil(HttpStatus.UNAUTHORIZED.value(),
                HttpStatus.UNAUTHORIZED.getReasonPhrase(),
                message,
                (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE));

        LOGGER.debug(LOGGER_MSG_END);

        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
    }

    /**
     * Handles exceptions where a requested resource cannot be found.
     * Maps to HTTP 404 (Not Found).
     *
     * @param request The original HTTP request.
     * @param ex      The exception indicating missing data (e.g., {@link NotFoundRecordException}).
     * @return A {@link ResponseEntity} with status 404.
     */
    @ExceptionHandler(NotFoundRecordException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<RestBodyResponseUtil> hadleNotFound(HttpServletRequest request, Exception ex) {

        LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
        String message = ex.getMessage() != null ? ex.getMessage() : LOGGER_MSG_EXCEPTION_NULL;
        LOGGER.debug(LOGGER_MSG_EXCEPTION, ex.getClass(), message);

        RestBodyResponseUtil response = new RestBodyResponseUtil(HttpStatus.NOT_FOUND.value(),
                HttpStatus.NOT_FOUND.getReasonPhrase(),
                message,
                (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE));

        LOGGER.debug(LOGGER_MSG_END);

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }

    /**
     * Handles server-side errors where the operation could not be completed due to invalid state.
     * Maps to HTTP 500 (Internal Server Error).
     *
     * @param request The original HTTP request.
     * @param ex      The exception (e.g., {@link UserNotValidException}).
     * @return A {@link ResponseEntity} with status 500.
     */
    @ExceptionHandler(UserNotValidException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<RestBodyResponseUtil> handleIllegalStateException(HttpServletRequest request, Exception ex) {

        LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
        String message = ex.getMessage() != null ? ex.getMessage() : LOGGER_MSG_EXCEPTION_NULL;
        LOGGER.debug(LOGGER_MSG_EXCEPTION, ex.getClass(), message);

        RestBodyResponseUtil response = new RestBodyResponseUtil(HttpStatus.INTERNAL_SERVER_ERROR.value(),
                HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                message,
                (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE));

        LOGGER.debug(LOGGER_MSG_END);

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }

    /**
     * Fallback handler for all other unhandled exceptions.
     * <p>
     * <strong>Security Note:</strong> This method maps generic exceptions to HTTP 404 (Not Found)
     * instead of 500. This pattern is often used to prevent "Server Error" leakage which might
     * expose stack traces or internal logic to attackers (Security through Obscurity).
     *
     * @param request The original HTTP request.
     * @param ex      The generic unhandled exception.
     * @return A generic "An error has occurred" message with status 404.
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ResponseEntity<RestBodyResponseUtil> noHandlerFound(HttpServletRequest request, Exception ex) {

        LOGGER.debug(LOGGER_MSG_BEGIN, this.hashCode());
        String message = ex.getMessage() != null ? ex.getMessage() : LOGGER_MSG_EXCEPTION_NULL;
        LOGGER.debug(LOGGER_MSG_EXCEPTION, ex.getClass(), message);

        RestBodyResponseUtil response = new RestBodyResponseUtil(HttpStatus.NOT_FOUND.value(),
                HttpStatus.NOT_FOUND.getReasonPhrase(),
                "An error has occurred.",
                (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE));

        LOGGER.debug("Fine");

        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
    }

}
