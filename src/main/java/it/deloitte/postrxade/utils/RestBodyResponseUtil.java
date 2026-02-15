package it.deloitte.postrxade.utils;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.Objects;

/**
 * Utility class that defines the standard JSON response body for API responses.
 * <p>
 * This class is typically used to wrap error details in a consistent format across
 * the application (e.g., inside a {@code @ControllerAdvice}), ensuring that clients
 * always receive the timestamp, HTTP status, error message, and the specific path
 * where the error occurred.
 */
@Setter
@Getter
public class RestBodyResponseUtil {

    private Instant timestamp;
    private Integer status;
    private String error;
    private String message;
    private String path;

    /**
     * Constructs a new response object with the current timestamp.
     *
     * @param status  The HTTP status code (e.g., 404, 500).
     * @param error   The short error title (e.g., "Not Found").
     * @param message The detailed error description.
     * @param path    The URI path where the event occurred.
     */
    public RestBodyResponseUtil(Integer status, String error, String message, String path) {
        this.timestamp = Instant.now();
        this.status = status;
        this.error = error;
        this.message = message;
        this.path = path;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, status, error, message, path);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof RestBodyResponseUtil restBodyResponseUtil)) {
            return false;
        }
        return Objects.equals(timestamp, restBodyResponseUtil.timestamp)
                && Objects.equals(status, restBodyResponseUtil.status)
                && Objects.equals(error, restBodyResponseUtil.error)
                && Objects.equals(message, restBodyResponseUtil.message)
                && Objects.equals(path, restBodyResponseUtil.path);
    }

    @Override
    public String toString() {
        return "{" +
                " timestamp='" + getTimestamp() + "'" +
                ", status='" + getStatus() + "'" +
                ", error='" + getError() + "'" +
                ", message='" + getMessage() + "'" +
                ", path='" + getPath() + "'" +
                "}";
    }
}
