package it.deloitte.postrxade.utils;

import it.deloitte.postrxade.entity.Log;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Publisher component for Audit Log events.
 * <p>
 * This class acts as the entry point for the audit logging system. Instead of writing
 * directly to the repository, services use this logger to publish events.
 * The actual persistence is handled asynchronously by the {@link AuditLogAsyncListener},
 * implementing the "Fire and Forget" pattern to ensure performance.
 */
@Component
@RequiredArgsConstructor
public class AuditLogger {

    // --- Standard Log Message Templates ---

    /**
     * Template for status change logs.
     * Args: [FirstName] [LastName] [SubmissionID] [ObligationPeriod] [ObligationYear] [OldStatus] [NewStatus]
     */
    public static final String STATUS_CHANGE = "%s %s moved submission with id: %s of obligation %s-%s from status: %s to %s";

    /**
     * Message for report download actions.
     */
    public static final String DOWNLOAD_REPORT = "Downloaded full Validation report";

    /**
     * Template for login logs.
     * Args: [FirstName] [LastName]
     */
    public static final String LOGIN_MESSAGE = "%s %s logged in";

    private final ApplicationEventPublisher publisher;

    /**
     * Publishes a Log entity to the application event bus.
     * <p>
     * This method returns immediately. The {@link AuditLogAsyncListener} will pick up
     * the event and handle the database persistence in a background thread.
     *
     * @param log The fully constructed Log entity to be saved.
     */
    public void save(Log log) {
        publisher.publishEvent(log);
    }
}