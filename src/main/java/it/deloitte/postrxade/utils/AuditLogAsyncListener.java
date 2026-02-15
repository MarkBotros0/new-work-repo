package it.deloitte.postrxade.utils;

import it.deloitte.postrxade.entity.Log;
import it.deloitte.postrxade.repository.LogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Asynchronous event listener for persisting Audit Logs.
 * <p>
 * This component listens for {@link Log} events published via the Spring ApplicationContext.
 * Using {@link Async}, it processes the save operation in a separate thread, ensuring that
 * logging latency or failures do not impact the performance or success of the main business transaction.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class AuditLogAsyncListener {

    private final LogRepository logRepository;

    /**
     * Handles the {@link Log} event and persists it to the database.
     * <p>
     * This method is triggered automatically whenever a {@link Log} object is published
     * using {@code applicationEventPublisher.publishEvent(log)}.
     *
     * @param logEntity The log entity to save.
     */
    @Async
    @EventListener
    @Transactional
    public void saveLog(Log logEntity) {
        try {
            // Ensure timestamp is set if the caller didn't set it
            if (logEntity.getTimestamp() == null) {
                logEntity.setTimestamp(Instant.now());
            }

            logRepository.save(logEntity);
            log.debug("Audit log saved successfully: {}", logEntity);

        } catch (Exception e) {
            // Log the error but do not rethrow, as this is an async background task
            log.error("FAILED TO SAVE LOG: {}", e.getMessage(), e);
        }
    }
}