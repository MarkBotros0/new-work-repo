package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.LogDTO;
import it.deloitte.postrxade.dto.AuditLogsSearchDTO;
import org.springframework.data.domain.Page;

/**
 * Service interface for business logic related to audit trail logs.
 * <p>
 * This service is responsible for retrieving system logs, allowing administrators
 * to track user actions and system events through pagination and filtering.
 */
public interface AuditTrailService {

    /**
     * Retrieves a paginated list of audit trail logs based on search criteria.
     *
     * @param auditLogsSearchDTO The DTO containing pagination, sorting, and filter parameters.
     * @return A {@link Page} of {@link LogDTO} objects representing the audit logs.
     */
    Page<LogDTO> getAuditLogs(AuditLogsSearchDTO auditLogsSearchDTO);
}