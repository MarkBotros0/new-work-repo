package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.LogDTO;
import it.deloitte.postrxade.dto.AuditLogsSearchDTO;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.security.RequireAuthorities;
import it.deloitte.postrxade.service.AuditTrailService;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for handling audit trail log requests.
 * <p>
 * This controller provides endpoints to search and retrieve system logs,
 * allowing administrators to track user actions and system events.
 */
@RestController
@RequestMapping("/api/audit")
@CrossOrigin("*")
@Tag(name = "Audit Trail Service", description = "Endpoints for searching and retrieving Audit Logs")
public class AuditTrailController {

    private final AuditTrailService auditTrailService;

    /**
     * Constructor-based dependency injection.
     *
     * @param auditTrailService The service responsible for retrieving audit log data.
     */
    public AuditTrailController(AuditTrailService auditTrailService) {
        this.auditTrailService = auditTrailService;
    }

    /**
     * Retrieves a paginated list of audit logs based on search criteria.
     * <p>
     * Endpoint: POST /api/audit/logs
     * <p>
     * This endpoint uses a POST request to support complex filtering criteria
     * passed in the request body (DTO).
     *
     * @param auditLogsSearchDTO The DTO containing filter parameters (date range, user, action type, etc.).
     * @return A {@link ResponseEntity} containing a {@link Page} of {@link LogDTO} objects.
     */
    @PostMapping("/logs")
    @Operation(summary = "Search audit logs", description = "Retrieve paginated and filtered audit logs based on search criteria")
    @RequireAuthorities({AuthIdProfilo.AUDITOR})
    public ResponseEntity<Page<LogDTO>> getAuditLogs(
            @RequestBody AuditLogsSearchDTO auditLogsSearchDTO) {

        Page<LogDTO> page = auditTrailService.getAuditLogs(auditLogsSearchDTO);
        return ResponseEntity.ok(page);
    }
}
