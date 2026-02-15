package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.IngestionBatchDTO;
import it.deloitte.postrxade.dto.IngestionStatusDTO;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.security.RequireAuthorities;
import it.deloitte.postrxade.service.IngestionDashboardService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller for handling ingestion-related dashboard requests.
 * <p>
 * This controller provides endpoints to retrieve ingestion metadata,
 * specifically batch history and available ingestion statuses.
 */
@RestController
@CrossOrigin(origins = "http://localhost:8080")
@RequestMapping("/api/ingestion")
@Tag(name = "Ingestion Dashboard Service", description = "Endpoints for retrieving Ingestion Batch and Status data")
public class IngestionDashboardController {
    @Autowired
    private IngestionDashboardService ingestionDashboardService;

    /**
     * Retrieves a list of all ingestion batch records.
     * <p>
     * Endpoint: GET /api/ingestion/batches
     *
     * @return A {@link ResponseEntity} containing a list of {@link IngestionBatchDTO} and HTTP 200 OK.
     */
    @GetMapping("/batches")
    @Operation(summary = "Get ingestion batches", description = "Retrieve all ingestion batch records for the dashboard")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<List<IngestionBatchDTO>> getAllBatches() {
        List<IngestionBatchDTO> batches = ingestionDashboardService.getIngestionBatches();
        return ResponseEntity.ok(batches);
    }

    /**
     * Retrieves all available ingestion statuses.
     * <p>
     * Endpoint: GET /api/ingestion/statuses
     * <p>
     * Used for filtering or status displays in the UI.
     *
     * @return A {@link ResponseEntity} containing a list of {@link IngestionStatusDTO} and HTTP 200 OK.
     */
    @GetMapping("/statuses")
    @Operation(summary = "Get ingestion statuses", description = "Retrieve all available ingestion statuses")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<List<IngestionStatusDTO>> getAllStatuses() {
        List<IngestionStatusDTO> statuses = ingestionDashboardService.getIngestionStatuses();
        return ResponseEntity.ok(statuses);
    }
}
