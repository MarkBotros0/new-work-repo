package it.deloitte.postrxade.web.rest;

import it.deloitte.postrxade.dto.IngestionDTO;
import it.deloitte.postrxade.service.IngestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * REST controller for managing Ingestion operations.
 */
@RestController
@RequestMapping("/api/ingestions")
@Tag(name = "Ingestion", description = "API for managing ingestion operations")
public class IngestionController {

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    /**
     * GET /api/ingestions : Get all ingestions ordered by ingestion date (most recent first).
     *
     * @return ResponseEntity with list of ingestions
     */
    @GetMapping
    @Operation(
        summary = "Get all ingestions",
        description = "Retrieve all ingestions ordered by ingestion date (most recent first)"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestions"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<IngestionDTO>> getAllIngestions() {
        List<IngestionDTO> ingestions = ingestionService.getAllIngestionsOrderByDate();
        return ResponseEntity.ok(ingestions);
    }

    /**
     * GET /api/ingestions/paginated : Get all ingestions with pagination.
     *
     * @param pageable pagination information
     * @return ResponseEntity with page of ingestions
     */
    @GetMapping("/paginated")
    @Operation(
        summary = "Get all ingestions with pagination",
        description = "Retrieve all ingestions with pagination support"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved paginated ingestions"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Page<IngestionDTO>> getAllIngestionsPaginated(
            @Parameter(description = "Pagination information") Pageable pageable) {
        Page<IngestionDTO> ingestions = ingestionService.getAllIngestions(pageable);
        return ResponseEntity.ok(ingestions);
    }

    /**
     * GET /api/ingestions/asc : Get all ingestions ordered by ingestion date (oldest first).
     *
     * @return ResponseEntity with list of ingestions
     */
    @GetMapping("/asc")
    @Operation(
        summary = "Get all ingestions (ascending order)",
        description = "Retrieve all ingestions ordered by ingestion date (oldest first)"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestions"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<IngestionDTO>> getAllIngestionsAsc() {
        List<IngestionDTO> ingestions = ingestionService.getAllIngestionsOrderByDateAsc();
        return ResponseEntity.ok(ingestions);
    }

    /**
     * GET /api/ingestions/{id} : Get ingestion by ID.
     *
     * @param id the ingestion ID
     * @return ResponseEntity with ingestion DTO
     */
    @GetMapping("/{id}")
    @Operation(
        summary = "Get ingestion by ID",
        description = "Retrieve a specific ingestion by its ID"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestion"),
        @ApiResponse(responseCode = "404", description = "Ingestion not found"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<IngestionDTO> getIngestionById(
            @Parameter(description = "Ingestion ID") @PathVariable Long id) {
        Optional<IngestionDTO> ingestion = ingestionService.getIngestionById(id);
        return ingestion.map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * GET /api/ingestions/submission/{submissionId} : Get ingestions by submission ID.
     *
     * @param submissionId the submission ID
     * @return ResponseEntity with list of ingestions
     */
    @GetMapping("/submission/{submissionId}")
    @Operation(
        summary = "Get ingestions by submission ID",
        description = "Retrieve all ingestions for a specific submission"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestions"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<IngestionDTO>> getIngestionsBySubmissionId(
            @Parameter(description = "Submission ID") @PathVariable Long submissionId) {
        List<IngestionDTO> ingestions = ingestionService.getIngestionsBySubmissionId(submissionId);
        return ResponseEntity.ok(ingestions);
    }

    /**
     * GET /api/ingestions/type/{typeId} : Get ingestions by type.
     *
     * @param typeId the ingestion type ID
     * @return ResponseEntity with list of ingestions
     */
    @GetMapping("/type/{typeId}")
    @Operation(
        summary = "Get ingestions by type",
        description = "Retrieve all ingestions for a specific ingestion type"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestions"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<IngestionDTO>> getIngestionsByType(
            @Parameter(description = "Ingestion Type ID") @PathVariable Long typeId) {
        List<IngestionDTO> ingestions = ingestionService.getIngestionsByType(typeId);
        return ResponseEntity.ok(ingestions);
    }

    /**
     * GET /api/ingestions/status/{statusId} : Get ingestions by status.
     *
     * @param statusId the ingestion status ID
     * @return ResponseEntity with list of ingestions
     */
    @GetMapping("/status/{statusId}")
    @Operation(
        summary = "Get ingestions by status",
        description = "Retrieve all ingestions for a specific ingestion status"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestions"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<IngestionDTO>> getIngestionsByStatus(
            @Parameter(description = "Ingestion Status ID") @PathVariable Long statusId) {
        List<IngestionDTO> ingestions = ingestionService.getIngestionsByStatus(statusId);
        return ResponseEntity.ok(ingestions);
    }

    /**
     * GET /api/ingestions/date-range : Get ingestions by date range.
     *
     * @param startDate the start date
     * @param endDate the end date
     * @return ResponseEntity with list of ingestions
     */
    @GetMapping("/date-range")
    @Operation(
        summary = "Get ingestions by date range",
        description = "Retrieve all ingestions within a specific date range"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully retrieved ingestions"),
        @ApiResponse(responseCode = "400", description = "Invalid date range"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<List<IngestionDTO>> getIngestionsByDateRange(
            @Parameter(description = "Start date") @RequestParam LocalDateTime startDate,
            @Parameter(description = "End date") @RequestParam LocalDateTime endDate) {
        List<IngestionDTO> ingestions = ingestionService.getIngestionsByDateRange(startDate, endDate);
        return ResponseEntity.ok(ingestions);
    }


    /**
     * POST /api/ingestions : Create a new ingestion.
     *
     * @param ingestionDTO the ingestion DTO
     * @return ResponseEntity with created ingestion DTO
     */
    @PostMapping
    @Operation(
        summary = "Create new ingestion",
        description = "Create a new ingestion"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Successfully created ingestion"),
        @ApiResponse(responseCode = "400", description = "Invalid ingestion data"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<IngestionDTO> createIngestion(
            @Parameter(description = "Ingestion data") @RequestBody IngestionDTO ingestionDTO) {
        IngestionDTO createdIngestion = ingestionService.createIngestion(ingestionDTO);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdIngestion);
    }

    /**
     * PUT /api/ingestions/{id} : Update an existing ingestion.
     *
     * @param id the ingestion ID
     * @param ingestionDTO the updated ingestion DTO
     * @return ResponseEntity with updated ingestion DTO
     */
    @PutMapping("/{id}")
    @Operation(
        summary = "Update ingestion",
        description = "Update an existing ingestion"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "Successfully updated ingestion"),
        @ApiResponse(responseCode = "404", description = "Ingestion not found"),
        @ApiResponse(responseCode = "400", description = "Invalid ingestion data"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<IngestionDTO> updateIngestion(
            @Parameter(description = "Ingestion ID") @PathVariable Long id,
            @Parameter(description = "Updated ingestion data") @RequestBody IngestionDTO ingestionDTO) {
        IngestionDTO updatedIngestion = ingestionService.updateIngestion(id, ingestionDTO);
        return ResponseEntity.ok(updatedIngestion);
    }

    /**
     * DELETE /api/ingestions/{id} : Delete an ingestion by ID.
     *
     * @param id the ingestion ID
     * @return ResponseEntity with no content
     */
    @DeleteMapping("/{id}")
    @Operation(
        summary = "Delete ingestion",
        description = "Delete an ingestion by its ID"
    )
    @ApiResponses(value = {
        @ApiResponse(responseCode = "204", description = "Successfully deleted ingestion"),
        @ApiResponse(responseCode = "404", description = "Ingestion not found"),
        @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    public ResponseEntity<Void> deleteIngestion(
            @Parameter(description = "Ingestion ID") @PathVariable Long id) {
        ingestionService.deleteIngestion(id);
        return ResponseEntity.noContent().build();
    }
}
