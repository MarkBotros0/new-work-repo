package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.FilterOptionsDTO;
import it.deloitte.postrxade.service.FilterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST controller for serving filter-related metadata.
 * <p>
 * This controller provides endpoints to retrieve configuration options used for
 * filtering data in the frontend (e.g., available Fiscal Years, Periods).
 */
@RestController
@CrossOrigin(origins = "http://localhost:8080")
@RequestMapping("/api/filters")
@Tag(name = "Filter Service", description = "Endpoints for retrieving UI Filter Options")
public class FilterController {

    private final FilterService filterService;

    /**
     * Constructor-based dependency injection.
     *
     * @param filterService The service responsible for aggregating filter options.
     */
    public FilterController(FilterService filterService) {
        this.filterService = filterService;
    }

    /**
     * Retrieves all available filter options (Years, Periods, etc.).
     * <p>
     * Endpoint: GET /api/filters/options
     * <p>
     * Used by the frontend to populate dropdowns and search filters.
     *
     * @return A {@link ResponseEntity} containing the {@link FilterOptionsDTO} with all available options.
     */
    @GetMapping("/options")
    @Operation(summary = "Get filter options", description = "Retrieve all available filter options (Fiscal Years, Periods) for UI dropdowns")
    public ResponseEntity<FilterOptionsDTO> getFilterOptions() {
        FilterOptionsDTO options = filterService.getFilterOptions();
        return ResponseEntity.ok(options);
    }
}