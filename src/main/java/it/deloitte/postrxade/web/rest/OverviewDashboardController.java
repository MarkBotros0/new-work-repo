package it.deloitte.postrxade.web.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import it.deloitte.postrxade.dto.DashboardStatsDTO;
import it.deloitte.postrxade.enums.AuthIdProfilo;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import it.deloitte.postrxade.security.RequireAuthorities;
import it.deloitte.postrxade.service.OverviewDashboardService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST controller for serving dashboard-related data.
 * <p>
 * This controller handles requests related to the high-level overview of the application,
 * providing aggregated statistics for specific Fiscal Years and Periods.
 */
@RestController
@CrossOrigin(origins = "http://localhost:8080")
@RequestMapping("/api/overview")
@Tag(name = "Dashboard Overview Service", description = "Endpoints for retrieving high-level dashboard statistics")
public class OverviewDashboardController {

    private final OverviewDashboardService dashboardService;

    /**
     * Constructor-based dependency injection for the DashboardService.
     *
     * @param dashboardService The service responsible for aggregating dashboard logic.
     */
    public OverviewDashboardController(OverviewDashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    /**
     * Retrieves dashboard statistics for a specific timeframe.
     * <p>
     * Endpoint: GET /api/overview/stats
     *
     * @param fiscalYear The fiscal year (e.g., 2023).
     * @param period     The specific period or month name (e.g., "January", "Q1").
     * @return A {@link ResponseEntity} containing the {@link DashboardStatsDTO} with the aggregated stats.
     * @throws NotFoundRecordException if data cannot be found or parameters are invalid.
     */
    @GetMapping("/stats")
    @Operation(summary = "Get dashboard statistics", description = "Retrieves aggregated statistics for the dashboard based on Fiscal Year and Period.")
    @RequireAuthorities({
            AuthIdProfilo.MANAGER,
            AuthIdProfilo.REVIEWER,
            AuthIdProfilo.APPROVER
    })
    public ResponseEntity<DashboardStatsDTO> getDashboardStatistics(
            @RequestParam Integer fiscalYear,
            @RequestParam String period) throws NotFoundRecordException {

        DashboardStatsDTO stats = dashboardService.getDashboardStats(period, fiscalYear);
        return ResponseEntity.ok(stats);
    }
}