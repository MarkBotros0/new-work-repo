package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.DashboardStatsDTO;
import it.deloitte.postrxade.exception.NotFoundRecordException;

/**
 * Service interface for handling dashboard-related business logic.
 * <p>
 * This service is responsible for aggregating high-level statistics for the
 * application's main overview dashboard, summarizing data from various sources
 * (Ingestions, Validations, Obligations).
 */
public interface OverviewDashboardService {

    /**
     * Retrieves dashboard statistics for a given period and fiscal year.
     *
     * @param period     The financial period (e.g., "January", "Q1").
     * @param fiscalYear The fiscal year (e.g., 2024).
     * @return A {@link DashboardStatsDTO} containing the dashboard statistics.
     * @throws NotFoundRecordException If the requested data cannot be found or is invalid.
     */
    DashboardStatsDTO getDashboardStats(String period, Integer fiscalYear) throws NotFoundRecordException;
}