package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.IngestionBatchDTO;
import it.deloitte.postrxade.dto.IngestionStatusDTO;

import java.util.List;

/**
 * Service interface for business logic related to ingestion batches.
 * <p>
 * This service is responsible for aggregating data for the Ingestion Dashboard,
 * focusing on the technical status of file uploads (Batches) and their outcomes.
 */
public interface IngestionDashboardService {

    /**
     * Retrieves a list of all ingestion batch records.
     * <p>
     * This method aggregates submission data to show a high-level view of
     * file uploads, including transaction counts, error counts, and overall status.
     *
     * @return A list of {@link IngestionBatchDTO} objects representing the dashboard rows.
     */
    List<IngestionBatchDTO> getIngestionBatches();

    /**
     * Retrieves all available ingestion statuses.
     * <p>
     * Used for populating UI filters (e.g., "Success", "Failed", "Processing").
     *
     * @return A list of {@link IngestionStatusDTO} objects.
     */
    List<IngestionStatusDTO> getIngestionStatuses();
}