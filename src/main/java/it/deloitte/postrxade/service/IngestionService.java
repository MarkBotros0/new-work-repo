package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.IngestionDTO;
import it.deloitte.postrxade.entity.Ingestion;
import it.deloitte.postrxade.entity.IngestionType;
import it.deloitte.postrxade.entity.Submission;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service interface for Ingestion operations.
 */
public interface IngestionService {

    /**
     * Get all ingestions ordered by ingestion date (most recent first).
     * @return List of all ingestions ordered by ingestedAt descending
     */
    List<IngestionDTO> getAllIngestionsOrderByDate();

    /**
     * Get all ingestions ordered by ingestion date (oldest first).
     * @return List of all ingestions ordered by ingestedAt ascending
     */
    List<IngestionDTO> getAllIngestionsOrderByDateAsc();

    /**
     * Get all ingestions with pagination.
     * @param pageable pagination information
     * @return Page of ingestions
     */
    Page<IngestionDTO> getAllIngestions(Pageable pageable);

    /**
     * Get ingestion by ID.
     * @param id the ingestion ID
     * @return Optional containing the ingestion DTO if found
     */
    Optional<IngestionDTO> getIngestionById(Long id);

    /**
     * Get ingestions by submission ID.
     * @param submissionId the submission ID
     * @return List of ingestions for the given submission
     */
    List<IngestionDTO> getIngestionsBySubmissionId(Long submissionId);

    /**
     * Get ingestions by ingestion type.
     * @param ingestionTypeId the ingestion type ID
     * @return List of ingestions for the given type
     */
    List<IngestionDTO> getIngestionsByType(Long ingestionTypeId);

    /**
     * Get ingestions by ingestion status.
     * @param ingestionStatusId the ingestion status ID
     * @return List of ingestions for the given status
     */
    List<IngestionDTO> getIngestionsByStatus(Long ingestionStatusId);

    /**
     * Get ingestions between two dates.
     * @param startDate the start date
     * @param endDate the end date
     * @return List of ingestions between the given dates
     */
    List<IngestionDTO> getIngestionsByDateRange(LocalDateTime startDate, LocalDateTime endDate);


    /**
     * Create a new ingestion.
     * @param ingestionDTO the ingestion DTO
     * @return the created ingestion DTO
     */
    IngestionDTO createIngestion(IngestionDTO ingestionDTO);

    /**
     * Update an existing ingestion.
     * @param id the ingestion ID
     * @param ingestionDTO the updated ingestion DTO
     * @return the updated ingestion DTO
     */
    IngestionDTO updateIngestion(Long id, IngestionDTO ingestionDTO);

    /**
     * Delete an ingestion by ID.
     * @param id the ingestion ID
     */
    void deleteIngestion(Long id);

    Ingestion createIngestionBySubmission(Submission submission, IngestionType ingestionType);

    void markAsSuccess(Ingestion ingestion);
}
