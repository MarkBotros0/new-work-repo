package it.deloitte.postrxade.service;

import it.deloitte.postrxade.dto.*;
import it.deloitte.postrxade.entity.Obligation;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.exception.NotFoundRecordException;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Service interface for Ingestion and Obligation operations.
 * <p>
 * This service acts as the core retrieval mechanism for Obligation data.
 * It provides methods to fetch submissions, enforce validity rules (e.g., single active submission),
 * and helper methods for retrieving historical data for comparisons.
 */
public interface ObligationService {

    /**
     * Retrieves the list of submissions formatted as Custom DTOs for the UI Grid.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List of {@link SubmissionCustomDTO}.
     * @throws NotFoundRecordException If data is missing.
     */
    List<SubmissionCustomDTO> getAllObligationsByFyAndPeriod(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Retrieves the raw list of all submissions (Active + Cancelled/Rejected).
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List of {@link Submission} entities.
     * @throws NotFoundRecordException If data is missing.
     */
    List<Submission> getAllSubmissionByFyAndPeriod(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Retrieves submissions from a past period calculated by an offset.
     *
     * @param fiscalYear      The starting Fiscal Year.
     * @param period          The starting Period Name.
     * @param periodsToGoBack Number of periods to regress.
     * @return List of {@link Submission} entities from the past period.
     * @throws NotFoundRecordException If the past period or data is missing.
     */
    List<Submission> getSubmissionsFromPastPeriod(Integer fiscalYear, String period, int periodsToGoBack) throws NotFoundRecordException;

    /**
     * Validates that an Obligation has a valid number of active submissions (<= 1).
     *
     * @param obligation The obligation entity to check.
     * @throws NotFoundRecordException If the obligation has > 1 active submission (Data Corruption).
     */
    Submission checkIfValid(Obligation obligation) throws NotFoundRecordException;

    /**
     * Retrieves the single active submission for a specific period (Optional wrapper).
     * <p>
     * Used for statistical calculations where only the "Active" data matters.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return An {@link Optional} containing the active submission, or empty if none/invalid.
     * @throws NotFoundRecordException If general retrieval fails.
     */
    @Transactional(readOnly = true)
    Optional<Submission> getActiveSubmissionForStats(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Finds an Obligation by FY and period, and returns it *along with*
     * its associated submissions in a wrapper DTO.
     * <p>
     * Returns a wrapper with a null obligation if not found, rather than throwing an exception.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return {@link PeriodSubmissionData} wrapper.
     */
    PeriodSubmissionData getDataByFyAndPeriod(Integer fy, String period);

    /**
     * Finds a past Obligation based on offset, returning it in a wrapper DTO.
     *
     * @param fy              The Fiscal Year.
     * @param period          The Period Name.
     * @param periodsToGoBack Number of periods to regress.
     * @return {@link PeriodSubmissionData} wrapper.
     */
    PeriodSubmissionData getDataFromPastPeriod(Integer fy, String period, int periodsToGoBack);

    /**
     * Retrieves a List containing the single active submission for statistics.
     * <p>
     * Similar to {@link #getActiveSubmissionForStats(Integer, String)} but returns a List
     * for convenience in iterating.
     *
     * @param fy     The Fiscal Year.
     * @param period The Period Name.
     * @return List containing the active submission (or empty).
     * @throws NotFoundRecordException If retrieval fails.
     */
    @Transactional(readOnly = true)
    List<Submission> getSubmissionsForStats(Integer fy, String period) throws NotFoundRecordException;

    /**
     * Filters the submissions of an Obligation to find only the "Active" ones.
     *
     * @param obligation The obligation entity.
     * @return List of active submissions.
     */
    List<Submission> getActiveSubmissions(Obligation obligation);

//    void ingestObligationFiles() throws NotFoundRecordException, IOException;
    void ingestObligationFilesForMerchants() throws NotFoundRecordException, IOException;

    CompletableFuture<Void> ingestObligationFilesAsync() throws NotFoundRecordException, IOException;
}