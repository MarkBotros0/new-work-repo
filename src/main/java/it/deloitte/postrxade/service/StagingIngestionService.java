package it.deloitte.postrxade.service;

import java.io.IOException;
import java.util.List;

import it.deloitte.postrxade.entity.ErrorRecord;
import it.deloitte.postrxade.entity.Ingestion;
import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.Transaction;
import it.deloitte.postrxade.parser.transaction.RemoteFile;
import it.deloitte.postrxade.records.StagingResult;

/**
 * Service for high-performance file ingestion using staging tables.
 *
 * <h2>Architecture Overview</h2>
 * <p>
 * Instead of processing records row-by-row with individual duplicate checks,
 * this service uses a 3-phase ETL approach:
 * </p>
 * <ol>
 *   <li><b>Load Phase:</b> Bulk insert all parsed records into staging tables (no validation)</li>
 *   <li><b>Transform Phase:</b> Set-based operations to identify duplicates, missing references, and errors</li>
 *   <li><b>Extract Phase:</b> Single INSERT INTO ... SELECT to move valid records to final tables</li>
 * </ol>
 *
 * <h2>Performance Benefits</h2>
 * <ul>
 *   <li>Reduces N database roundtrips to 2-3 set-based operations</li>
 *   <li>Leverages database indexes for duplicate detection</li>
 *   <li>Minimizes connection usage and lock contention</li>
 *   <li>Scales efficiently for millions of records</li>
 * </ul>
 */
public interface StagingIngestionService {

    /**
     * Process a soggetti file using staging tables.
     *
     * @param file       the remote file to process
     * @param ingestion  the ingestion entity
     * @param submission the submission entity
     * @return result with counts of processed, inserted, duplicates, and errors
     * @throws IOException if file reading fails
     */
    StagingResult processSoggettiFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException;

    /**
     * Process a rapporti file using staging tables.
     *
     * @param file       the remote file to process
     * @param ingestion  the ingestion entity
     * @param submission the submission entity
     * @return result with counts of processed, inserted, duplicates, and errors
     * @throws IOException if file reading fails
     */
    StagingResult processRapportiFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException;

    /**
     * Process a daticontabili file using staging tables.
     *
     * @param file       the remote file to process
     * @param ingestion  the ingestion entity
     * @param submission the submission entity
     * @return result with counts of processed, inserted, duplicates, and errors
     * @throws IOException if file reading fails
     */
    StagingResult processDaticontabiliFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException;

    /**
     * Process a collegamenti file using staging tables.
     *
     * @param file       the remote file to process
     * @param ingestion  the ingestion entity
     * @param submission the submission entity
     * @return result with counts of processed, inserted, duplicates, and errors
     * @throws IOException if file reading fails
     */
    StagingResult processCollegamentiFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException;

    /**
     * Process a cambiondg file using staging tables.
     *
     * @param file       the remote file to process
     * @param ingestion  the ingestion entity
     * @param submission the submission entity
     * @return result with counts of processed, inserted, duplicates, and errors
     * @throws IOException if file reading fails
     */
    StagingResult processCambioNdgFile(RemoteFile file, Ingestion ingestion, Submission submission) throws IOException;

//    /**
//     * Process merchant records directly (already parsed and validated).
//     *
//     * @param soggettiList list of parsed merchant entities
//     * @param validationErrors list of error records from validation phase
//     * @param ingestion    the ingestion entity
//     * @param submission   the submission entity
//     * @return result with counts
//     */
//    StagingResult processSoggetti(List<Soggetti> soggettiList, List<ErrorRecord> validationErrors, Ingestion ingestion, Submission submission);

    /**
     * Clean up staging tables for a submission.
     *
     * @param submissionId the submission ID to clean up
     */
    void cleanupStaging(Long submissionId);

//    /**
//     * Clean up only transaction staging table for a submission.
//     * Used for incremental cleanup between multiple transaction files.
//     *
//     * @param submissionId the submission ID to clean up
//     */
//    void cleanupTransactionStaging(Long submissionId);
}
