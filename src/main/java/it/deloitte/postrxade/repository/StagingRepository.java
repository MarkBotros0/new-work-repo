package it.deloitte.postrxade.repository;

import java.util.List;

import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.records.StagingResult;

/**
 * Repository for staging table operations.
 * Used for high-performance ETL ingestion with set-based operations.
 */
public interface StagingRepository {

    /**
     * Clear all staging data for a submission.
     */
    void clearStaging(Long submissionId);

    /**
     * Clear only transaction staging data for a submission.
     * Used for incremental cleanup between multiple transaction files.
     */
//    void clearTransactionStaging(Long submissionId);

    /**
     * Bulk load merchants into staging table.
     * No duplicate checks during load - handled later by set-based operations.
     *
     * @param soggettiList list of merchants to load
     * @param ingestionId the ingestion ID
     * @param submissionId the submission ID
     */
    void bulkLoadSoggettiToStaging(List<Soggetti> soggettiList, Long ingestionId, Long submissionId);

    /**
     * Bulk load rapporti into staging table.
     * No duplicate checks during load - handled later by set-based operations.
     *
     * @param rapportiList list of rapporti to load
     * @param ingestionId the ingestion ID
     * @param submissionId the submission ID
     */
    void bulkLoadRapportiToStaging(List<it.deloitte.postrxade.entity.Rapporti> rapportiList, Long ingestionId, Long submissionId);

    /**
     * Bulk load dati contabili into staging table.
     * No duplicate checks during load - handled later by set-based operations.
     *
     * @param datiContabiliList list of dati contabili to load
     * @param ingestionId the ingestion ID
     * @param submissionId the submission ID
     */
    void bulkLoadDatiContabiliToStaging(List<it.deloitte.postrxade.entity.DatiContabili> datiContabiliList, Long ingestionId, Long submissionId);

    /**
     * Bulk load collegamenti into staging table.
     * No duplicate checks during load - handled later by set-based operations.
     *
     * @param collegamentiList list of collegamenti to load
     * @param ingestionId the ingestion ID
     * @param submissionId the submission ID
     */
    void bulkLoadCollegamentiToStaging(List<it.deloitte.postrxade.entity.Collegamenti> collegamentiList, Long ingestionId, Long submissionId);

    /**
     * Bulk load cambio ndg into staging table.
     * No duplicate checks during load - handled later by set-based operations.
     *
     * @param cambioNdgList list of cambio ndg to load
     * @param ingestionId the ingestion ID
     * @param submissionId the submission ID
     */
    void bulkLoadCambioNdgToStaging(List<it.deloitte.postrxade.entity.CambioNdg> cambioNdgList, Long ingestionId, Long submissionId);

//    /**
//     * Bulk load transactions into staging table.
//     * No duplicate checks during load - handled later by set-based operations.
//     *
//     * @param transactions list of transactions to load
//     * @param ingestionId the ingestion ID
//     * @param submissionId the submission ID
//     */
//    void bulkLoadTransactionsToStaging(List<Transaction> transactions, Long ingestionId, Long submissionId);

//    /**
//     * Process merchants from staging to final table using set-based operations.
//     * Identifies duplicates and inserts new records.
//     *
//     * @param submissionId the submission ID
//     * @return result with counts of inserted, duplicates, and errors
//     */
//    StagingResult processMerchantsFromStaging(Long submissionId);

    /**
     * Process transactions from staging to final table using set-based operations.
     * Identifies duplicates, missing merchants, and inserts new records.
     *
     * @param submissionId the submission ID
     * @return result with counts of inserted, duplicates, missing merchants, and errors
     */
    StagingResult processSoggettiFromStaging(Long submissionId);
    /**
     * Process transactions from staging to final table using set-based operations.
     * Identifies duplicates, missing merchants, and inserts new records.
     *
     * @param submissionId the submission ID
     * @return result with counts of inserted, duplicates, missing merchants, and errors
     */
    StagingResult processRapportiFromStaging(Long submissionId);
    /**
     * Process transactions from staging to final table using set-based operations.
     * Identifies duplicates, missing merchants, and inserts new records.
     *
     * @param submissionId the submission ID
     * @return result with counts of inserted, duplicates, missing merchants, and errors
     */
    StagingResult processDatiContabiliFromStaging(Long submissionId);
    /**
     * Process transactions from staging to final table using set-based operations.
     * Identifies duplicates, missing merchants, and inserts new records.
     *
     * @param submissionId the submission ID
     * @return result with counts of inserted, duplicates, missing merchants, and errors
     */
    StagingResult processCollegamentiFromStaging(Long submissionId);
    /**
     * Process transactions from staging to final table using set-based operations.
     * Identifies duplicates, missing merchants, and inserts new records.
     *
     * @param submissionId the submission ID
     * @return result with counts of inserted, duplicates, missing merchants, and errors
     */
    StagingResult processCambioNdgFromStaging(Long submissionId);

//    /**
//     * Get raw rows of merchants that were marked as duplicates.
//     *
//     * @param submissionId the submission ID
//     * @return list of raw row strings
//     */
//    List<String> getDuplicateMerchantRawRows(Long submissionId);

//    /**
//     * Get raw rows of transactions that were marked as duplicates.
//     *
//     * @param submissionId the submission ID
//     * @return list of raw row strings
//     */
//    List<String> getDuplicateTransactionRawRows(Long submissionId);

//    /**
//     * Get raw rows of transactions with missing merchants.
//     *
//     * @param submissionId the submission ID
//     * @return list of raw row strings
//     */
//    List<String> getMissingMerchantTransactionRawRows(Long submissionId);

    /**
     * Get detailed info for duplicate merchants (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getDuplicateSoggettiDetails(Long submissionId);

    /**
     * Get detailed info for duplicate merchants (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getDuplicateRapportiDetails(Long submissionId);

    /**
     * Get detailed info for duplicate merchants (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getDuplicateDatiContabiliDetails(Long submissionId);

    /**
     * Get detailed info for duplicate merchants (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getDuplicateCollegamentiDetails(Long submissionId);

    /**
     * Get detailed info for collegamenti with missing reference (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getMissingReferenceCollegamentiDetails(Long submissionId);

    /**
     * Get detailed info for soggetti with missing Collegamenti parent (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getMissingSoggettiCollegamentiDetails(Long submissionId);

    /**
     * Get detailed info for rapporti with missing Collegamenti parent (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getMissingRapportiCollegamentiDetails(Long submissionId);

    /**
     * Get detailed info for dati contabili with missing Collegamenti parent (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getMissingDatiContabiliCollegamentiDetails(Long submissionId);

    /**
     * Get detailed info for duplicate merchants (for ErrorRecord creation).
     * Returns: raw_row, error_message
     *
     * @param submissionId the submission ID
     * @return list of Object[] with [raw_row, error_message]
     */
    List<Object[]> getDuplicateCambioNdgDetails(Long submissionId);

//    /**
//     * Get detailed info for duplicate transactions (for ErrorRecord creation).
//     * Returns: raw_row, error_message
//     *
//     * @param submissionId the submission ID
//     * @return list of Object[] with [raw_row, error_message]
//     */
//    List<Object[]> getDuplicateTransactionDetails(Long submissionId);

//    /**
//     * Get detailed info for transactions with missing merchants (for ErrorRecord creation).
//     * Returns: raw_row, error_message
//     *
//     * @param submissionId the submission ID
//     * @return list of Object[] with [raw_row, error_message]
//     */
//    List<Object[]> getMissingMerchantTransactionDetails(Long submissionId);
}
