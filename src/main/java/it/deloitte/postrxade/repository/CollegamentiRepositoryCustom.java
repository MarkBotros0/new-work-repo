package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.Collegamenti;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.records.MerchantOutputData;

public interface CollegamentiRepositoryCustom {
    void bulkInsert(List<Collegamenti> collegamenti, Submission submission);

    Map<String, Integer> checkExisting(List<Collegamenti> collegamenti, Submission submission);
    
    /**
     * Check if Collegamenti parent exists for Soggetti records (validates ndg FK).
     * Returns map with key="ndg_submissionId" and value=1 if Collegamenti exists, 0 if missing.
     */
    Map<String, Integer> checkCollegamentiForSoggetti(List<it.deloitte.postrxade.entity.Soggetti> soggettiList, Submission submission);
    
    /**
     * Check if Collegamenti parent exists for Rapporti records (validates chiave_rapporto FK).
     * Returns map with key="chiave_rapporto_submissionId" and value=1 if Collegamenti exists, 0 if missing.
     */
    Map<String, Integer> checkCollegamentiForRapporti(List<it.deloitte.postrxade.entity.Rapporti> rapportiList, Submission submission);
    
    /**
     * Check if Collegamenti parent exists for DatiContabili records (validates chiave_rapporto FK).
     * Returns map with key="chiave_rapporto_submissionId" and value=1 if Collegamenti exists, 0 if missing.
     */
    Map<String, Integer> checkCollegamentiForDatiContabili(List<it.deloitte.postrxade.entity.DatiContabili> datiContabiliList, Submission submission);

    List<Collegamenti> findByOutputId(Long outputId, int offset, int limit);

    List<MerchantOutputData> findCollegamentiDetailsBySubmissionId(Long submissionId, int limit);

    List<Long> findCollegamentiIdsBySubmissionIdAndNullOutput(Long submissionId, int rowsPerPage);

    /**
     * Fetch Collegamenti with all children (Rapporti, Soggetti, DatiContabili) using JOINs.
     * This method is optimized for output generation to avoid N+1 queries.
     * 
     * @param submissionId The submission ID
     * @param outputId The output ID to filter by
     * @param limit Maximum number of records to fetch
     * @return List of Collegamenti with populated children
     */
    List<Collegamenti> findCollegamentiWithChildrenByOutputId(Long submissionId, Long outputId, int limit);

    /**
     * Fetch NDG and chiave_rapporto for given Collegamenti IDs.
     * Returns List<Object[]> where each array contains [ndg, chiave_rapporto].
     */
    List<Object[]> findNdgAndChiaviByIds(List<Long> collegamentiIds);

    /**
     * Find orphan Collegamenti records that don't have ANY children (missing BOTH Soggetti AND Rapporti).
     * Returns a list of Object[] where:
     * - [0] = pk_collegamenti (Long)
     * - [1] = ndg (String)
     * - [2] = chiave_rapporto (String)
     * - [3] = has_soggetti (Integer: 1 if exists, 0 if missing)
     * - [4] = has_rapporti (Integer: 1 if exists, 0 if missing)
     * - [5] = raw_row (String) - reconstructed from Collegamenti fields
     * 
     * @param submissionId The submission ID to check
     * @return List of orphan Collegamenti with metadata about missing children
     */
    List<Object[]> findOrphanCollegamenti(Long submissionId);

    /**
     * Find Soggetti records whose Collegamenti parent is missing BOTH children (will be deleted).
     * These Soggetti will become orphans when their Collegamenti parent is deleted.
     * 
     * @param submissionId The submission ID to check
     * @return List of Object[] with [pk_soggetti, ndg, raw_row]
     */
    List<Object[]> findSoggettiToOrphan(Long submissionId);

    /**
     * Find Rapporti records whose Collegamenti parent is missing BOTH children (will be deleted).
     * These Rapporti will become orphans when their Collegamenti parent is deleted.
     * 
     * @param submissionId The submission ID to check
     * @return List of Object[] with [pk_rapporti, chiave_rapporto, raw_row]
     */
    List<Object[]> findRapportiToOrphan(Long submissionId);
}
