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

    List<Collegamenti> findCollegamentiBySubmissionIdAndNullOutputBulkFetched(Long submissionId, Long lastId, int limit);
}
