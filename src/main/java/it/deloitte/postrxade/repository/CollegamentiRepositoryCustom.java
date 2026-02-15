package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.Collegamenti;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.records.MerchantOutputData;

public interface CollegamentiRepositoryCustom {
    void bulkInsert(List<Collegamenti> collegamenti, Submission submission);

    Map<String, Integer> checkExisting(List<Collegamenti> collegamenti, Submission submission);

    List<Collegamenti> findByOutputId(Long outputId, int offset, int limit);

    List<MerchantOutputData> findCollegamentiDetailsBySubmissionId(Long submissionId, int limit);

    List<Long> findCollegamentiIdsBySubmissionIdAndNullOutput(Long submissionId, int rowsPerPage);

    List<Collegamenti> findCollegamentiBySubmissionIdAndNullOutputBulkFetched(Long submissionId, Long lastId, int limit)
}
