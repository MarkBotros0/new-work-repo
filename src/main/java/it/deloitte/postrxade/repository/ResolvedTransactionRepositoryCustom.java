package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.ResolvedTransaction;
import it.deloitte.postrxade.entity.Submission;

import java.util.List;
import java.util.Map;

public interface ResolvedTransactionRepositoryCustom {
    void bulkInsert(List<ResolvedTransaction> transactions, Submission currentSubmission, Submission oldSubmission);
    Map<String, Integer> checkExisting(List<ResolvedTransaction> transactions);
    List<ResolvedTransaction> findByOutputIdWithMerchantsBulkFetched(Long outputId, int offset, int limit);
    List<Long> findResolvedTransactionIdsByCurrentSubmissionIdAndNullOutput(Long submissionId, int rowsPerPage);
}

