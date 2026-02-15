package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.ResolvedTransaction;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.Transaction;

import java.util.List;
import java.util.Map;

public interface TransactionRepositoryCustom {
    void bulkInsert(List<Transaction> transactions, Submission submission);
    Map<String, Integer> checkExisting(List<Transaction> transactions);
    Map<String, Integer> checkExistingWithResolved(List<ResolvedTransaction> resolvedTransactions);
    List<Transaction> findByOutputIdWithMerchantsBulkFetched(Long outputId, int offset, int limit);
    List<Long> findTransactionIdsBySubmissionIdAndNullOutput(Long submissionId, int rowsPerPage);
}

