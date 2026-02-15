package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Merchant;
import it.deloitte.postrxade.entity.ResolvedTransaction;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.Transaction;

import java.util.List;
import java.util.Map;

public interface MerchantRepositoryCustom {
    void bulkInsert(List<Merchant> merchants, Submission submission);
    Map<String, Integer> checkExisting(List<Merchant> merchants, Submission submission);
    Map<String, Integer> checkExistingByTransactions(List<Transaction> transactions);
    Map<String, Integer> checkExistingByResolvedTransactions(List<ResolvedTransaction> transactions);
}

