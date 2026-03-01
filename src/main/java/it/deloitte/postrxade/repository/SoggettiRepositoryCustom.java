package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.entity.Submission;

public interface SoggettiRepositoryCustom {
    void bulkInsert(List<Soggetti> soggetti, Submission submission);
    Map<String, Integer> checkExisting(List<Soggetti> soggetti, Submission submission);
    List<Soggetti> findByOutputId(Long outputId, int offset, int limit);
    
    /**
     * Bulk delete Soggetti by IDs using a single DELETE statement.
     * 
     * @param ids List of Soggetti IDs to delete
     * @return Number of records deleted
     */
    int bulkDeleteByIds(List<Long> ids);
}
