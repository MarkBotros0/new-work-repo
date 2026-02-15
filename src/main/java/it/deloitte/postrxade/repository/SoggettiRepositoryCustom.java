package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.entity.Submission;

public interface SoggettiRepositoryCustom {
    void bulkInsert(List<Soggetti> soggetti, Submission submission);
    Map<String, Integer> checkExisting(List<Soggetti> soggetti, Submission submission);
    List<Soggetti> findByOutputId(Long outputId, int offset, int limit);
}
