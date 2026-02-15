package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.Rapporti;
import it.deloitte.postrxade.entity.Submission;

public interface RapportiRepositoryCustom {
    void bulkInsert(List<Rapporti> rapporti, Submission submission);
    Map<String, Integer> checkExisting(List<Rapporti> rapporti, Submission submission);
    List<Rapporti> findByOutputId(Long outputId, int offset, int limit);
}
