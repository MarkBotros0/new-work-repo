package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.DatiContabili;
import it.deloitte.postrxade.entity.Submission;

public interface DatiContabiliRepositoryCustom {
    void bulkInsert(List<DatiContabili> datiContabili, Submission submission);
    Map<String, Integer> checkExisting(List<DatiContabili> datiContabili, Submission submission);
    List<DatiContabili> findByOutputId(Long outputId, int offset, int limit);
}
