package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import it.deloitte.postrxade.entity.CambioNdg;
import it.deloitte.postrxade.entity.Submission;

public interface CambioNdgRepositoryCustom {
    void bulkInsert(List<CambioNdg> cambioNdg, Submission submission);
    Map<String, Integer> checkExisting(List<CambioNdg> cambioNdg, Submission submission);
}
