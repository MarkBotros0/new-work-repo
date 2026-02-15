package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.Collegamenti;
import it.deloitte.postrxade.entity.DatiContabili;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.repository.DatiContabiliRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class DatiContabiliRepositoryImpl implements DatiContabiliRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<DatiContabili> datiContabili, Submission submission) {
    }

    @Override
    public Map<String, Integer> checkExisting(List<DatiContabili> datiContabiliList, Submission submission) {
        String valuesBlock = buildValuesBlock(datiContabiliList, submission);

        String sql = """
                    WITH MERCHANT_DATI_CONTABILI_INPUT (intermediario, chiave_rapporto, fk_submission) AS (
                        %s
                    )
                    SELECT 
                        msi.intermediario,
                        msi.chiave_rapporto,
                        msi.fk_submission,
                        CASE WHEN mi.intermediario IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_DATI_CONTABILI_INPUT msi
                    LEFT JOIN MERCHANT_DATI_CONTABILI ms ON ms.intermediario = msi.intermediario 
                        AND ms.chiave_rapporto = msi.chiave_rapporto
                        AND ms.fk_submission = msi.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Be defensive with JDBC return types (VARCHAR should map to String, but drivers can vary)
            String intermediario = String.valueOf(row[0]);
            String chiaveRapporto = String.valueOf(row[1]);
            String submission_fk = String.valueOf(row[2]);
            Number existsNumber = row[3] instanceof Number ? (Number) row[3] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = intermediario
                    + "_" + chiaveRapporto
                    + "_" + submission_fk;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<DatiContabili> merchants, Submission submission) {
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (DatiContabili dc : merchants) {
            uniqueRows.add("SELECT '"
                    + escape(dc.getIntermediario()) + "' AS intermediario, '"
                    + escape(dc.getChiaveRapporto()) + "' AS chiave_rapporto, "
                    + submission.getId() + " AS fk_submission");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String escape(String s) {
        return s.replace("'", "''");
    }

    @Override
    public List<DatiContabili> findByOutputId(Long outputId, int offset, int limit) {
        String jpql = "SELECT DISTINCT t FROM MERCHANT_DATI_CONTABILI mdc " +
                "WHERE mdc.output.id = :outputId " +
                "ORDER BY mc.id ASC";

        @SuppressWarnings("unchecked")
        List<DatiContabili> datiContabiliList = entityManager
                .createQuery(jpql)
                .setParameter("outputId", outputId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultList();

        return datiContabiliList;
    }
}
