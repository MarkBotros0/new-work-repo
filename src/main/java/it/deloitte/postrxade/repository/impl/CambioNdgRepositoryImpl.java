package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.CambioNdg;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.repository.CambioNdgRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class CambioNdgRepositoryImpl implements CambioNdgRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<CambioNdg> cambioNdg, Submission submission) {
    }


    @Override
    public Map<String, Integer> checkExisting(List<CambioNdg> datiContabiliList, Submission submission) {
        String valuesBlock = buildValuesBlock(datiContabiliList, submission);

        String sql = """
                    WITH MERCHANT_CAMBIO_NDG_INPUT (intermediario, ndg_vecchio, ndg_nuovo, fk_submission) AS (
                        %s
                    )
                    SELECT 
                        mcni.intermediario,
                        mcni.ndg_vecchio,
                        mcni.ndg_nuovo,
                        mcni.fk_submission,
                        CASE WHEN mi.intermediario IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_CAMBIO_NDG_INPUT mcni
                    LEFT JOIN MERCHANT_CAMBIO_NDG mcn ON mcn.intermediario = mcni.intermediario 
                        AND mcn.ndg_vecchio = mcni.ndg_vecchio
                        AND mcn.ndg_nuovo = mcni.ndg_nuovo
                        AND mcn.fk_submission = mcni.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Be defensive with JDBC return types (VARCHAR should map to String, but drivers can vary)
            String intermediario = String.valueOf(row[0]);
            String ndgVecchio = String.valueOf(row[1]);
            String ndgNuovo = String.valueOf(row[2]);
            String submission_fk = String.valueOf(row[3]);
            Number existsNumber = row[4] instanceof Number ? (Number) row[4] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = intermediario
                    + "_" + ndgVecchio
                    + "_" + ndgNuovo
                    + "_" + submission_fk;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<CambioNdg> merchants, Submission submission) {
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (CambioNdg dc : merchants) {
            uniqueRows.add("SELECT '"
                    + escape(dc.getIntermediario()) + "' AS intermediario, '"
                    + escape(dc.getNdgVecchio()) + "' AS ndg_vecchio, "
                    + escape(dc.getNdgNuovo()) + "' AS ndg_nuovo, "
                    + submission.getId() + " AS fk_submission");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String escape(String s) {
        return s.replace("'", "''");
    }

}
