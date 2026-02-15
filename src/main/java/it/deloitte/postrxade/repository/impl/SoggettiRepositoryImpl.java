package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.repository.SoggettiRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class SoggettiRepositoryImpl implements SoggettiRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<Soggetti> soggetti, Submission submission) {
    }

    @Override
    public Map<String, Integer> checkExisting(List<Soggetti> soggetti, Submission submission) {
        String valuesBlock = buildValuesBlock(soggetti, submission);

        String sql = """
                    WITH MERCHANT_SOGGETTI_INPUT (intermediario, ndg, fk_submission) AS (
                        %s
                    )
                    SELECT 
                        msi.intermediario,
                        msi.ndg,
                        msi.fk_submission,
                        CASE WHEN mi.intermediario IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_SOGGETTI_INPUT msi
                    LEFT JOIN MERCHANT_SOGGETTI ms ON ms.intermediario = msi.intermediario 
                        AND ms.ndg = msi.ndg
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
            String ndg = String.valueOf(row[1]);
            // fk_submission is BIGINT (Long), convert to String safely
            String submission_fk = String.valueOf(row[2]);
            Number existsNumber = row[3] instanceof Number ? (Number) row[3] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = intermediario
                    + "_" + ndg
                    + "_" + submission_fk;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<Soggetti> merchants, Submission submission) {
        // Optimization: Remove exact duplicates WITHIN the same batch to reduce CTE query size
        // This does NOT affect duplicate detection between batches - that's handled by the DB query
        // The checkExisting() method queries the DB for ALL merchants in the batch using
        // (id_esercente, id_intermediario, fk_submission), so duplicates across batches are detected
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Soggetti m : merchants) {
            uniqueRows.add("SELECT '"
                    + escape(m.getIntermediario()) + "' AS intermediario, '"
                    + escape(m.getNdg()) + "' AS ndg, "
                    + submission.getId() + " AS fk_submission");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String escape(String s) {
        return s.replace("'", "''");
    }

    @Override
    public List<Soggetti> findByOutputId(Long outputId, int offset, int limit) {
        String jpql = "SELECT DISTINCT t FROM MERCHANT_SOGGETTI ms " +
                "WHERE ms.output.id = :outputId " +
                "ORDER BY ms.id ASC";

        @SuppressWarnings("unchecked")
        List<Soggetti> soggettiList = entityManager
                .createQuery(jpql)
                .setParameter("outputId", outputId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultList();

        return soggettiList;
    }
}
