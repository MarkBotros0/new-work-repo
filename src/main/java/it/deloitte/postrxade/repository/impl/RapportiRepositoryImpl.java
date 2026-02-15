package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.Collegamenti;
import it.deloitte.postrxade.entity.Rapporti;
import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.repository.RapportiRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class RapportiRepositoryImpl implements RapportiRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<Rapporti> rapporti, Submission submission) {

    }

    @Override
    public Map<String, Integer> checkExisting(List<Rapporti> rapportiList, Submission submission) {
        String valuesBlock = buildValuesBlock(rapportiList, submission);

        String sql = """
                    WITH MERCHANT_RAPPORTI_INPUT (intermediario, chiave_rapporto, fk_submission) AS (
                        %s
                    )
                    SELECT 
                        mri.intermediario,
                        mri.chiave_rapporto,
                        mri.fk_submission,
                        CASE WHEN mi.intermediario IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_RAPPORTI_INPUT mri
                    LEFT JOIN MERCHANT_RAPPORTI mr ON mr.intermediario = mri.intermediario 
                        AND mr.chiave_rapporto = mri.chiave_rapporto
                        AND mr.fk_submission = mri.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Be defensive with JDBC return types (VARCHAR should map to String, but drivers can vary)
            String intermediario = String.valueOf(row[0]);
            String chiave_rapporto = String.valueOf(row[1]);
            // fk_submission is BIGINT (Long), convert to String safely
            String submission_fk = String.valueOf(row[2]);
            Number existsNumber = row[3] instanceof Number ? (Number) row[3] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = intermediario
                    + "_" + chiave_rapporto
                    + "_" + submission_fk;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<Rapporti> merchants, Submission submission) {
        // Optimization: Remove exact duplicates WITHIN the same batch to reduce CTE query size
        // This does NOT affect duplicate detection between batches - that's handled by the DB query
        // The checkExisting() method queries the DB for ALL merchants in the batch using
        // (id_esercente, id_intermediario, fk_submission), so duplicates across batches are detected
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Rapporti m : merchants) {
            uniqueRows.add("SELECT '"
                    + escape(m.getIntermediario()) + "' AS intermediario, '"
                    + escape(m.getChiaveRapporto()) + "' AS chiave_rapporto, "
                    + submission.getId() + " AS fk_submission");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String escape(String s) {
        return s.replace("'", "''");
    }

    @Override
    public List<Rapporti> findByOutputId(Long outputId, int offset, int limit) {
        String jpql = "SELECT DISTINCT t FROM MERCHANT_RAPPORTI mr " +
                "WHERE mr.output.id = :outputId " +
                "ORDER BY mr.id ASC";

        @SuppressWarnings("unchecked")
        List<Rapporti> rapportiList = entityManager
                .createQuery(jpql)
                .setParameter("outputId", outputId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultList();

        return rapportiList;
    }
}
