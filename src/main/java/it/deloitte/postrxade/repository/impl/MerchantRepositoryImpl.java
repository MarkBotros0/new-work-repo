package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.Merchant;
import it.deloitte.postrxade.entity.ResolvedTransaction;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.Transaction;
import it.deloitte.postrxade.repository.MerchantRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Repository
@Transactional(timeout = 120) // 2 minutes timeout to prevent lock wait timeout
public class MerchantRepositoryImpl implements MerchantRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    // Maximum records per single INSERT to avoid lock timeout
    // Smaller batches = shorter transactions = less lock contention
    private static final int MAX_INSERT_BATCH_SIZE = 500;

    @Override
    public void bulkInsert(List<Merchant> merchants, Submission submission) {
        if (merchants == null || merchants.isEmpty()) {
            return;
        }

        // Split into smaller batches to reduce transaction time and lock contention
        for (int i = 0; i < merchants.size(); i += MAX_INSERT_BATCH_SIZE) {
            int endIndex = Math.min(i + MAX_INSERT_BATCH_SIZE, merchants.size());
            List<Merchant> batch = merchants.subList(i, endIndex);
            
            String sql = buildInsertSql(batch.size());
            Query query = entityManager.createNativeQuery(sql);

            for (int j = 0; j < batch.size(); j++) {
                Merchant merchant = batch.get(j);
                setParams(query, j, merchant, submission);
            }

            query.executeUpdate();
            entityManager.flush(); // Flush to release locks earlier
            entityManager.clear(); // Clear to free memory
        }
    }

    private String buildInsertSql(int batchSize) {
        StringJoiner values = new StringJoiner(", ");
        for (int i = 0; i < batchSize; i++) {
            values.add("( :fk_ingestion_" + i
                    + ", :fk_submission_" + i
                    + ", :tp_rec_" + i
                    + ", :id_intermediario_" + i
                    + ", :id_esercente_" + i
                    + ", :cod_fiscale_" + i
                    + ", :partita_iva_" + i
                    + ", :id_salmov_" + i
                    + ", CURRENT_TIMESTAMP)");
        }

        return String.format("INSERT INTO MERCHANT (fk_ingestion, fk_submission, tp_rec, id_intermediario, id_esercente, " +
                "cod_fiscale, partita_iva, id_salmov, created_at) VALUES %s", values);
    }

    private void setParams(Query query, int index, Merchant merchant, Submission submission) {
        query.setParameter("fk_ingestion_" + index, merchant.getIngestion() != null ? merchant.getIngestion().getId() : null);
        query.setParameter("fk_submission_" + index, submission.getId());
        query.setParameter("tp_rec_" + index, merchant.getTpRec());
        query.setParameter("id_intermediario_" + index, merchant.getIdIntermediario());
        query.setParameter("id_esercente_" + index, merchant.getIdEsercente());
        query.setParameter("cod_fiscale_" + index, merchant.getCodFiscale());
        query.setParameter("partita_iva_" + index, merchant.getPartitaIva());
        query.setParameter("id_salmov_" + index, merchant.getIdSalmov());
    }

    @Override
    @Transactional(timeout = 60, readOnly = true) // Read-only transaction with shorter timeout
    public Map<String, Integer> checkExisting(List<Merchant> merchants, Submission submission) {
        String valuesBlock = buildValuesBlock(merchants, submission);

        String sql = """
                    WITH MERCHANT_INPUT (id_esercente, id_intermediario, fk_submission) AS (
                        %s
                    )
                    SELECT 
                        mi.id_esercente,
                        mi.id_intermediario,
                        mi.fk_submission,
                        CASE WHEN m.id_esercente IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_INPUT mi
                    LEFT JOIN MERCHANT m ON m.id_esercente = mi.id_esercente 
                        AND m.id_intermediario = mi.id_intermediario
                        AND m.fk_submission = mi.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Be defensive with JDBC return types (VARCHAR should map to String, but drivers can vary)
            String idEsercente = String.valueOf(row[0]);
            String idIntermediario = String.valueOf(row[1]);
            // fk_submission is BIGINT (Long), convert to String safely
            String submission_fk = String.valueOf(row[2]);
            Number existsNumber = row[3] instanceof Number ? (Number) row[3] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = idEsercente
                    + "_" + idIntermediario
                    + "_" + submission_fk;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    @Override
    @Transactional(timeout = 60, readOnly = true) // Read-only transaction with shorter timeout
    public Map<String, Integer> checkExistingByTransactions(List<Transaction> transactions) {
        String valuesBlock = buildValuesBlockByTransaction(transactions);

        String sql = """
                    WITH MERCHANT_INPUT (id_esercente, id_intermediario) AS (
                        %s
                    )
                    SELECT 
                        mi.id_esercente,
                        mi.id_intermediario,
                        CASE WHEN m.id_esercente IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_INPUT mi
                    LEFT JOIN MERCHANT m ON m.id_esercente = mi.id_esercente 
                        AND m.id_intermediario = mi.id_intermediario
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            String idEsercente = String.valueOf(row[0]);
            String idIntermediario = String.valueOf(row[1]);
            Number existsNumber = row[2] instanceof Number ? (Number) row[2] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = idEsercente + "_" + idIntermediario;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<Merchant> merchants, Submission submission) {
        // Optimization: Remove exact duplicates WITHIN the same batch to reduce CTE query size
        // This does NOT affect duplicate detection between batches - that's handled by the DB query
        // The checkExisting() method queries the DB for ALL merchants in the batch using
        // (id_esercente, id_intermediario, fk_submission), so duplicates across batches are detected
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Merchant m : merchants) {
            uniqueRows.add("SELECT '"
                    + escape(m.getIdEsercente()) + "' AS id_esercente, '"
                    + escape(m.getIdIntermediario()) + "' AS id_intermediario, "
                    + submission.getId() + " AS fk_submission");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String buildValuesBlockByTransaction(List<Transaction> transactions) {
        // Optimization: Deduplicate merchant keys WITHIN the batch to reduce CTE query size
        // Multiple transactions can reference the same merchant, so we only need to check each merchant once
        // This does NOT affect duplicate detection - the query still checks ALL merchants in the DB
        Set<String> uniqueKeys = transactions.stream()
                .map(t -> "SELECT '" + escape(t.getIdEsercente()) + "' AS id_esercente, '"
                        + escape(t.getIdIntermediario()) + "' AS id_intermediario")
                .collect(Collectors.toSet());
        return String.join(" UNION ALL ", uniqueKeys);
    }

    private String escape(String s) {
        return s.replace("'", "''");
    }

    @Override
    @Transactional(timeout = 60, readOnly = true) // Read-only transaction with shorter timeout
    public Map<String, Integer> checkExistingByResolvedTransactions(List<ResolvedTransaction> transactions) {
        if (transactions == null || transactions.isEmpty()) {
            return new HashMap<>();
        }

        String valuesBlock = buildValuesBlockByResolved(transactions);

        String sql = """
                WITH MERCHANT_INPUT (id_esercente, id_intermediario) AS (
                    %s
                )
                SELECT 
                    mi.id_esercente,
                    mi.id_intermediario,
                    CASE WHEN m.id_esercente IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                FROM MERCHANT_INPUT mi
                LEFT JOIN MERCHANT m ON m.id_esercente = mi.id_esercente 
                    AND m.id_intermediario = mi.id_intermediario
            """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            String idEsercente = String.valueOf(row[0]);
            String idIntermediario = String.valueOf(row[1]);

            // Handling potential nulls or different numeric types from the DB driver
            Number existsNumber = (row[2] instanceof Number) ? (Number) row[2] : 0;
            Integer existsFlag = (existsNumber != null) ? existsNumber.intValue() : 0;

            String key = idEsercente + "_" + idIntermediario;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory
        entityManager.clear();
        return result;
    }

    /**
     * Builds the SELECT ... UNION ALL block using fields from ResolvedTransaction
     */
    private String buildValuesBlockByResolved(List<ResolvedTransaction> transactions) {
        // Optimized: Use distinct merchant keys to reduce query size significantly
        Set<String> uniqueKeys = transactions.stream()
                .map(t -> "SELECT '" + escape(t.getIdEsercente()) + "' AS id_esercente, '"
                        + escape(t.getIdIntermediario()) + "' AS id_intermediario")
                .collect(Collectors.toSet());
        return String.join(" UNION ALL ", uniqueKeys);
    }
}

