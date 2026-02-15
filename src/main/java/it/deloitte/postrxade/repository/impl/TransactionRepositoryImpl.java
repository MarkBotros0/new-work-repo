package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.ResolvedTransaction;
import it.deloitte.postrxade.entity.Submission;
import it.deloitte.postrxade.entity.Transaction;
import it.deloitte.postrxade.repository.TransactionRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@Transactional(timeout = 120) // 2 minutes timeout to prevent lock wait timeout
public class TransactionRepositoryImpl implements TransactionRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    // Maximum records per single INSERT to avoid lock timeout
    // Smaller batches = shorter transactions = less lock contention
    private static final int MAX_INSERT_BATCH_SIZE = 500;

    @Override
    public void bulkInsert(List<Transaction> transactions, Submission submission) {
        if (transactions == null || transactions.isEmpty()) {
            return;
        }

        // Split into smaller batches to reduce transaction time and lock contention
        for (int i = 0; i < transactions.size(); i += MAX_INSERT_BATCH_SIZE) {
            int endIndex = Math.min(i + MAX_INSERT_BATCH_SIZE, transactions.size());
            List<Transaction> batch = transactions.subList(i, endIndex);
            
            String sql = buildInsertSql(batch.size());
            Query query = entityManager.createNativeQuery(sql);

            for (int j = 0; j < batch.size(); j++) {
                setParams(query, j, batch.get(j), submission);
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
                    + ", :chiave_banca_" + i
                    + ", :id_pos_" + i
                    + ", :tipo_ope_" + i
                    + ", :dt_ope_" + i
                    + ", :divisa_ope_" + i
                    + ", :tipo_pag_" + i
                    + ", :imp_ope_" + i
                    + ", :tot_ope_" + i
                    + ", :fk_output_" + i
                    + ", CURRENT_TIMESTAMP)");
        }

        return String.format(
                "INSERT INTO TRANSACTION (fk_ingestion, fk_submission, tp_rec, id_intermediario, id_esercente, "
                        + "chiave_banca, id_pos, tipo_ope, dt_ope, divisa_ope, tipo_pag, "
                        + "imp_ope, tot_ope, fk_output, created_at) VALUES %s",
                values);
    }

    private void setParams(Query query, int index, Transaction transaction, Submission submission) {
        query.setParameter("fk_ingestion_" + index,
                transaction.getIngestion() != null ? transaction.getIngestion().getId() : null);
        query.setParameter("fk_submission_" + index, submission.getId());
        query.setParameter("tp_rec_" + index, transaction.getTpRec());
        query.setParameter("id_intermediario_" + index, transaction.getIdIntermediario());
        query.setParameter("id_esercente_" + index, transaction.getIdEsercente());
        query.setParameter("chiave_banca_" + index, transaction.getChiaveBanca());
        query.setParameter("id_pos_" + index, transaction.getIdPos());
        query.setParameter("tipo_ope_" + index, transaction.getTipoOpe());
        query.setParameter("dt_ope_" + index, transaction.getDtOpe());
        query.setParameter("divisa_ope_" + index, transaction.getDivisaOpe());
        query.setParameter("tipo_pag_" + index, transaction.getTipoPag());
        query.setParameter("imp_ope_" + index, transaction.getImpOpe());
        query.setParameter("tot_ope_" + index, transaction.getTotOpe());
        query.setParameter("fk_output_" + index,
                transaction.getOutput() != null ? transaction.getOutput().getId() : null);
    }

    @Override
    @Transactional(timeout = 60, readOnly = true) // Read-only transaction with shorter timeout
    public Map<String, Integer> checkExisting(List<Transaction> transactions) {
        if (transactions == null || transactions.isEmpty()) {
            return new HashMap<>();
        }

        String valuesBlock = buildValuesBlock(transactions);

        String sql = """
                WITH TRANSACTION_INPUT (id_esercente, chiave_banca, id_pos, tipo_ope, dt_ope, divisa_ope) AS (
                    %s
                )
                SELECT 
                    ti.id_esercente,
                    ti.chiave_banca,
                    ti.id_pos,
                    ti.tipo_ope,
                    ti.dt_ope,
                    ti.divisa_ope,
                    CASE 
                        WHEN t.id_esercente IS NOT NULL OR rt.id_esercente IS NOT NULL 
                        THEN 1 ELSE 0 
                    END AS ExistsFlag
                FROM TRANSACTION_INPUT ti
                LEFT JOIN TRANSACTION t ON 
                    t.id_esercente = ti.id_esercente AND t.chiave_banca = ti.chiave_banca AND 
                    t.id_pos = ti.id_pos AND t.tipo_ope = ti.tipo_ope AND 
                    t.dt_ope = ti.dt_ope AND t.divisa_ope = ti.divisa_ope
                LEFT JOIN RESOLVED_TRANSACTION rt ON 
                    rt.id_esercente = ti.id_esercente AND rt.chiave_banca = ti.chiave_banca AND 
                    rt.id_pos = ti.id_pos AND rt.tipo_ope = ti.tipo_ope AND 
                    rt.dt_ope = ti.dt_ope AND rt.divisa_ope = ti.divisa_ope
            """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Constructing the key safely
            String key = String.valueOf(row[0]) + "_" + // id_esercente
                    String.valueOf(row[1]) + "_" + // chiave_banca
                    String.valueOf(row[2]) + "_" + // id_pos
                    String.valueOf(row[3]) + "_" + // tipo_ope
                    String.valueOf(row[4]) + "_" + // dt_ope
                    String.valueOf(row[5]);        // divisa_ope

            Number existsNumber = (row[6] instanceof Number) ? (Number) row[6] : 0;
            result.put(key, existsNumber.intValue());
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

//    @Override
//    public Map<String, Integer> checkExistingWithResolved(List<Transaction> transactions) {
//        if (transactions == null || transactions.isEmpty()) {
//            return new HashMap<>();
//        }
//
//        String valuesBlock = buildValuesBlock(transactions);
//
//        // SQL logic: Join both tables and return 1 if found in either
//        String sql = """
//                WITH TRANSACTION_INPUT (id_esercente, chiave_banca, id_pos, tipo_ope, dt_ope, divisa_ope) AS (
//                    %s
//                )
//                SELECT
//                    ti.id_esercente,
//                    ti.chiave_banca,
//                    ti.id_pos,
//                    ti.tipo_ope,
//                    ti.dt_ope,
//                    ti.divisa_ope,
//                    CASE
//                        WHEN t.pk_transaction IS NOT NULL OR rt.pk_resolved_transaction IS NOT NULL
//                        THEN 1 ELSE 0
//                    END AS ExistsFlag
//                FROM TRANSACTION_INPUT ti
//                LEFT JOIN TRANSACTION t ON
//                    t.id_esercente = ti.id_esercente AND t.chiave_banca = ti.chiave_banca AND
//                    t.id_pos = ti.id_pos AND t.tipo_ope = ti.tipo_ope AND
//                    t.dt_ope = ti.dt_ope AND t.divisa_ope = ti.divisa_ope
//                LEFT JOIN RESOLVED_TRANSACTION rt ON
//                    rt.id_esercente = ti.id_esercente AND rt.chiave_banca = ti.chiave_banca AND
//                    rt.id_pos = ti.id_pos AND rt.tipo_ope = ti.tipo_ope AND
//                    rt.dt_ope = ti.dt_ope AND rt.divisa_ope = ti.divisa_ope
//            """.formatted(valuesBlock);
//
//        @SuppressWarnings("unchecked")
//        List<Object[]> rows = entityManager
//                .createNativeQuery(sql)
//                .getResultList();
//
//        Map<String, Integer> result = new HashMap<>();
//        for (Object[] row : rows) {
//            // Using String.valueOf to be null-safe and handle different DB types for dtOpe
//            String key = (String) row[0] + "_" + // id_esercente
//                    (String) row[1] + "_" + // chiave_banca
//                    (String) row[2] + "_" + // id_pos
//                    (String) row[3] + "_" + // tipo_ope
//                    (String) row[4] + "_" + // dt_ope
//                    (String) row[5];        // divisa_ope
//
//            Number existsNumber = (row[6] instanceof Number) ? (Number) row[6] : 0;
//            result.put(key, existsNumber.intValue());
//        }
//
//        return result;
//    }

    private String buildValuesBlock(List<Transaction> transactions) {
        // Optimization: Remove exact duplicates WITHIN the same batch to reduce CTE query size
        // This does NOT affect duplicate detection between batches - that's handled by the DB query
        // The checkExisting() method queries the DB for ALL transactions in the batch,
        // so duplicates across batches are still detected correctly
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Transaction t : transactions) {
            uniqueRows.add("SELECT '"
                    + escape(t.getIdEsercente()) + "' AS id_esercente, '"
                    + escape(t.getChiaveBanca()) + "' AS chiave_banca, '"
                    + escape(t.getIdPos()) + "' AS id_pos, '"
                    + escape(t.getTipoOpe()) + "' AS tipo_ope, '"
                    + escape(t.getDtOpe()) + "' AS dt_ope, '"
                    + escape(t.getDivisaOpe()) + "' AS divisa_ope");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    @Override
    @Transactional(timeout = 60, readOnly = true) // Read-only transaction with shorter timeout
    public Map<String, Integer> checkExistingWithResolved(List<ResolvedTransaction> resolvedTransactions) {
        if (resolvedTransactions == null || resolvedTransactions.isEmpty()) {
            return new HashMap<>();
        }

        String valuesBlock = buildResolvedValuesBlock(resolvedTransactions);

        String sql = """
            WITH TRANSACTION_INPUT (id_esercente, chiave_banca, id_pos, tipo_ope, dt_ope, divisa_ope) AS (
                %s
            )
            SELECT 
                ti.id_esercente,
                ti.chiave_banca,
                ti.id_pos,
                ti.tipo_ope,
                ti.dt_ope,
                ti.divisa_ope,
                CASE 
                    WHEN t.pk_transaction IS NOT NULL OR rt.pk_resolved_transaction IS NOT NULL 
                    THEN 1 ELSE 0 
                END AS ExistsFlag
            FROM TRANSACTION_INPUT ti
            LEFT JOIN TRANSACTION t ON 
                t.id_esercente = ti.id_esercente AND t.chiave_banca = ti.chiave_banca AND 
                t.id_pos = ti.id_pos AND t.tipo_ope = ti.tipo_ope AND 
                t.dt_ope = ti.dt_ope AND t.divisa_ope = ti.divisa_ope
            LEFT JOIN RESOLVED_TRANSACTION rt ON 
                rt.id_esercente = ti.id_esercente AND rt.chiave_banca = ti.chiave_banca AND 
                rt.id_pos = ti.id_pos AND rt.tipo_ope = ti.tipo_ope AND 
                rt.dt_ope = ti.dt_ope AND rt.divisa_ope = ti.divisa_ope
        """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Construct the composite key to match the records in memory later
            String key = String.valueOf(row[0]) + "_" + // id_esercente
                    String.valueOf(row[1]) + "_" + // chiave_banca
                    String.valueOf(row[2]) + "_" + // id_pos
                    String.valueOf(row[3]) + "_" + // tipo_ope
                    String.valueOf(row[4]) + "_" + // dt_ope
                    String.valueOf(row[5]);        // divisa_ope

            Number existsNumber = (row[6] instanceof Number) ? (Number) row[6] : 0;
            result.put(key, existsNumber.intValue());
        }

        return result;
    }

    private String buildResolvedValuesBlock(List<ResolvedTransaction> transactions) {
        // Optimized: Use LinkedHashSet to remove exact duplicates within batch
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (ResolvedTransaction t : transactions) {
            uniqueRows.add("SELECT '"
                    + escape(t.getIdEsercente()) + "' AS id_esercente, '"
                    + escape(t.getChiaveBanca()) + "' AS chiave_banca, '"
                    + escape(t.getIdPos()) + "' AS id_pos, '"
                    + escape(t.getTipoOpe()) + "' AS tipo_ope, '"
                    + escape(t.getDtOpe()) + "' AS dt_ope, '"
                    + escape(t.getDivisaOpe()) + "' AS divisa_ope");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("'", "''");
    }

    @Override
    public List<Transaction> findByOutputIdWithMerchantsBulkFetched(Long outputId, int offset, int limit) {
        String jpql = "SELECT DISTINCT t FROM Transaction t " +
                "LEFT JOIN FETCH t.merchant m " +
                "WHERE t.output.id = :outputId " +
                "ORDER BY t.id ASC";

        @SuppressWarnings("unchecked")
        List<Transaction> transactions = entityManager
                .createQuery(jpql)
                .setParameter("outputId", outputId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultList();

        return transactions;
    }

    @Override
    public List<Long> findTransactionIdsBySubmissionIdAndNullOutput(Long submissionId, int rowsPerPage) {
        String nativeSql = "SELECT t.pk_transaction " +
                "FROM TRANSACTION t " +
                "INNER JOIN INGESTION i ON t.fk_ingestion = i.pk_ingestion " +
                "INNER JOIN SUBMISSION s ON i.fk_submission = s.pk_submission " +
                "WHERE s.pk_submission = :submissionId " +
                "AND t.fk_output IS NULL " +
                "ORDER BY t.pk_transaction ASC " +
                "LIMIT :limit";

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);
        query.setParameter("limit", rowsPerPage);

        @SuppressWarnings("unchecked")
        List<Object> results = query.getResultList();

        return results.stream()
                .filter(obj -> obj instanceof Number) // Filter out nulls and non-Number types
                .map(obj -> ((Number) obj).longValue())
                .collect(Collectors.toList());
    }
}

