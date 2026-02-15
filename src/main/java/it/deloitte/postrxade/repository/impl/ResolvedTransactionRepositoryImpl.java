package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.ResolvedTransaction;
import it.deloitte.postrxade.entity.Submission;

import it.deloitte.postrxade.repository.ResolvedTransactionRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Repository
@Transactional(timeout = 120) // 2 minutes timeout to prevent lock wait timeout
public class ResolvedTransactionRepositoryImpl implements ResolvedTransactionRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<ResolvedTransaction> transactions, Submission currentSubmission, Submission oldSubmission) {
        if (transactions == null || transactions.isEmpty()) {
            return;
        }

        String sql = buildInsertSql(transactions.size());
        Query query = entityManager.createNativeQuery(sql);

        for (int i = 0; i < transactions.size(); i++) {
            setParams(query, i, transactions.get(i), currentSubmission, oldSubmission);
        }

        query.executeUpdate();
    }

    private String buildInsertSql(int batchSize) {
        StringJoiner values = new StringJoiner(", ");
        for (int i = 0; i < batchSize; i++) {
            values.add("( :fk_ingestion_" + i
                    + ", :fk_submission_" + i
                    + ", :fk_current_submission_" + i
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
                "INSERT INTO RESOLVED_TRANSACTION (fk_ingestion, fk_submission, fk_current_submission, tp_rec, id_intermediario, id_esercente, "
                        + "chiave_banca, id_pos, tipo_ope, dt_ope, divisa_ope, tipo_pag, "
                        + "imp_ope, tot_ope, fk_output, created_at) VALUES %s",
                values);
    }

    private void setParams(Query query, int index, ResolvedTransaction transaction, Submission currentSubmission, Submission oldSubmission) {
        query.setParameter("fk_ingestion_" + index,
                transaction.getIngestion() != null ? transaction.getIngestion().getId() : null);
        query.setParameter("fk_submission_" + index, oldSubmission.getId());
        query.setParameter("fk_current_submission_" + index, currentSubmission.getId());
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
    public Map<String, Integer> checkExisting(List<ResolvedTransaction> transactions) {
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
                        CASE WHEN t.id_esercente IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM TRANSACTION_INPUT ti
                    LEFT JOIN TRANSACTION t ON t.id_esercente = ti.id_esercente 
                        AND t.chiave_banca = ti.chiave_banca
                        AND t.id_pos = ti.id_pos
                        AND t.tipo_ope = ti.tipo_ope
                        AND t.dt_ope = ti.dt_ope
                        AND t.divisa_ope = ti.divisa_ope
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager
                .createNativeQuery(sql)
                .getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            // Use String.valueOf to be null-safe and handle JDBC drivers returning non-String types
            // (e.g. dt_ope as java.sql.Date/Timestamp).
            String idEsercente = String.valueOf(row[0]);
            String chiaveBanca = String.valueOf(row[1]);
            String idPos = String.valueOf(row[2]);
            String tipoOpe = String.valueOf(row[3]);
            String dtOpe = String.valueOf(row[4]);
            String divisaOpe = String.valueOf(row[5]);
            Number existsNumber = row[6] instanceof Number ? (Number) row[6] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = idEsercente
                    + "_" + chiaveBanca
                    + "_" + idPos
                    + "_" + tipoOpe
                    + "_" + dtOpe
                    + "_" + divisaOpe;
            result.put(key, existsFlag);
        }
        
        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<ResolvedTransaction> transactions) {
        return transactions.stream()
                .map(t -> "SELECT '"
                        + escape(t.getIdEsercente()) + "' AS id_esercente, '"
                        + escape(t.getChiaveBanca()) + "' AS chiave_banca, '"
                        + escape(t.getIdPos()) + "' AS id_pos, '"
                        + escape(t.getTipoOpe()) + "' AS tipo_ope, '"
                        + escape(t.getDtOpe()) + "' AS dt_ope, '"
                        + escape(t.getDivisaOpe()) + "' AS divisa_ope")
                .collect(Collectors.joining(" UNION ALL "));
    }

    private String escape(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("'", "''");
    }

    @Override
    public List<Long> findResolvedTransactionIdsByCurrentSubmissionIdAndNullOutput(Long submissionId, int rowsPerPage) {
        String nativeSql = "SELECT rt.pk_resolved_transaction " +
                "FROM RESOLVED_TRANSACTION rt " +
                "WHERE rt.fk_current_submission = :submissionId " +
                "AND rt.fk_output IS NULL " +
                "ORDER BY rt.pk_resolved_transaction ASC " +
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

    @Override
    public List<ResolvedTransaction> findByOutputIdWithMerchantsBulkFetched(Long outputId, int offset, int limit) {
        String jpql = "SELECT DISTINCT rt FROM ResolvedTransaction rt " +
                "LEFT JOIN FETCH rt.merchant m " +
                "WHERE rt.output.id = :outputId " +
                "ORDER BY rt.id ASC";

        @SuppressWarnings("unchecked")
        List<ResolvedTransaction> transactions = entityManager
                .createQuery(jpql)
                .setParameter("outputId", outputId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultList();

        return transactions;
    }
}

