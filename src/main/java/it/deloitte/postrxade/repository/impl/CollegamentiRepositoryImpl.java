package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.records.MerchantOutputData;
import it.deloitte.postrxade.repository.CollegamentiRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class CollegamentiRepositoryImpl implements CollegamentiRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<Collegamenti> collegamenti, Submission submission) {
    }

    @Override
    public Map<String, Integer> checkExisting(List<Collegamenti> collegamentiList, Submission submission) {
        String valuesBlock = buildValuesBlock(collegamentiList, submission);

        String sql = """
                    WITH MERCHANT_COLLEGAMENTI_INPUT (intermediario, chiave_rapporto, ndg, fk_submission) AS (
                        %s
                    )
                    SELECT 
                        msi.intermediario,
                        msi.chiave_rapporto,
                        msi.ndg,
                        msi.fk_submission,
                        CASE WHEN mi.intermediario IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                    FROM MERCHANT_COLLEGAMENTI_INPUT msi
                    LEFT JOIN MERCHANT_COLLEGAMENTI ms ON ms.intermediario = msi.intermediario 
                        AND ms.chiave_rapporto = msi.chiave_rapporto
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
            String chiaveRapporto = String.valueOf(row[1]);
            String ndg = String.valueOf(row[2]);
            String submission_fk = String.valueOf(row[3]);
            Number existsNumber = row[4] instanceof Number ? (Number) row[4] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = intermediario
                    + "_" + chiaveRapporto
                    + "_" + ndg
                    + "_" + submission_fk;
            result.put(key, existsFlag);
        }

        // Clear persistence context to free memory (read-only query, but Hibernate may cache metadata)
        entityManager.clear();
        return result;
    }

    private String buildValuesBlock(List<Collegamenti> merchants, Submission submission) {
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Collegamenti c : merchants) {
            uniqueRows.add("SELECT '"
                    + escape(c.getIntermediario()) + "' AS intermediario, '"
                    + escape(c.getChiaveRapporto()) + "' AS chiave_rapporto, "
                    + escape(c.getNdg()) + "' AS ndg, "
                    + submission.getId() + " AS fk_submission");
        }
        return String.join(" UNION ALL ", uniqueRows);
    }

    private String escape(String s) {
        return s.replace("'", "''");
    }

    @Override
    public List<Long> findCollegamentiIdsBySubmissionIdAndNullOutput(Long submissionId, int rowsPerPage) {
        String nativeSql = "SELECT mc.pk_collegamenti " +
                "FROM MERCHANT_COLLEGAMENTI mc " +
                "WHERE mc.pk_submission = :submissionId " +
                "AND mc.fk_output IS NULL " +
                "ORDER BY mc.pk_collegamenti ASC " +
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
    public List<MerchantOutputData> findCollegamentiDetailsBySubmissionId(Long submissionId, int limit) {
        String nativeSql = "SELECT mc.ruolo, " +
                "mdc.anno_di_riferimento, " +
                "mdc.divisa, " +
                "mdc.importo_saldo_iniziale, " +
                "mdc.importo_saldo_finale, " +
                "mdc.totale_operazioni_attive, " +
                "mdc.totale_operazioni_passive, " +
                "mdc.giacenza_media, " +
                "mdc.flag_soglia_saldo_iniziale, " +
                "mdc.flag_soglia_saldo_finale, " +
                "mdc.flag_soglia_operazioni_attive, " +
                "mdc.flag_soglia_operazioni_passive, " +
                "mdc.flag_soglia_giacenza_media, " +
                "mdc.altre_informazioni, " +
                "mdc.flag_soglia_altre_informazioni, " +
                "mdc.natura_valuta, " +
                "mdc.importo_titoli_di_stato, " +
                "mdc.flag_soglia_titoli_di_stato, " +
                "mr.cab, " +
                "mr.data_inizio_rapporto, " +
                "mr.data_fine_rapporto, " +
                "ms.sesso, " +
                "ms.codice_fiscale, " +
                "ms.cognome, " +
                "ms.nome, " +
                "ms.data_nascita, " +
                "ms.comune, " +
                "ms.provincia ," +
                "mdc.pk_dati_contabili ," +
                "ms.pk_soggetti ," +
                "mr.pk_rapporti ," +
                "mc.pk_collegamenti " +
                "FROM MERCHANT_COLLEGAMENTI mc " +
                "JOIN MERCHANT_DATI_CONTABILI mdc ON mc.chiave_rapporto = mdc.chiave_rapporto " +
                "JOIN MERCHANT_RAPPORTI mr ON mdc.chiave_rapporto = mr.chiave_rapporto " +
                "JOIN MERCHANT_COLLEGAMENTI ms ON mr.ndg = ms.ndg " +
                "WHERE mc.pk_submission = :submissionId " +
                "AND mc.fk_output IS NULL " +
                "ORDER BY mc.pk_collegamenti ASC " +
                "LIMIT :limit";

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);
        query.setParameter("limit", limit);

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();

        return results.stream()
                .map(this::mapToRecord)
                .collect(Collectors.toList());
    }

    private MerchantOutputData mapToRecord(Object[] row) {
        return new MerchantOutputData(
                (String) row[0],
                row[1] != null ? ((Number) row[1]).intValue() : null,
                (String) row[2],
                (BigDecimal) row[3],
                (BigDecimal) row[4],
                (BigDecimal) row[5],
                (BigDecimal) row[6],
                (BigDecimal) row[7],
                parseBoolean(row[8]),
                parseBoolean(row[9]),
                parseBoolean(row[10]),
                parseBoolean(row[11]),
                parseBoolean(row[12]),
                (String) row[13],
                parseBoolean(row[14]),
                (String) row[15],
                (BigDecimal) row[16],
                parseBoolean(row[17]),
                (String) row[18],
                (Date) row[19],
                (Date) row[20],
                (String) row[21],
                (String) row[22],
                (String) row[23],
                (String) row[24],
                (Date) row[25],
                (String) row[26],
                (String) row[27],
                (long) row[28],
                (long) row[29],
                (long) row[30],
                (long) row[31]
        );
    }

    private Boolean parseBoolean(Object obj) {
        if (obj == null) return null;
        if (obj instanceof Boolean b) return b;
        if (obj instanceof Number n) return n.intValue() == 1;
        if (obj instanceof String s) return s.equalsIgnoreCase("Y") || s.equals("1");
        return false;
    }

    @Override
    public List<Collegamenti> findByOutputId(Long outputId, int offset, int limit) {
        String jpql = "SELECT DISTINCT t FROM MERCHANT_COLLEGAMENTI mc " +
                "WHERE mc.output.id = :outputId " +
                "ORDER BY mc.id ASC";

        @SuppressWarnings("unchecked")
        List<Collegamenti> transactions = entityManager
                .createQuery(jpql)
                .setParameter("outputId", outputId)
                .setFirstResult(offset)
                .setMaxResults(limit)
                .getResultList();

        return transactions;
    }

    @Override
    public List<Collegamenti> findCollegamentiBySubmissionIdAndNullOutputBulkFetched(Long submissionId, Long lastId, int limit) {
        String nativeSql;

        if (lastId == null) {
            nativeSql = """
            SELECT
                c.pk_collegamenti,
                c.fk_ingestion,
                c.fk_submission,
                c.intermediario,
                c.chiave_rapporto,
                c.ndg,
                c.ruolo,
                c.data_inizio_collegamento,
                c.data_fine_collegamento,
                c.ruolo_interno,
                c.flag_stato_collegamento,
                c.data_predisposizione_flusso,
                c.controllo_di_fine_riga,
                c.created_at,
                c.fk_output
            FROM MERCHANT_COLLEGAMENTI c
            WHERE c.fk_submission = :submissionId
              AND c.fk_output IS NULL
            ORDER BY c.pk_collegamenti ASC
            LIMIT :limit
            """;
        } else {
            nativeSql = """
            SELECT
                c.pk_collegamenti,
                c.fk_ingestion,
                c.fk_submission,
                c.intermediario,
                c.chiave_rapporto,
                c.ndg,
                c.ruolo,
                c.data_inizio_collegamento,
                c.data_fine_collegamento,
                c.ruolo_interno,
                c.flag_stato_collegamento,
                c.data_predisposizione_flusso,
                c.controllo_di_fine_riga,
                c.created_at,
                c.fk_output
            FROM MERCHANT_COLLEGAMENTI c
            WHERE c.fk_submission = :submissionId
              AND c.fk_output IS NULL
              AND c.pk_collegamenti > :lastId
            ORDER BY c.pk_collegamenti ASC
            LIMIT :limit
            """;
        }

        Query query = entityManager
                .createNativeQuery(nativeSql)
                .setParameter("submissionId", submissionId)
                .setParameter("limit", limit);

        if (lastId != null) {
            query.setParameter("lastId", lastId);
        }

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();

        List<Collegamenti> collegamentiList = new ArrayList<>(results.size());

        for (Object[] row : results) {
            Collegamenti c = new Collegamenti();
            int idx = 0;

            c.setId(((Number) row[idx++]).longValue());

            c.setIngestion(row[idx] != null
                    ? entityManager.getReference(Ingestion.class, ((Number) row[idx]).longValue())
                    : null);
            idx++;

            c.setSubmission(row[idx] != null
                    ? entityManager.getReference(Submission.class, ((Number) row[idx]).longValue())
                    : null);
            idx++;

            c.setIntermediario(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setChiaveRapporto(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setNdg(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setRuolo(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setDataInizioCollegamento(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setDataFineCollegamento(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setRuoloInterno(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setFlagStatoCollegamento(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setDataPredisposizioneFlusso(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setControlloDiFineRiga(row[idx] != null ? String.valueOf(row[idx]) : null);
            idx++;

            c.setCreatedAt(row[idx] != null
                    ? ((java.sql.Timestamp) row[idx]).toLocalDateTime()
                    : null);
            idx++;

            c.setOutput(row[idx] != null
                    ? entityManager.getReference(Output.class, ((Number) row[idx]).longValue())
                    : null);

            collegamentiList.add(c);
        }

        return collegamentiList;
    }

    @Override
    public Map<String, Integer> checkCollegamentiForSoggetti(List<Soggetti> soggettiList, Submission submission) {
        if (soggettiList == null || soggettiList.isEmpty()) {
            return new HashMap<>();
        }

        // Build VALUES block for batch query
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Soggetti s : soggettiList) {
            uniqueRows.add("SELECT '" + escape(s.getNdg()) + "' AS ndg, " + submission.getId() + " AS fk_submission");
        }
        String valuesBlock = String.join(" UNION ALL ", uniqueRows);

        String sql = """
                WITH SOGGETTI_INPUT (ndg, fk_submission) AS (
                    %s
                )
                SELECT 
                    si.ndg,
                    si.fk_submission,
                    CASE WHEN c.pk_collegamenti IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                FROM SOGGETTI_INPUT si
                LEFT JOIN MERCHANT_COLLEGAMENTI c 
                    ON si.ndg = c.ndg 
                    AND si.fk_submission = c.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager.createNativeQuery(sql).getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            String ndg = String.valueOf(row[0]);
            String submissionId = String.valueOf(row[1]);
            Number existsNumber = row[2] instanceof Number ? (Number) row[2] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = ndg + "_" + submissionId;
            result.put(key, existsFlag);
        }

        entityManager.clear();
        return result;
    }

    @Override
    public Map<String, Integer> checkCollegamentiForRapporti(List<Rapporti> rapportiList, Submission submission) {
        if (rapportiList == null || rapportiList.isEmpty()) {
            return new HashMap<>();
        }

        // Build VALUES block for batch query
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (Rapporti r : rapportiList) {
            uniqueRows.add("SELECT '" + escape(r.getChiaveRapporto()) + "' AS chiave_rapporto, " + submission.getId() + " AS fk_submission");
        }
        String valuesBlock = String.join(" UNION ALL ", uniqueRows);

        String sql = """
                WITH RAPPORTI_INPUT (chiave_rapporto, fk_submission) AS (
                    %s
                )
                SELECT 
                    ri.chiave_rapporto,
                    ri.fk_submission,
                    CASE WHEN c.pk_collegamenti IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                FROM RAPPORTI_INPUT ri
                LEFT JOIN MERCHANT_COLLEGAMENTI c 
                    ON ri.chiave_rapporto = c.chiave_rapporto 
                    AND ri.fk_submission = c.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager.createNativeQuery(sql).getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            String chiaveRapporto = String.valueOf(row[0]);
            String submissionId = String.valueOf(row[1]);
            Number existsNumber = row[2] instanceof Number ? (Number) row[2] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = chiaveRapporto + "_" + submissionId;
            result.put(key, existsFlag);
        }

        entityManager.clear();
        return result;
    }

    @Override
    public Map<String, Integer> checkCollegamentiForDatiContabili(List<DatiContabili> datiContabiliList, Submission submission) {
        if (datiContabiliList == null || datiContabiliList.isEmpty()) {
            return new HashMap<>();
        }

        // Build VALUES block for batch query
        Set<String> uniqueRows = new LinkedHashSet<>();
        for (DatiContabili d : datiContabiliList) {
            uniqueRows.add("SELECT '" + escape(d.getChiaveRapporto()) + "' AS chiave_rapporto, " + submission.getId() + " AS fk_submission");
        }
        String valuesBlock = String.join(" UNION ALL ", uniqueRows);

        String sql = """
                WITH DATI_CONTABILI_INPUT (chiave_rapporto, fk_submission) AS (
                    %s
                )
                SELECT 
                    di.chiave_rapporto,
                    di.fk_submission,
                    CASE WHEN c.pk_collegamenti IS NOT NULL THEN 1 ELSE 0 END AS ExistsFlag
                FROM DATI_CONTABILI_INPUT di
                LEFT JOIN MERCHANT_COLLEGAMENTI c 
                    ON di.chiave_rapporto = c.chiave_rapporto 
                    AND di.fk_submission = c.fk_submission
                """.formatted(valuesBlock);

        @SuppressWarnings("unchecked")
        List<Object[]> rows = entityManager.createNativeQuery(sql).getResultList();

        Map<String, Integer> result = new HashMap<>();
        for (Object[] row : rows) {
            String chiaveRapporto = String.valueOf(row[0]);
            String submissionId = String.valueOf(row[1]);
            Number existsNumber = row[2] instanceof Number ? (Number) row[2] : 0;
            Integer existsFlag = existsNumber != null ? existsNumber.intValue() : 0;
            String key = chiaveRapporto + "_" + submissionId;
            result.put(key, existsFlag);
        }

        entityManager.clear();
        return result;
    }


}
