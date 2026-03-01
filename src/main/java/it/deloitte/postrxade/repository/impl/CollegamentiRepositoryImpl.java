package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.*;
import it.deloitte.postrxade.records.MerchantOutputData;
import it.deloitte.postrxade.repository.CollegamentiRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

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
                "WHERE mc.fk_submission = :submissionId " +
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
    public List<Collegamenti> findCollegamentiWithChildrenByOutputId(Long submissionId, Long outputId, int limit) {
        // Fetch Collegamenti with all children (Rapporti, Soggetti, DatiContabili) using JOINs
        // This avoids N+1 query problem and fetches everything in a single query
        String nativeSql = """
            SELECT
                -- Collegamenti fields (15 fields)
                c.pk_collegamenti, c.fk_ingestion, c.fk_submission, c.intermediario, c.chiave_rapporto,
                c.ndg, c.ruolo, c.data_inizio_collegamento, c.data_fine_collegamento, c.ruolo_interno,
                c.flag_stato_collegamento, c.data_predisposizione_flusso, c.controllo_di_fine_riga,
                c.created_at, c.fk_output,
                
                -- Rapporti fields (18 fields)
                r.pk_rapporti, r.fk_ingestion, r.fk_submission, r.intermediario, r.chiave_rapporto,
                r.tipo_rapporto_interno, r.forma_tecnica, r.filiale, r.cab, r.numero_conto,
                r.cin, r.divisa, r.data_inizio_rapporto, r.data_fine_rapporto, r.note,
                r.flag_stato_rapporto, r.data_predisposizione, r.controllo_di_fine_riga,
                
                -- Soggetti fields (17 fields)
                s.pk_soggetti, s.fk_ingestion, s.fk_submission, s.intermediario, s.ndg,
                s.data_censimento_anagrafico, s.data_estinzione_anagrafica, s.filiale_censimento_anagrafico,
                s.tipo_soggetto, s.natura_giuridica, s.sesso, s.codice_fiscale, s.cognome,
                s.nome, s.data_nascita, s.comune, s.provincia,
                
                -- DatiContabili fields (26 fields)
                d.pk_dati_contabili, d.fk_ingestion, d.fk_submission, d.intermediario, d.chiave_rapporto,
                d.anno_di_riferimento, d.periodicita, d.progressivo_periodicita, d.divisa,
                d.data_inizio_riferimento, d.data_fine_riferimento, d.importo_saldo_iniziale,
                d.importo_saldo_finale, d.totale_operazioni_attive, d.totale_operazioni_passive,
                d.giacenza_media, d.flag_soglia_saldo_iniziale, d.flag_soglia_saldo_finale,
                d.flag_soglia_operazioni_attive, d.flag_soglia_operazioni_passive, d.flag_soglia_giacenza_media,
                d.altre_informazioni, d.flag_stato_importo, d.data_predisposizione,
                d.tipo_rapporto_interno, d.forma_tecnica
                
            FROM MERCHANT_COLLEGAMENTI c
            LEFT JOIN MERCHANT_RAPPORTI r 
                ON c.chiave_rapporto = r.chiave_rapporto 
                AND c.fk_submission = r.fk_submission
                AND r.fk_output = :outputId
            LEFT JOIN MERCHANT_SOGGETTI s 
                ON c.ndg = s.ndg 
                AND c.fk_submission = s.fk_submission
                AND s.fk_output = :outputId
            LEFT JOIN MERCHANT_DATI_CONTABILI d 
                ON c.chiave_rapporto = d.chiave_rapporto 
                AND c.fk_submission = d.fk_submission
                AND d.fk_output = :outputId
            WHERE c.fk_submission = :submissionId
              AND c.fk_output = :outputId
            ORDER BY c.pk_collegamenti ASC
            LIMIT :limit
            """;

        Query query = entityManager.createNativeQuery(nativeSql)
                .setParameter("submissionId", submissionId)
                .setParameter("outputId", outputId)
                .setParameter("limit", limit);

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();
        List<Collegamenti> collegamentiList = new ArrayList<>(results.size());

        for (Object[] row : results) {
            int idx = 0;
            
            // Parse Collegamenti (15 fields)
            Collegamenti c = new Collegamenti();
            c.setId(getLong(row[idx++]));
            c.setIngestion(row[idx] != null ? entityManager.getReference(Ingestion.class, getLong(row[idx])) : null); idx++;
            c.setSubmission(row[idx] != null ? entityManager.getReference(Submission.class, getLong(row[idx])) : null); idx++;
            c.setIntermediario(getString(row[idx++]));
            c.setChiaveRapporto(getString(row[idx++]));
            c.setNdg(getString(row[idx++]));
            c.setRuolo(getString(row[idx++]));
            c.setDataInizioCollegamento(getString(row[idx++]));
            c.setDataFineCollegamento(getString(row[idx++]));
            c.setRuoloInterno(getString(row[idx++]));
            c.setFlagStatoCollegamento(getString(row[idx++]));
            c.setDataPredisposizioneFlusso(getString(row[idx++]));
            c.setControlloDiFineRiga(getString(row[idx++]));
            c.setCreatedAt(row[idx] != null ? ((java.sql.Timestamp) row[idx]).toLocalDateTime() : null); idx++;
            c.setOutput(row[idx] != null ? entityManager.getReference(Output.class, getLong(row[idx])) : null); idx++;
            
            // Parse Rapporti (18 fields) - if exists
            if (row[idx] != null) {
                Rapporti r = new Rapporti();
                r.setId(getLong(row[idx++]));
                r.setIngestion(row[idx] != null ? entityManager.getReference(Ingestion.class, getLong(row[idx])) : null); idx++;
                r.setSubmission(row[idx] != null ? entityManager.getReference(Submission.class, getLong(row[idx])) : null); idx++;
                r.setIntermediario(getString(row[idx++]));
                r.setChiaveRapporto(getString(row[idx++]));
                r.setTipoRapportoInterno(getString(row[idx++]));
                r.setFormaTecnica(getString(row[idx++]));
                r.setFiliale(getString(row[idx++]));
                r.setCab(getString(row[idx++]));
                r.setNumeroConto(getString(row[idx++]));
                r.setCin(getString(row[idx++]));
                r.setDivisa(getString(row[idx++]));
                r.setDataInizioRapporto(getString(row[idx++]));
                r.setDataFineRapporto(getString(row[idx++]));
                r.setNote(getString(row[idx++]));
                r.setFlagStatoRapporto(getString(row[idx++]));
                r.setDataPredisposizione(getString(row[idx++]));
                r.setControlloDiFineRiga(getString(row[idx++]));
                c.setRapporto(r);
            } else {
                idx += 18; // Skip Rapporti fields
            }
            
            // Parse Soggetti (17 fields) - if exists
            if (row[idx] != null) {
                Soggetti s = new Soggetti();
                s.setId(getLong(row[idx++]));
                s.setIngestion(row[idx] != null ? entityManager.getReference(Ingestion.class, getLong(row[idx])) : null); idx++;
                s.setSubmission(row[idx] != null ? entityManager.getReference(Submission.class, getLong(row[idx])) : null); idx++;
                s.setIntermediario(getString(row[idx++]));
                s.setNdg(getString(row[idx++]));
                s.setDataCensimentoAnagrafico(getString(row[idx++]));
                s.setDataEstinzioneAnagrafica(getString(row[idx++]));
                s.setFilialeCensimentoAnagrafico(getString(row[idx++]));
                s.setTipoSoggetto(getString(row[idx++]));
                s.setNaturaGiuridica(getString(row[idx++]));
                s.setSesso(getString(row[idx++]));
                s.setCodiceFiscale(getString(row[idx++]));
                s.setCognome(getString(row[idx++]));
                s.setNome(getString(row[idx++]));
                s.setDataNascita(getString(row[idx++]));
                s.setComune(getString(row[idx++]));
                s.setProvincia(getString(row[idx++]));
                c.setSoggetto(s);
            } else {
                idx += 17; // Skip Soggetti fields
            }
            
            // Parse DatiContabili (26 fields) - if exists
            if (row[idx] != null) {
                DatiContabili d = new DatiContabili();
                d.setId(getLong(row[idx++]));
                d.setIngestion(row[idx] != null ? entityManager.getReference(Ingestion.class, getLong(row[idx])) : null); idx++;
                d.setSubmission(row[idx] != null ? entityManager.getReference(Submission.class, getLong(row[idx])) : null); idx++;
                d.setIntermediario(getString(row[idx++]));
                d.setChiaveRapporto(getString(row[idx++]));
                d.setAnnoDiRiferimento(getString(row[idx++]));
                d.setPeriodicita(getString(row[idx++]));
                d.setProgressivoPeriodicita(getString(row[idx++]));
                d.setDivisa(getString(row[idx++]));
                d.setDataInizioRiferimento(getString(row[idx++]));
                d.setDataFineRiferimento(getString(row[idx++]));
                d.setImportoSaldoIniziale(getString(row[idx++]));
                d.setImportoSaldoFinale(getString(row[idx++]));
                d.setTotaleOperazioniAttive(getString(row[idx++]));
                d.setTotaleOperazioniPassive(getString(row[idx++]));
                d.setGiacenzaMedia(getString(row[idx++]));
                d.setFlagSogliaSaldoIniziale(getString(row[idx++]));
                d.setFlagSogliaSaldoFinale(getString(row[idx++]));
                d.setFlagSogliaOperazioniAttive(getString(row[idx++]));
                d.setFlagSogliaOperazioniPassive(getString(row[idx++]));
                d.setFlagSogliaGiacenzaMedia(getString(row[idx++]));
                d.setAltreInformazioni(getString(row[idx++]));
                d.setFlagStatoImporto(getString(row[idx++]));
                d.setDataPredisposizione(getString(row[idx++]));
                d.setTipoRapportoInterno(getString(row[idx++]));
                d.setFormaTecnica(getString(row[idx++]));
                c.setDatiContabili(d);
            } else {
                idx += 26; // Skip DatiContabili fields
            }

            collegamentiList.add(c);
        }

        return collegamentiList;
    }
    
    // Helper methods for safe type conversion
    private Long getLong(Object value) {
        return value != null ? ((Number) value).longValue() : null;
    }
    
    private String getString(Object value) {
        return value != null ? String.valueOf(value) : null;
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


    @Override
    public List<Object[]> findNdgAndChiaviByIds(List<Long> collegamentiIds) {
        if (collegamentiIds == null || collegamentiIds.isEmpty()) {
            return new ArrayList<>();
        }

        String nativeSql = """
                SELECT DISTINCT c.ndg, c.chiave_rapporto
                FROM MERCHANT_COLLEGAMENTI c
                WHERE c.pk_collegamenti IN (:ids)
                """;

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("ids", collegamentiIds);

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();

        return results;
    }

    @Override
    public List<Object[]> findOrphanCollegamenti(Long submissionId) {
        String nativeSql = """
                SELECT 
                    c.pk_collegamenti,
                    c.ndg,
                    c.chiave_rapporto,
                    CASE WHEN s.pk_soggetti IS NOT NULL THEN 1 ELSE 0 END AS has_soggetti,
                    CASE WHEN r.pk_rapporti IS NOT NULL THEN 1 ELSE 0 END AS has_rapporti,
                    CONCAT_WS('|',
                        COALESCE(c.intermediario, ''),
                        COALESCE(c.chiave_rapporto, ''),
                        COALESCE(c.ndg, ''),
                        COALESCE(c.ruolo, ''),
                        COALESCE(c.data_inizio_collegamento, ''),
                        COALESCE(c.data_fine_collegamento, ''),
                        COALESCE(c.ruolo_interno, ''),
                        COALESCE(c.flag_stato_collegamento, ''),
                        COALESCE(c.data_predisposizione_flusso, ''),
                        COALESCE(c.controllo_di_fine_riga, '')
                    ) AS raw_row
                FROM MERCHANT_COLLEGAMENTI c
                LEFT JOIN MERCHANT_SOGGETTI s 
                    ON c.ndg = s.ndg 
                    AND c.fk_submission = s.fk_submission
                LEFT JOIN MERCHANT_RAPPORTI r 
                    ON c.chiave_rapporto = r.chiave_rapporto 
                    AND c.fk_submission = r.fk_submission
                WHERE c.fk_submission = :submissionId
                  AND (s.pk_soggetti IS NULL OR r.pk_rapporti IS NULL)
                """;

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();

        return results;
    }

    @Override
    public List<Object[]> findSoggettiToOrphan(Long submissionId) {
        // Find Soggetti whose Collegamenti parent has NO children (will be deleted)
        String nativeSql = """
                SELECT 
                    s.pk_soggetti,
                    s.ndg,
                    CONCAT_WS('|',
                        COALESCE(s.intermediario, ''),
                        COALESCE(s.ndg, ''),
                        COALESCE(s.data_censimento_anagrafico, ''),
                        COALESCE(s.data_estinzione_anagrafica, ''),
                        COALESCE(s.filiale_censimento_anagrafico, ''),
                        COALESCE(s.tipo_soggetto, ''),
                        COALESCE(s.natura_giuridica, ''),
                        COALESCE(s.sesso, ''),
                        COALESCE(s.codice_fiscale, ''),
                        COALESCE(s.cognome, ''),
                        COALESCE(s.nome, ''),
                        COALESCE(s.data_nascita, ''),
                        COALESCE(s.comune, ''),
                        COALESCE(s.provincia, ''),
                        COALESCE(s.nazione, ''),
                        COALESCE(s.data_predisposizione_flusso, ''),
                        COALESCE(s.controllo_di_fine_riga, '')
                    ) AS raw_row
                FROM MERCHANT_SOGGETTI s
                INNER JOIN MERCHANT_COLLEGAMENTI c 
                    ON s.ndg = c.ndg 
                    AND s.fk_submission = c.fk_submission
                LEFT JOIN MERCHANT_SOGGETTI s2 
                    ON c.ndg = s2.ndg 
                    AND c.fk_submission = s2.fk_submission
                LEFT JOIN MERCHANT_RAPPORTI r 
                    ON c.chiave_rapporto = r.chiave_rapporto 
                    AND c.fk_submission = r.fk_submission
                WHERE s.fk_submission = :submissionId
                  AND (s2.pk_soggetti IS NULL OR r.pk_rapporti IS NULL)
                """;

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();

        return results;
    }

    @Override
    public List<Object[]> findRapportiToOrphan(Long submissionId) {
        // Find Rapporti whose Collegamenti parent has NO children (will be deleted)
        String nativeSql = """
                SELECT 
                    r.pk_rapporti,
                    r.chiave_rapporto,
                    CONCAT_WS('|',
                        COALESCE(r.intermediario, ''),
                        COALESCE(r.chiave_rapporto, ''),
                        COALESCE(r.tipo_rapporto_interno, ''),
                        COALESCE(r.forma_tecnica, ''),
                        COALESCE(r.filiale, ''),
                        COALESCE(r.cab, ''),
                        COALESCE(r.numero_conto, ''),
                        COALESCE(r.cin, ''),
                        COALESCE(r.divisa, ''),
                        COALESCE(r.data_inizio_rapporto, ''),
                        COALESCE(r.data_fine_rapporto, ''),
                        COALESCE(r.note, ''),
                        COALESCE(r.flag_stato_rapporto, ''),
                        COALESCE(r.data_predisposizione, ''),
                        COALESCE(r.controllo_di_fine_riga, '')
                    ) AS raw_row
                FROM MERCHANT_RAPPORTI r
                INNER JOIN MERCHANT_COLLEGAMENTI c 
                    ON r.chiave_rapporto = c.chiave_rapporto 
                    AND r.fk_submission = c.fk_submission
                LEFT JOIN MERCHANT_SOGGETTI s 
                    ON c.ndg = s.ndg 
                    AND c.fk_submission = s.fk_submission
                LEFT JOIN MERCHANT_RAPPORTI r2 
                    ON c.chiave_rapporto = r2.chiave_rapporto 
                    AND c.fk_submission = r2.fk_submission
                WHERE r.fk_submission = :submissionId
                  AND (s.pk_soggetti IS NULL OR r2.pk_rapporti IS NULL)
                """;

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);

        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();

        return results;
    }

    @Override
    @Transactional
    public int bulkDeleteByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }

        String nativeSql = """
                DELETE FROM MERCHANT_COLLEGAMENTI
                WHERE pk_collegamenti IN (:ids)
                """;

        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("ids", ids);

        int deletedCount = query.executeUpdate();
        entityManager.flush();
        entityManager.clear();

        return deletedCount;
    }
}
