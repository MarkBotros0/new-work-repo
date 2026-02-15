package it.deloitte.postrxade.repository.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import it.deloitte.postrxade.entity.CambioNdg;
import it.deloitte.postrxade.entity.Collegamenti;
import it.deloitte.postrxade.entity.DatiContabili;
import it.deloitte.postrxade.entity.Rapporti;
import it.deloitte.postrxade.entity.Soggetti;
import it.deloitte.postrxade.records.StagingResult;
import it.deloitte.postrxade.repository.StagingRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of staging repository for high-performance ETL operations.
 * Uses native SQL for maximum performance with bulk operations.
 */
@Repository
@Slf4j
public class StagingRepositoryImpl implements StagingRepository {

    @PersistenceContext
    private EntityManager entityManager;

    // Batch size for bulk inserts into staging
    private static final int STAGING_BATCH_SIZE = 5000;

    @Override
    @Transactional
    public void clearStaging(Long submissionId) {
        log.info("Clearing staging tables for submission: {}", submissionId);

        entityManager.createNativeQuery("DELETE FROM STG_SOGGETTI WHERE fk_submission = :submissionId")
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        entityManager.createNativeQuery("DELETE FROM STG_RAPPORTI WHERE fk_submission = :submissionId")
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        entityManager.createNativeQuery("DELETE FROM STG_DATI_CONTABILI WHERE fk_submission = :submissionId")
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        entityManager.createNativeQuery("DELETE FROM STG_COLLEGAMENTI WHERE fk_submission = :submissionId")
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        entityManager.createNativeQuery("DELETE FROM STG_CAMBIO_NDG WHERE fk_submission = :submissionId")
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        log.info("Staging tables cleared for submission: {}", submissionId);
    }

    @Override
    @Transactional
    public void bulkLoadSoggettiToStaging(List<Soggetti> soggettiList, Long ingestionId, Long submissionId) {
        if (soggettiList == null || soggettiList.isEmpty()) {
            return;
        }

        log.info("Loading {} Soggetti to staging for submission: {}", soggettiList.size(), submissionId);
        long startTime = System.currentTimeMillis();

        // Process in batches to avoid memory issues
        for (int i = 0; i < soggettiList.size(); i += STAGING_BATCH_SIZE) {
            int endIndex = Math.min(i + STAGING_BATCH_SIZE, soggettiList.size());
            List<Soggetti> batch = soggettiList.subList(i, endIndex);

            insertSoggettiBatchToStaging(batch, ingestionId, submissionId);

            // Clear persistence context to free memory
            entityManager.flush();
            entityManager.clear();

            if (i > 0 && i % 50000 == 0) {
                log.info("Loaded {}/{} merchants to staging", i, soggettiList.size());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loaded {} merchants to staging in {}ms", soggettiList.size(), elapsed);
    }
    private void insertSoggettiBatchToStaging(List<Soggetti> soggettiList, Long ingestionId, Long submissionId) {
        if (soggettiList.isEmpty()) return;

        StringBuilder sql = new StringBuilder();
        sql.append("""
        INSERT INTO STG_SOGGETTI (
            intermediario, ndg, data_censimento_anagrafico, data_estinzione_anagrafica, 
            filiale_censimento_anagrafico, tipo_soggetto, natura_giuridica, sesso, 
            codice_fiscale, cognome, nome, data_nascita, comune, provincia, nazione, 
            data_predisposizione_flusso, controllo_di_fine_riga, 
            fk_ingestion, fk_submission, raw_row
        ) VALUES """);

        List<String> valueRows = new ArrayList<>();
        for (Soggetti s : soggettiList) {
            valueRows.add(String.format(
                    "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s')",
                    escape(s.getIntermediario()),
                    escape(s.getNdg()),
                    escape(s.getDataCensimentoAnagrafico()),
                    escape(s.getDataEstinzioneAnagrafica()),
                    escape(s.getFilialeCensimentoAnagrafico()),
                    escape(s.getTipoSoggetto()),
                    escape(s.getNaturaGiuridica()),
                    escape(s.getSesso()),
                    escape(s.getCodiceFiscale()),
                    escape(s.getCognome()),
                    escape(s.getNome()),
                    escape(s.getDataNascita()),
                    escape(s.getComune()),
                    escape(s.getProvincia()),
                    escape(s.getNazione()),
                    escape(s.getDataPredisposizioneFlusso()),
                    escape(s.getControlloDiFineRiga()), // Added this field
                    ingestionId,
                    submissionId,
                    escape(truncate(s.getRawRow(), 1000)) // Increased truncation to match schema
            ));
        }

        sql.append(String.join(", ", valueRows));
        entityManager.createNativeQuery(sql.toString()).executeUpdate();
    }

    @Override
    @Transactional
    public void bulkLoadRapportiToStaging(List<Rapporti> rapportiList, Long ingestionId, Long submissionId) {
        if (rapportiList == null || rapportiList.isEmpty()) {
            return;
        }

        log.info("Loading {} rapporti to staging for submission: {}", rapportiList.size(), submissionId);
        long startTime = System.currentTimeMillis();

        // Process in batches to avoid memory issues
        for (int i = 0; i < rapportiList.size(); i += STAGING_BATCH_SIZE) {
            int endIndex = Math.min(i + STAGING_BATCH_SIZE, rapportiList.size());
            List<Rapporti> batch = rapportiList.subList(i, endIndex);

            insertRapportiBatchToStaging(batch, ingestionId, submissionId);

            // Clear persistence context to free memory
            entityManager.flush();
            entityManager.clear();

            if (i > 0 && i % 50000 == 0) {
                log.info("Loaded {}/{} rapporti to staging", i, rapportiList.size());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loaded {} rapporti to staging in {}ms", rapportiList.size(), elapsed);
    }

    private void insertRapportiBatchToStaging(List<Rapporti> rapportiList, Long ingestionId, Long submissionId) {
        if (rapportiList.isEmpty()) return;

        StringBuilder sql = new StringBuilder();
        sql.append("""
        INSERT INTO STG_RAPPORTI (
            intermediario, chiave_rapporto, tipo_rapporto_interno, forma_tecnica, 
            filiale, cab, numero_conto, cin, divisa, data_inizio_rapporto, 
            data_fine_rapporto, note, flag_stato_rapporto, data_predisposizione, 
            controllo_di_fine_riga, fk_ingestion, fk_submission, raw_row
        ) VALUES """);

        List<String> valueRows = new ArrayList<>();
        for (Rapporti r : rapportiList) {
            valueRows.add(String.format(
                    "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s')",
                    escape(r.getIntermediario()),
                    escape(r.getChiaveRapporto()),
                    escape(r.getTipoRapportoInterno()),
                    escape(r.getFormaTecnica()),
                    escape(r.getFiliale()),
                    escape(r.getCab()),
                    escape(r.getNumeroConto()),
                    escape(r.getCin()),
                    escape(r.getDivisa()),
                    escape(r.getDataInizioRapporto()),
                    escape(r.getDataFineRapporto()),
                    escape(r.getNote()),
                    escape(r.getFlagStatoRapporto()),
                    escape(r.getDataPredisposizione()),
                    escape(r.getControlloDiFineRiga()),
                    ingestionId,
                    submissionId,
                    escape(truncate(r.getRawRow(), 1000))
            ));
        }

        sql.append(String.join(", ", valueRows));
        entityManager.createNativeQuery(sql.toString()).executeUpdate();
    }

    @Override
    @Transactional
    public void bulkLoadDatiContabiliToStaging(List<DatiContabili> datiContabiliList, Long ingestionId, Long submissionId) {
        if (datiContabiliList == null || datiContabiliList.isEmpty()) {
            return;
        }

        log.info("Loading {} dati contabili to staging for submission: {}", datiContabiliList.size(), submissionId);
        long startTime = System.currentTimeMillis();

        // Process in batches to avoid memory issues
        for (int i = 0; i < datiContabiliList.size(); i += STAGING_BATCH_SIZE) {
            int endIndex = Math.min(i + STAGING_BATCH_SIZE, datiContabiliList.size());
            List<DatiContabili> batch = datiContabiliList.subList(i, endIndex);

            insertDatiContabiliBatchToStaging(batch, ingestionId, submissionId);

            // Clear persistence context to free memory
            entityManager.flush();
            entityManager.clear();

            if (i > 0 && i % 50000 == 0) {
                log.info("Loaded {}/{} dati contabili to staging", i, datiContabiliList.size());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loaded {} dati contabili to staging in {}ms", datiContabiliList.size(), elapsed);
    }

    private void insertDatiContabiliBatchToStaging(List<DatiContabili> datiContabiliList, Long ingestionId, Long submissionId) {
        if (datiContabiliList.isEmpty()) return;

        StringBuilder sql = new StringBuilder();
        sql.append("""
        INSERT INTO STG_DATI_CONTABILI (
            intermediario, chiave_rapporto, anno_di_riferimento, periodicita, 
            progressivo_periodicita, divisa, data_inizio_riferimento, data_fine_riferimento, 
            importo_saldo_iniziale, importo_saldo_finale, totale_operazioni_attive, 
            totale_operazioni_passive, giacenza_media, flag_soglia_saldo_iniziale, 
            flag_soglia_saldo_finale, flag_soglia_operazioni_attive, 
            flag_soglia_operazioni_passive, flag_soglia_giacenza_media, 
            altre_informazioni, flag_stato_importo, data_predisposizione, 
            tipo_rapporto_interno, forma_tecnica, flag_soglia_altre_informazioni, 
            controllo_di_fine_riga, fk_ingestion, fk_submission, raw_row
        ) VALUES """);

        List<String> valueRows = new ArrayList<>();
        for (DatiContabili d : datiContabiliList) {
            valueRows.add(String.format(
                    "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s')",
                    escape(d.getIntermediario()),
                    escape(d.getChiaveRapporto()),
                    escape(d.getAnnoDiRiferimento()),
                    escape(d.getPeriodicita()),
                    escape(d.getProgressivoPeriodicita()),
                    escape(d.getDivisa()),
                    escape(d.getDataInizioRiferimento()),
                    escape(d.getDataFineRiferimento()),
                    escape(d.getImportoSaldoIniziale()),
                    escape(d.getImportoSaldoFinale()),
                    escape(d.getTotaleOperazioniAttive()),
                    escape(d.getTotaleOperazioniPassive()),
                    escape(d.getGiacenzaMedia()),
                    escape(d.getFlagSogliaSaldoIniziale()),
                    escape(d.getFlagSogliaSaldoFinale()),
                    escape(d.getFlagSogliaOperazioniAttive()),
                    escape(d.getFlagSogliaOperazioniPassive()),
                    escape(d.getFlagSogliaGiacenzaMedia()),
                    escape(d.getAltreInformazioni()),
                    escape(d.getFlagStatoImporto()),
                    escape(d.getDataPredisposizione()),
                    escape(d.getTipoRapportoInterno()),
                    escape(d.getFormaTecnica()),
                    escape(d.getFlagSogliaAltreInformazioni()),
                    escape(d.getControlloDiFineRiga()),
                    ingestionId,
                    submissionId,
                    escape(truncate(d.getRawRow(), 1000))
            ));
        }

        sql.append(String.join(", ", valueRows));
        entityManager.createNativeQuery(sql.toString()).executeUpdate();
    }

    @Override
    @Transactional
    public void bulkLoadCollegamentiToStaging(List<Collegamenti> collegamentiList, Long ingestionId, Long submissionId) {
        if (collegamentiList == null || collegamentiList.isEmpty()) {
            return;
        }

        log.info("Loading {} collegamenti to staging for submission: {}", collegamentiList.size(), submissionId);
        long startTime = System.currentTimeMillis();

        // Process in batches to avoid memory issues
        for (int i = 0; i < collegamentiList.size(); i += STAGING_BATCH_SIZE) {
            int endIndex = Math.min(i + STAGING_BATCH_SIZE, collegamentiList.size());
            List<Collegamenti> batch = collegamentiList.subList(i, endIndex);

            insertCollegamentiBatchToStaging(batch, ingestionId, submissionId);

            // Clear persistence context to free memory
            entityManager.flush();
            entityManager.clear();

            if (i > 0 && i % 50000 == 0) {
                log.info("Loaded {}/{} collegamenti to staging", i, collegamentiList.size());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loaded {} collegamenti to staging in {}ms", collegamentiList.size(), elapsed);
    }
    private void insertCollegamentiBatchToStaging(List<Collegamenti> collegamentiList, Long ingestionId, Long submissionId) {
        if (collegamentiList.isEmpty()) return;

        StringBuilder sql = new StringBuilder();
        sql.append("""
        INSERT INTO STG_COLLEGAMENTI (
            intermediario, chiave_rapporto, ndg, ruolo, 
            data_inizio_collegamento, data_fine_collegamento, ruolo_interno, 
            flag_stato_collegamento, data_predisposizione_flusso, controllo_di_fine_riga, 
            fk_ingestion, fk_submission, raw_row
        ) VALUES """);

        List<String> valueRows = new ArrayList<>();
        for (Collegamenti c : collegamentiList) {
            valueRows.add(String.format(
                    "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s')",
                    escape(c.getIntermediario()),
                    escape(c.getChiaveRapporto()),
                    escape(c.getNdg()),
                    escape(c.getRuolo()),
                    escape(c.getDataInizioCollegamento()),
                    escape(c.getDataFineCollegamento()),
                    escape(c.getRuoloInterno()),
                    escape(c.getFlagStatoCollegamento()),
                    escape(c.getDataPredisposizioneFlusso()),
                    escape(c.getControlloDiFineRiga()),
                    ingestionId,
                    submissionId,
                    escape(truncate(c.getRawRow(), 1000))
            ));
        }

        sql.append(String.join(", ", valueRows));
        entityManager.createNativeQuery(sql.toString()).executeUpdate();
    }

    @Override
    @Transactional
    public void bulkLoadCambioNdgToStaging(List<CambioNdg> cambioNdgList, Long ingestionId, Long submissionId) {
        if (cambioNdgList == null || cambioNdgList.isEmpty()) {
            return;
        }

        log.info("Loading {} cambio ndg to staging for submission: {}", cambioNdgList.size(), submissionId);
        long startTime = System.currentTimeMillis();

        // Process in batches to avoid memory issues
        for (int i = 0; i < cambioNdgList.size(); i += STAGING_BATCH_SIZE) {
            int endIndex = Math.min(i + STAGING_BATCH_SIZE, cambioNdgList.size());
            List<CambioNdg> batch = cambioNdgList.subList(i, endIndex);

            insertCambioNdgBatchToStaging(batch, ingestionId, submissionId);

            // Clear persistence context to free memory
            entityManager.flush();
            entityManager.clear();

            if (i > 0 && i % 50000 == 0) {
                log.info("Loaded {}/{} cambio ndg to staging", i, cambioNdgList.size());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Loaded {} cambio ndg to staging in {}ms", cambioNdgList.size(), elapsed);
    }

    private void insertCambioNdgBatchToStaging(List<CambioNdg> cambioNdgList, Long ingestionId, Long submissionId) {
        if (cambioNdgList.isEmpty()) return;

        StringBuilder sql = new StringBuilder();
        sql.append("""
        INSERT INTO STG_CAMBIO_NDG (
            intermediario, ndg_vecchio, ndg_nuovo, controllo_di_fine_riga, 
            fk_ingestion, fk_submission, raw_row
        ) VALUES """);

        List<String> valueRows = new ArrayList<>();
        for (CambioNdg cn : cambioNdgList) {
            valueRows.add(String.format(
                    "('%s', '%s', '%s', '%s', %d, %d, '%s')",
                    escape(cn.getIntermediario()),
                    escape(cn.getNdgVecchio()),
                    escape(cn.getNdgNuovo()),
                    escape(cn.getControlloDiFineRiga()),
                    ingestionId,
                    submissionId,
                    escape(truncate(cn.getRawRow(), 1000))
            ));
        }

        sql.append(String.join(", ", valueRows));
        entityManager.createNativeQuery(sql.toString()).executeUpdate();
    }

    @Override
    @Transactional
    public StagingResult processSoggettiFromStaging(Long submissionId) {
        log.info("Processing soggetti from staging for submission: {}", submissionId);
        long startTime = System.currentTimeMillis();

        // Step 1: Mark duplicates existing in production (using COLLATE fix)
        int existingDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_SOGGETTI stg
                    INNER JOIN MERCHANT_SOGGETTI ms
                        ON stg.intermediario COLLATE utf8mb4_0900_ai_ci = ms.intermediario COLLATE utf8mb4_0900_ai_ci
                        AND stg.ndg COLLATE utf8mb4_0900_ai_ci = ms.ndg COLLATE utf8mb4_0900_ai_ci
                        AND stg.fk_submission = ms.fk_submission
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate: soggetto already exists'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 2: Mark duplicates within the staging batch (keeping only the first record by PK)
        int batchDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_SOGGETTI stg
                    INNER JOIN (
                        SELECT intermediario, ndg, MIN(pk_stg_merchant) as first_pk
                        FROM STG_SOGGETTI
                        WHERE fk_submission = :submissionId AND process_status IS NULL
                        GROUP BY intermediario, ndg
                        HAVING COUNT(*) > 1
                    ) dups ON stg.intermediario = dups.intermediario
                          AND stg.ndg = dups.ndg
                          AND stg.pk_stg_merchant > dups.first_pk
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate within submission'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 3: CRITICAL - Mark records with missing Collegamenti parent (FK validation)
        // This MUST happen BEFORE the INSERT to prevent FK constraint violations
        int missingParents = entityManager.createNativeQuery("""
                    UPDATE STG_SOGGETTI stg
                    LEFT JOIN MERCHANT_COLLEGAMENTI c 
                        ON stg.ndg COLLATE utf8mb4_0900_ai_ci = c.ndg COLLATE utf8mb4_0900_ai_ci
                        AND stg.fk_submission = c.fk_submission
                    SET stg.process_status = 3,
                        stg.error_message = CONCAT('Missing Collegamenti parent for ndg: ', stg.ndg)
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                      AND c.pk_collegamenti IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        int totalDuplicates = existingDuplicates + batchDuplicates;
        
        log.info("Soggetti validation: duplicates={}, missingParents={}", totalDuplicates, missingParents);

        // Step 3: Insert new records (Synchronized with Soggetti entity fields)
        int insertedCount = entityManager.createNativeQuery("""
                    INSERT INTO MERCHANT_SOGGETTI (
                        fk_ingestion, fk_submission, intermediario, ndg, 
                        data_censimento_anagrafico, data_estinzione_anagrafica, 
                        filiale_censimento_anagrafico, tipo_soggetto, natura_giuridica, 
                        sesso, codice_fiscale, cognome, nome, data_nascita, 
                        comune, provincia, nazione, data_predisposizione_flusso, 
                        controllo_di_fine_riga, created_at
                    )
                    SELECT 
                        fk_ingestion, fk_submission, intermediario, ndg, 
                        data_censimento_anagrafico, data_estinzione_anagrafica, 
                        filiale_censimento_anagrafico, tipo_soggetto, natura_giuridica, 
                        sesso, codice_fiscale, cognome, nome, data_nascita, 
                        comune, provincia, nazione, data_predisposizione_flusso, 
                        controllo_di_fine_riga, CURRENT_TIMESTAMP
                    FROM STG_SOGGETTI
                    WHERE fk_submission = :submissionId
                      AND process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 4: Finalize - Mark as success in staging
        entityManager.createNativeQuery("""
                    UPDATE STG_SOGGETTI
                    SET process_status = 1
                    WHERE fk_submission = :submissionId
                      AND process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Processed soggetti from staging in {}ms: inserted={}, duplicates={}, missingParents={}",
                elapsed, insertedCount, totalDuplicates, missingParents);

        return new StagingResult(insertedCount, totalDuplicates, missingParents, 0);
    }

    @Override
    @Transactional
    public StagingResult processRapportiFromStaging(Long submissionId) {
        log.info("Processing rapporti from staging for submission: {}", submissionId);
        long startTime = System.currentTimeMillis();

        // Step 1: Mark duplicates (existing in MERCHANT_RAPPORTI)
        // We use COLLATE to prevent the "Illegal mix of collations" error
        int existingDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_RAPPORTI stg
                    INNER JOIN MERCHANT_RAPPORTI mr
                        ON stg.intermediario COLLATE utf8mb4_0900_ai_ci = mr.intermediario COLLATE utf8mb4_0900_ai_ci
                        AND stg.chiave_rapporto COLLATE utf8mb4_0900_ai_ci = mr.chiave_rapporto COLLATE utf8mb4_0900_ai_ci
                        AND stg.fk_submission = mr.fk_submission
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate: rapporto already exists'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 2: Mark duplicates within the same staging batch (keep the first record)
        int batchDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_RAPPORTI stg
                    INNER JOIN (
                        SELECT intermediario, chiave_rapporto, MIN(pk_stg_rapporti) as first_pk
                        FROM STG_RAPPORTI
                        WHERE fk_submission = :submissionId AND process_status IS NULL
                        GROUP BY intermediario, chiave_rapporto
                        HAVING COUNT(*) > 1
                    ) dups ON stg.intermediario = dups.intermediario
                          AND stg.chiave_rapporto = dups.chiave_rapporto
                          AND stg.pk_stg_rapporti > dups.first_pk
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate within submission'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 3: CRITICAL - Mark records with missing Collegamenti parent (FK validation)
        // This MUST happen BEFORE the INSERT to prevent FK constraint violations
        int missingParents = entityManager.createNativeQuery("""
                    UPDATE STG_RAPPORTI stg
                    LEFT JOIN MERCHANT_COLLEGAMENTI c 
                        ON stg.chiave_rapporto COLLATE utf8mb4_0900_ai_ci = c.chiave_rapporto COLLATE utf8mb4_0900_ai_ci
                        AND stg.fk_submission = c.fk_submission
                    SET stg.process_status = 3,
                        stg.error_message = CONCAT('Missing Collegamenti parent for chiave_rapporto: ', stg.chiave_rapporto)
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                      AND c.pk_collegamenti IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        int totalDuplicates = existingDuplicates + batchDuplicates;
        
        log.info("Rapporti validation: duplicates={}, missingParents={}", totalDuplicates, missingParents);

        // Step 3: Insert new records with ALL columns from the entity
        int insertedCount = entityManager.createNativeQuery("""
                    INSERT INTO MERCHANT_RAPPORTI (
                        fk_ingestion, fk_submission, intermediario, chiave_rapporto, 
                        tipo_rapporto_interno, forma_tecnica, filiale, cab, 
                        numero_conto, cin, divisa, data_inizio_rapporto, 
                        data_fine_rapporto, note, flag_stato_rapporto, 
                        data_predisposizione, controllo_di_fine_riga, created_at
                    )
                    SELECT 
                        fk_ingestion, fk_submission, intermediario, chiave_rapporto, 
                        tipo_rapporto_interno, forma_tecnica, filiale, cab, 
                        numero_conto, cin, divisa, data_inizio_rapporto, 
                        data_fine_rapporto, note, flag_stato_rapporto, 
                        data_predisposizione, controllo_di_fine_riga, CURRENT_TIMESTAMP
                    FROM STG_RAPPORTI
                    WHERE fk_submission = :submissionId
                      AND process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 4: Finalize - Mark staging records as successfully inserted
        entityManager.createNativeQuery("""
                UPDATE STG_RAPPORTI 
                SET process_status = 1 
                WHERE fk_submission = :submissionId 
                  AND process_status IS NULL
                """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Processed rapporti in {}ms: inserted={}, duplicates={}, missingParents={}", 
                elapsed, insertedCount, totalDuplicates, missingParents);

        return new StagingResult(insertedCount, totalDuplicates, missingParents, 0);
    }

    @Override
    @Transactional
    public StagingResult processDatiContabiliFromStaging(Long submissionId) {
        log.info("Processing dati contabili from staging for submission: {}", submissionId);
        long startTime = System.currentTimeMillis();

        // Step 1: Global Dups (Composite Key with Collation Fix)
        int existingDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_DATI_CONTABILI stg
                    INNER JOIN MERCHANT_DATI_CONTABILI mdc
                        ON stg.intermediario COLLATE utf8mb4_0900_ai_ci = mdc.intermediario COLLATE utf8mb4_0900_ai_ci
                        AND stg.chiave_rapporto COLLATE utf8mb4_0900_ai_ci = mdc.chiave_rapporto COLLATE utf8mb4_0900_ai_ci
                        AND stg.anno_di_riferimento = mdc.anno_di_riferimento
                        AND stg.periodicita = mdc.periodicita
                        AND stg.progressivo_periodicita = mdc.progressivo_periodicita
                        AND stg.fk_submission = mdc.fk_submission
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate: record already exists'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 2: Batch Dups
        int batchDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_DATI_CONTABILI stg
                    INNER JOIN (
                        SELECT intermediario, chiave_rapporto, anno_di_riferimento, periodicita, progressivo_periodicita, MIN(pk_stg_dati_contabili) as first_pk
                        FROM STG_DATI_CONTABILI
                        WHERE fk_submission = :submissionId AND process_status IS NULL
                        GROUP BY intermediario, chiave_rapporto, anno_di_riferimento, periodicita, progressivo_periodicita
                    ) dups ON stg.intermediario = dups.intermediario 
                          AND stg.chiave_rapporto = dups.chiave_rapporto 
                          AND stg.anno_di_riferimento = dups.anno_di_riferimento 
                          AND stg.periodicita = dups.periodicita 
                          AND stg.progressivo_periodicita = dups.progressivo_periodicita 
                          AND stg.pk_stg_dati_contabili > dups.first_pk
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate within submission'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 3: CRITICAL - Mark records with missing Collegamenti parent (FK validation)
        // This MUST happen BEFORE the INSERT to prevent FK constraint violations
        int missingParents = entityManager.createNativeQuery("""
                    UPDATE STG_DATI_CONTABILI stg
                    LEFT JOIN MERCHANT_COLLEGAMENTI c 
                        ON stg.chiave_rapporto COLLATE utf8mb4_0900_ai_ci = c.chiave_rapporto COLLATE utf8mb4_0900_ai_ci
                        AND stg.fk_submission = c.fk_submission
                    SET stg.process_status = 3,
                        stg.error_message = CONCAT('Missing Collegamenti parent for chiave_rapporto: ', stg.chiave_rapporto)
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                      AND c.pk_collegamenti IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        int totalDuplicates = existingDuplicates + batchDuplicates;
        
        log.info("DatiContabili validation: duplicates={}, missingParents={}", totalDuplicates, missingParents);

        // Step 3: Insert (Fully mapping all 25+ fields from the Entity)
        int insertedCount = entityManager.createNativeQuery("""
                    INSERT INTO MERCHANT_DATI_CONTABILI (
                        fk_ingestion, fk_submission, intermediario, chiave_rapporto, 
                        anno_di_riferimento, periodicita, progressivo_periodicita, divisa,
                        data_inizio_riferimento, data_fine_riferimento, importo_saldo_iniziale,
                        importo_saldo_finale, totale_operazioni_attive, totale_operazioni_passive,
                        giacenza_media, flag_soglia_saldo_iniziale, flag_soglia_saldo_finale,
                        flag_soglia_operazioni_attive, flag_soglia_operazioni_passive,
                        flag_soglia_giacenza_media, altre_informazioni, flag_stato_importo,
                        data_predisposizione, tipo_rapporto_interno, forma_tecnica,
                        flag_soglia_altre_informazioni, controllo_di_fine_riga, created_at
                    )
                    SELECT 
                        fk_ingestion, fk_submission, intermediario, chiave_rapporto, 
                        anno_di_riferimento, periodicita, progressivo_periodicita, divisa,
                        data_inizio_riferimento, data_fine_riferimento, importo_saldo_iniziale,
                        importo_saldo_finale, totale_operazioni_attive, totale_operazioni_passive,
                        giacenza_media, flag_soglia_saldo_iniziale, flag_soglia_saldo_finale,
                        flag_soglia_operazioni_attive, flag_soglia_operazioni_passive,
                        flag_soglia_giacenza_media, altre_informazioni, flag_stato_importo,
                        data_predisposizione, tipo_rapporto_interno, forma_tecnica,
                        flag_soglia_altre_informazioni, controllo_di_fine_riga, CURRENT_TIMESTAMP
                    FROM STG_DATI_CONTABILI
                    WHERE fk_submission = :submissionId AND process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 4: Finalize status
        entityManager.createNativeQuery("""
                UPDATE STG_DATI_CONTABILI 
                SET process_status = 1 
                WHERE fk_submission = :submissionId 
                  AND process_status IS NULL
                """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Processed dati contabili in {}ms: inserted={}, duplicates={}, missingParents={}", 
                elapsed, insertedCount, totalDuplicates, missingParents);
        return new StagingResult(insertedCount, totalDuplicates, missingParents, 0);
    }

    @Override
    @Transactional
    public StagingResult processCollegamentiFromStaging(Long submissionId) {
        log.info("Processing collegamenti from staging for submission: {}", submissionId);
        long startTime = System.currentTimeMillis();

        // Disable foreign key checks during insertion
        entityManager.createNativeQuery("SET foreign_key_checks = 0").executeUpdate();

        try {
            // Step 1: Mark duplicates that already exist in MERCHANT_COLLEGAMENTI
            // Check both unique constraints: (ndg, fk_submission) AND (chiave_rapporto, fk_submission)
            int existingDuplicates = entityManager.createNativeQuery("""
                        UPDATE STG_COLLEGAMENTI stg
                        INNER JOIN MERCHANT_COLLEGAMENTI mc
                            ON ((stg.ndg COLLATE utf8mb4_0900_ai_ci = mc.ndg COLLATE utf8mb4_0900_ai_ci
                                 AND stg.fk_submission = mc.fk_submission)
                                OR
                                (stg.chiave_rapporto COLLATE utf8mb4_0900_ai_ci = mc.chiave_rapporto COLLATE utf8mb4_0900_ai_ci
                                 AND stg.fk_submission = mc.fk_submission))
                        SET stg.process_status = 2,
                            stg.error_message = 'Duplicate: collegamento already exists in database'
                        WHERE stg.fk_submission = :submissionId
                          AND stg.process_status IS NULL
                        """)
                    .setParameter("submissionId", submissionId)
                    .executeUpdate();

            // Step 2a: Mark duplicates within batch based on (ndg, fk_submission)
            int batchDuplicatesNdg = entityManager.createNativeQuery("""
                        UPDATE STG_COLLEGAMENTI stg
                        INNER JOIN (
                            SELECT ndg, fk_submission, MIN(pk_stg_collegamenti) as first_pk
                            FROM STG_COLLEGAMENTI
                            WHERE fk_submission = :submissionId AND process_status IS NULL
                            GROUP BY ndg, fk_submission
                            HAVING COUNT(*) > 1
                        ) dups ON stg.ndg = dups.ndg 
                              AND stg.fk_submission = dups.fk_submission
                              AND stg.pk_stg_collegamenti > dups.first_pk
                        SET stg.process_status = 2,
                            stg.error_message = 'Duplicate within submission: same ndg'
                        WHERE stg.fk_submission = :submissionId
                          AND stg.process_status IS NULL
                        """)
                    .setParameter("submissionId", submissionId)
                    .executeUpdate();

            // Step 2b: Mark duplicates within batch based on (chiave_rapporto, fk_submission)
            int batchDuplicatesChiave = entityManager.createNativeQuery("""
                        UPDATE STG_COLLEGAMENTI stg
                        INNER JOIN (
                            SELECT chiave_rapporto, fk_submission, MIN(pk_stg_collegamenti) as first_pk
                            FROM STG_COLLEGAMENTI
                            WHERE fk_submission = :submissionId AND process_status IS NULL
                            GROUP BY chiave_rapporto, fk_submission
                            HAVING COUNT(*) > 1
                        ) dups ON stg.chiave_rapporto = dups.chiave_rapporto 
                              AND stg.fk_submission = dups.fk_submission
                              AND stg.pk_stg_collegamenti > dups.first_pk
                        SET stg.process_status = 2,
                            stg.error_message = 'Duplicate within submission: same chiave_rapporto'
                        WHERE stg.fk_submission = :submissionId
                          AND stg.process_status IS NULL
                        """)
                    .setParameter("submissionId", submissionId)
                    .executeUpdate();

            int totalDuplicates = existingDuplicates + batchDuplicatesNdg + batchDuplicatesChiave;
            
            log.info("Collegamenti duplicate detection: existing={}, batch_ndg={}, batch_chiave={}, total={}",
                    existingDuplicates, batchDuplicatesNdg, batchDuplicatesChiave, totalDuplicates);

            // Step 3: Insert all non-duplicate records (no FK validation for collegamenti)
            // Collegamenti is the base entity, other entities will be validated against it
            int insertedCount = entityManager.createNativeQuery("""
                        INSERT INTO MERCHANT_COLLEGAMENTI (
                            fk_ingestion, fk_submission, intermediario, chiave_rapporto, 
                            ndg, ruolo, data_inizio_collegamento, data_fine_collegamento, 
                            ruolo_interno, flag_stato_collegamento, data_predisposizione_flusso, 
                            controllo_di_fine_riga, created_at
                        )
                        SELECT 
                            fk_ingestion, fk_submission, intermediario, chiave_rapporto, 
                            ndg, ruolo, data_inizio_collegamento, data_fine_collegamento, 
                            ruolo_interno, flag_stato_collegamento, data_predisposizione_flusso, 
                            controllo_di_fine_riga, CURRENT_TIMESTAMP
                        FROM STG_COLLEGAMENTI
                        WHERE fk_submission = :submissionId 
                          AND process_status IS NULL
                        """)
                    .setParameter("submissionId", submissionId)
                    .executeUpdate();

            // Count statistics for logging
            Long validRecordsCount = (Long) entityManager.createNativeQuery("""
                        SELECT COUNT(*) FROM STG_COLLEGAMENTI
                        WHERE fk_submission = :submissionId AND process_status IS NULL
                        """)
                    .setParameter("submissionId", submissionId)
                    .getSingleResult();

            Long duplicateRecordsCount = (Long) entityManager.createNativeQuery("""
                        SELECT COUNT(*) FROM STG_COLLEGAMENTI
                        WHERE fk_submission = :submissionId AND process_status = 2
                        """)
                    .setParameter("submissionId", submissionId)
                    .getSingleResult();

            log.info("Staging statistics for submission {}: inserted={}, duplicates={}",
                    submissionId, insertedCount, totalDuplicates);

            // Step 4: Finalize
            entityManager.createNativeQuery("""
                    UPDATE STG_COLLEGAMENTI 
                    SET process_status = 1 
                    WHERE fk_submission = :submissionId 
                      AND process_status IS NULL
                    """)
                    .setParameter("submissionId", submissionId)
                    .executeUpdate();

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("Processed collegamenti in {}ms: inserted={}, duplicates={}", elapsed, insertedCount, totalDuplicates);
            return new StagingResult(insertedCount, totalDuplicates);
        } finally {
            // Re-enable foreign key checks
            entityManager.createNativeQuery("SET foreign_key_checks = 1").executeUpdate();
        }
    }

    @Override
    @Transactional
    public StagingResult processCambioNdgFromStaging(Long submissionId) {
        log.info("Processing cambio NDG from staging for submission: {}", submissionId);
        long startTime = System.currentTimeMillis();

        // Step 1: Global Dups (with Collation Fix)
        int existingDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_CAMBIO_NDG stg
                    INNER JOIN MERCHANT_CAMBIO_NDG mcn
                        ON stg.intermediario COLLATE utf8mb4_0900_ai_ci = mcn.intermediario COLLATE utf8mb4_0900_ai_ci
                        AND stg.ndg_vecchio COLLATE utf8mb4_0900_ai_ci = mcn.ndg_vecchio COLLATE utf8mb4_0900_ai_ci
                        AND stg.ndg_nuovo COLLATE utf8mb4_0900_ai_ci = mcn.ndg_nuovo COLLATE utf8mb4_0900_ai_ci
                        AND stg.fk_submission = mcn.fk_submission
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate: change already exists'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 2: Batch Dups (duplicates within the same submission batch)
        int batchDuplicates = entityManager.createNativeQuery("""
                    UPDATE STG_CAMBIO_NDG stg
                    INNER JOIN (
                        SELECT intermediario, ndg_vecchio, ndg_nuovo, MIN(pk_stg_cambio_ndg) as first_pk
                        FROM STG_CAMBIO_NDG
                        WHERE fk_submission = :submissionId AND process_status IS NULL
                        GROUP BY intermediario, ndg_vecchio, ndg_nuovo
                        HAVING COUNT(*) > 1
                    ) dups ON stg.intermediario = dups.intermediario 
                          AND stg.ndg_vecchio = dups.ndg_vecchio 
                          AND stg.ndg_nuovo = dups.ndg_nuovo 
                          AND stg.pk_stg_cambio_ndg > dups.first_pk
                    SET stg.process_status = 2,
                        stg.error_message = 'Duplicate within submission'
                    WHERE stg.fk_submission = :submissionId
                      AND stg.process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        int totalDuplicates = existingDuplicates + batchDuplicates;

        // Step 3: Insert into Production (Including all entity fields)
        int insertedCount = entityManager.createNativeQuery("""
                    INSERT INTO MERCHANT_CAMBIO_NDG (
                        fk_ingestion, fk_submission, intermediario, 
                        ndg_vecchio, ndg_nuovo, controllo_di_fine_riga, created_at
                    )
                    SELECT 
                        fk_ingestion, fk_submission, intermediario, 
                        ndg_vecchio, ndg_nuovo, controllo_di_fine_riga, CURRENT_TIMESTAMP
                    FROM STG_CAMBIO_NDG
                    WHERE fk_submission = :submissionId 
                      AND process_status IS NULL
                    """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        // Step 4: Finalize Staging Status
        entityManager.createNativeQuery("""
                UPDATE STG_CAMBIO_NDG 
                SET process_status = 1 
                WHERE fk_submission = :submissionId 
                  AND process_status IS NULL
                """)
                .setParameter("submissionId", submissionId)
                .executeUpdate();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Processed cambio NDG in {}ms: inserted={}, duplicates={}", elapsed, insertedCount, totalDuplicates);
        return new StagingResult(insertedCount, totalDuplicates);
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getDuplicateSoggettiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                        SELECT raw_row, error_message FROM STG_SOGGETTI
                        WHERE fk_submission = :submissionId AND process_status = 2
                        """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }


    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getDuplicateRapportiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                        SELECT raw_row, error_message FROM STG_RAPPORTI
                        WHERE fk_submission = :submissionId AND process_status = 2
                        """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getDuplicateDatiContabiliDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                        SELECT raw_row, error_message FROM STG_DATI_CONTABILI
                        WHERE fk_submission = :submissionId AND process_status = 2
                        """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getDuplicateCollegamentiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                        SELECT raw_row, error_message FROM STG_COLLEGAMENTI
                        WHERE fk_submission = :submissionId AND process_status = 2
                        """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getMissingReferenceCollegamentiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                        SELECT raw_row, error_message FROM STG_COLLEGAMENTI
                        WHERE fk_submission = :submissionId AND process_status = 3
                        """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getMissingSoggettiCollegamentiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                SELECT raw_row, error_message
                FROM STG_SOGGETTI
                WHERE fk_submission = :submissionId
                  AND process_status = 3
                """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getMissingRapportiCollegamentiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                SELECT raw_row, error_message
                FROM STG_RAPPORTI
                WHERE fk_submission = :submissionId
                  AND process_status = 3
                """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getMissingDatiContabiliCollegamentiDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                SELECT raw_row, error_message
                FROM STG_DATI_CONTABILI
                WHERE fk_submission = :submissionId
                  AND process_status = 3
                """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    @Override
    @Transactional(readOnly = true)
    public List<Object[]> getDuplicateCambioNdgDetails(Long submissionId) {
        @SuppressWarnings("unchecked")
        List<Object[]> results = entityManager.createNativeQuery("""
                        SELECT raw_row, error_message FROM STG_CAMBIO_NDG
                        WHERE fk_submission = :submissionId AND process_status = 2
                        """)
                .setParameter("submissionId", submissionId)
                .getResultList();
        return results != null ? results : new ArrayList<>();
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("'", "''").replace("\\", "\\\\");
    }

    private String truncate(String s, int maxLength) {
        if (s == null) return "";
        return s.length() > maxLength ? s.substring(0, maxLength) : s;
    }
}
