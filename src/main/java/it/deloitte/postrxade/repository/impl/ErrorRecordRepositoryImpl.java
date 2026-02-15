package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.entity.ErrorCause;
import it.deloitte.postrxade.entity.ErrorRecord;
import it.deloitte.postrxade.repository.ErrorCauseRepository;
import it.deloitte.postrxade.repository.ErrorRecordRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Repository
@Transactional(timeout = 120) // 2 minutes timeout to prevent lock wait timeout
public class ErrorRecordRepositoryImpl implements ErrorRecordRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private ErrorCauseRepository errorCauseRepository;

    @Override
    public void bulkInsert(List<ErrorRecord> errorRecords) {
        if (errorRecords == null || errorRecords.isEmpty()) {
            return;
        }

        String sql = buildInsertSql(errorRecords.size());
        Query query = entityManager.createNativeQuery(sql);

        for (int i = 0; i < errorRecords.size(); i++) {
            setParams(query, i, errorRecords.get(i));
        }

        query.executeUpdate();
    }

    private String buildInsertSql(int batchSize) {
        StringJoiner values = new StringJoiner(", ");
        for (int i = 0; i < batchSize; i++) {
            values.add("( :fk_ingestion_" + i
                    + ", :fk_submission_" + i
                    + ", :raw_row_" + i
                    + ", CURRENT_TIMESTAMP)");
        }

        return String.format(
                "INSERT INTO ERROR_RECORD (fk_ingestion, fk_submission, raw_row, created_at) VALUES %s",
                values);
    }

    private void setParams(Query query, int index, ErrorRecord errorRecord) {
        query.setParameter("fk_ingestion_" + index,
                errorRecord.getIngestion() != null ? errorRecord.getIngestion().getId() : null);
        query.setParameter("fk_submission_" + index,
                errorRecord.getSubmission() != null ? errorRecord.getSubmission().getId() : null);
        query.setParameter("raw_row_" + index, errorRecord.getRawRow());
    }

    @Override
    @Transactional
    public void bulkInsertRecordsWithCauses(List<ErrorRecord> records, Long ingestionId) {
        if (records == null || records.isEmpty()) {
            return;
        }
        
        // OPTIMIZED: Process in smaller batches to avoid slow SELECT with large IN clauses
        // Batch size of 1000 is optimal for MySQL IN clause performance
        final int BATCH_SIZE = 1000;
        
        for (int i = 0; i < records.size(); i += BATCH_SIZE) {
            int endIndex = Math.min(i + BATCH_SIZE, records.size());
            List<ErrorRecord> batch = records.subList(i, endIndex);
            
            // Insert ErrorRecords for this batch
            bulkInsert(batch);
            
            // Fetch IDs for this batch (smaller IN clause = faster)
            Map<String, ErrorRecord> recordIds = fetchInsertedErrorRecords(ingestionId, batch);
            
            // Collect ErrorCauses for this batch
            List<ErrorCause> causes = new ArrayList<>();
            for (ErrorRecord record : batch) {
                ErrorRecord errorRecord = recordIds.get(record.getRawRow());
                if (errorRecord != null && record.getErrorCauses() != null) {
                    for (ErrorCause cause : record.getErrorCauses()) {
                        cause.setErrorRecord(errorRecord);
                        causes.add(cause);
                    }
                }
            }
            
            // Insert ErrorCauses for this batch
            if (!causes.isEmpty()) {
                errorCauseRepository.bulkInsert(causes);
            }
            
            // Flush to ensure data is persisted before next batch
            entityManager.flush();
            entityManager.clear();
        }
    }

    public Map<String, ErrorRecord> fetchInsertedErrorRecords(
            Long ingestionId,
            List<ErrorRecord> records) {

        List<String> rawRows = records.stream()
                .map(ErrorRecord::getRawRow)
                .toList();

        Query query = entityManager.createNativeQuery("""
                SELECT pk_error_record, raw_row
                FROM ERROR_RECORD
                WHERE fk_ingestion = :ingestionId
                  AND raw_row IN (:rawRows)
                """);

        query.setParameter("ingestionId", ingestionId);
        query.setParameter("rawRows", rawRows);

        @SuppressWarnings("unchecked")
        List<Object[]> result = query.getResultList();

        Map<String, ErrorRecord> map = new HashMap<>(result.size());

        for (Object[] row : result) {
            // pk_error_record is BIGINT (primary key), but be defensive with JDBC return types
            Long id = (row[0] instanceof Number) ? ((Number) row[0]).longValue() : null;
            if (id == null) {
                throw new IllegalStateException("Expected Number for pk_error_record, got: " + 
                    (row[0] != null ? row[0].getClass().getName() : "null"));
            }
            // Be defensive: depending on column type/driver this can be String, Clob, etc.
            String rawRow = String.valueOf(row[1]);

            ErrorRecord recordRef =
                    entityManager.getReference(ErrorRecord.class, id);

            map.put(rawRow, recordRef);
        }

        return map;
    }


}

