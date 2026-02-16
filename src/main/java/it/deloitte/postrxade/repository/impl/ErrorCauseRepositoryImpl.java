package it.deloitte.postrxade.repository.impl;

import it.deloitte.postrxade.dto.ErrorTypeCountDTO;
import it.deloitte.postrxade.entity.ErrorCause;
import it.deloitte.postrxade.repository.ErrorCauseRepositoryCustom;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Slf4j
@Transactional(timeout = 120) // 2 minutes timeout to prevent lock wait timeout
// Nota: questa classe NON deve essere annotata con @Repository o @Component
// Spring Data JPA la userà automaticamente come implementazione di ErrorCauseRepositoryCustom
// seguendo la convenzione di naming (ErrorCauseRepository + Impl)
public class ErrorCauseRepositoryImpl implements ErrorCauseRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public void bulkInsert(List<ErrorCause> errorCauses) {
        if (errorCauses == null || errorCauses.isEmpty()) {
            return;
        }

        String sql = buildInsertSql(errorCauses.size());
        Query query = entityManager.createNativeQuery(sql);

        for (int i = 0; i < errorCauses.size(); i++) {
            setParams(query, i, errorCauses.get(i));
        }

        query.executeUpdate();
    }

    private String buildInsertSql(int batchSize) {
        StringJoiner values = new StringJoiner(", ");

        for (int i = 0; i < batchSize; i++) {
            values.add("(" +
                    ":fk_error_record_" + i + ", " +
                    ":fk_error_type_" + i + ", " +
                    ":fk_submission_" + i + ", " +
                    ":error_message_" + i +
                    ")");
        }

        return """
        INSERT INTO ERROR_CAUSE
        (fk_error_record, fk_error_type, fk_submission, error_message)
        VALUES %s
        """.formatted(values.toString());
    }


    private void setParams(Query query, int index, ErrorCause cause) {
        query.setParameter(
                "fk_error_record_" + index,
                cause.getErrorRecord().getId()
        );
        query.setParameter(
                "fk_error_type_" + index,
                cause.getErrorType().getId()
        );
        query.setParameter(
                "fk_submission_" + index,
                cause.getSubmission().getId()
        );
        query.setParameter(
                "error_message_" + index,
                cause.getErrorMessage()
        );
    }

    @Override
    @Transactional(readOnly = true, timeout = 180) // 3 minutes for read-only aggregation queries
    public long countDistinctErrorRecordsBySubmissionIdAndSeverityNative(Long submissionId, Integer severity) {
        long startTime = System.currentTimeMillis();
        log.debug("Starting countDistinctErrorRecordsBySubmissionIdAndSeverityNative for submissionId={}, severity={}", submissionId, severity);
        
        // OPTIMIZED: Use subquery to filter ERROR_TYPE first, then join with ERROR_RECORD
        // This reduces the dataset before the DISTINCT operation
        String nativeSql = """
            SELECT COUNT(DISTINCT er.pk_error_record)
            FROM ERROR_RECORD er
            INNER JOIN ERROR_CAUSE ec ON ec.fk_error_record = er.pk_error_record
            INNER JOIN (
                SELECT pk_error_type 
                FROM ERROR_TYPE 
                WHERE serverity_level = :severity
            ) et ON ec.fk_error_type = et.pk_error_type
            WHERE er.fk_submission = :submissionId
            """;
        
        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);
        query.setParameter("severity", severity);
        
        Object result = query.getSingleResult();
        long count = result instanceof Number ? ((Number) result).longValue() : 0L;
        
        long duration = System.currentTimeMillis() - startTime;
        log.info("countDistinctErrorRecordsBySubmissionIdAndSeverityNative completed in {}ms: submissionId={}, severity={}, count={}", 
                duration, submissionId, severity, count);
        
        return count;
    }

    @Override
    @Transactional(readOnly = true, timeout = 180)
    public List<ErrorTypeCountDTO> findErrorTypeCountsBySubmissionIdAndSeverityNative(Long submissionId, Integer severity) {
        String nativeSql = """
            SELECT 
                et.pk_error_type AS errorTypeId,
                et.name AS errorTypeName,
                et.error_code AS errorCode,
                COUNT(DISTINCT er.pk_error_record) AS count
            FROM ERROR_RECORD er
            INNER JOIN ERROR_CAUSE ec ON ec.fk_error_record = er.pk_error_record
            INNER JOIN (
                SELECT pk_error_type, name, error_code
                FROM ERROR_TYPE 
                WHERE serverity_level = :severity
            ) et ON ec.fk_error_type = et.pk_error_type
            WHERE er.fk_submission = :submissionId
            GROUP BY et.pk_error_type, et.name, et.error_code
            ORDER BY COUNT(DISTINCT er.pk_error_record) DESC
            """;
        
        Query query = entityManager.createNativeQuery(nativeSql);
        query.setParameter("submissionId", submissionId);
        query.setParameter("severity", severity);
        
        @SuppressWarnings("unchecked")
        List<Object[]> results = query.getResultList();
        
        List<ErrorTypeCountDTO> dtos = new ArrayList<>(results.size());
        for (Object[] row : results) {
            Long errorTypeId = ((Number) row[0]).longValue();
            String errorTypeName = (String) row[1];
            String errorCode = (String) row[2];
            Long count = ((Number) row[3]).longValue();
            
            dtos.add(new ErrorTypeCountDTO(errorTypeId, errorTypeName, errorCode, count));
        }

        return dtos;
    }

}

