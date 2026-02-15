package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.dto.ErrorTypeCountDTO;
import it.deloitte.postrxade.entity.ErrorCause;

import java.util.List;

public interface ErrorCauseRepositoryCustom {
    void bulkInsert(List<ErrorCause> errorCauses);
    
    /**
     * Optimized native SQL query to count distinct error records by submission and severity.
     * Much faster than JPQL for large datasets.
     */
    long countDistinctErrorRecordsBySubmissionIdAndSeverityNative(Long submissionId, Integer severity);
    
    /**
     * Optimized native SQL query to get error type counts grouped by submission and severity.
     * Much faster than JPQL for large datasets.
     */
    List<ErrorTypeCountDTO> findErrorTypeCountsBySubmissionIdAndSeverityNative(Long submissionId, Integer severity);
}

