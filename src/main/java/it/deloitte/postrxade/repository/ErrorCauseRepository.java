package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.dto.ErrorTypeCountDTO;
import it.deloitte.postrxade.entity.ErrorCause;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;


/**
 * Repository for ErrorRecord entity.
 */
@Repository
public interface ErrorCauseRepository extends JpaRepository<ErrorCause, Long>, ErrorCauseRepositoryCustom {
    @Query("""
                SELECT COUNT(ec)
                FROM ErrorRecord er
                JOIN er.errorCauses ec
                JOIN ec.errorType et
                WHERE er.ingestion.id = :ingestionId
                AND et.severityLevel = :severity
            """)
    long countByIngestionAndSeverity(
            @Param("ingestionId") Long ingestionId,
            @Param("severity") Integer severity
    );

    @Query("""
                SELECT COUNT(ec)
                FROM ErrorCause ec
                JOIN ec.errorRecord er
                JOIN er.ingestion i
                WHERE i.submission.id IN :submissionIds
                AND ec.errorType.id = :errorTypeId
            """)
    long countBySubmissionsAndErrorType(
            @Param("submissionIds") List<Long> submissionIds,
            @Param("errorTypeId") Long errorTypeId
    );

    @Query("""
            SELECT ec
            FROM ErrorCause ec
            JOIN ec.errorRecord er
            JOIN ec.errorType et
            JOIN er.ingestion i
            JOIN i.submission s
            JOIN s.obligation o
            WHERE o.id = :obligationId
              AND ec.errorType.severityLevel = :severity
            """)
    List<ErrorCause> findErrorCauseByObligationAndSeverity(
            @Param("obligationId") Long obligationId,
            @Param("severity") Integer severity
    );

    @Query("""
            SELECT ec
            FROM ErrorCause ec
            JOIN ec.errorRecord er
            JOIN er.ingestion i
            JOIN i.submission s
            WHERE s.id = :submissionId
              AND ec.errorType.severityLevel = :severity
            """)
    List<ErrorCause> findBySubmissionIdAndSeverity(
            @Param("submissionId") Long submissionId,
            @Param("severity") Integer severity
    );

    @Query("""
            SELECT COUNT(ec)
            FROM ErrorCause ec
            JOIN ec.errorRecord er
            JOIN er.ingestion i
            JOIN i.submission s
            WHERE s.id = :submissionId
              AND ec.errorType.errorCode = :errorCode
            """)
    int findBySubmissionIdAndErrorCode(
            @Param("submissionId") Long submissionId,
            @Param("errorCode") String errorCode
    );

    @Query("""
            SELECT ec
            FROM ErrorCause ec
            JOIN ec.errorRecord er
            JOIN er.ingestion i
            JOIN i.submission s
            WHERE s.id = :submissionId
            """)
    List<ErrorCause> findBySubmissionId(@Param("submissionId") Long submissionId);

    /**
     * Counts total error causes by severity for a submission (optimized - no entity loading).
     * OPTIMIZED: Uses direct fk_submission instead of JOIN through INGESTION for better performance.
     * This replaces loading all ErrorCause entities just to count them.
     */
    @Query("""
            SELECT COUNT(DISTINCT er.id)
            FROM ErrorCause ec
            JOIN ec.errorRecord er
            JOIN ec.errorType et
            WHERE er.submission.id = :submissionId
              AND et.severityLevel = :severity
            """)
    long countDistinctErrorRecordsBySubmissionIdAndSeverity(
            @Param("submissionId") Long submissionId,
            @Param("severity") Integer severity
    );

    /**
     * Aggregates error counts grouped by ErrorType for a submission and severity.
     * OPTIMIZED: Uses direct fk_submission instead of JOIN through INGESTION for better performance.
     * Returns only aggregated data (ErrorType info + count), avoiding loading all ErrorCause entities.
     * Uses DISTINCT er.id to count unique error records (not error causes).
     */
    @Query("""
            SELECT new it.deloitte.postrxade.dto.ErrorTypeCountDTO(
                et.id,
                et.name,
                et.errorCode,
                COUNT(DISTINCT er.id)
            )
            FROM ErrorCause ec
            JOIN ec.errorRecord er
            JOIN ec.errorType et
            WHERE er.submission.id = :submissionId
              AND et.severityLevel = :severity
            GROUP BY et.id, et.name, et.errorCode
            ORDER BY COUNT(DISTINCT er.id) DESC
            """)
    List<ErrorTypeCountDTO> findErrorTypeCountsBySubmissionIdAndSeverity(
            @Param("submissionId") Long submissionId,
            @Param("severity") Integer severity
    );

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM ErrorCause ec
            WHERE ec.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);
}