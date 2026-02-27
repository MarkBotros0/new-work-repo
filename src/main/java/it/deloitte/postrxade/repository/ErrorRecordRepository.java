package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.ErrorCause;
import it.deloitte.postrxade.entity.ErrorRecord;
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
public interface ErrorRecordRepository extends JpaRepository<ErrorRecord, Long>, ErrorRecordRepositoryCustom {
//    long countByIngestionIdAndErrorTypeSeverityLevel(Long ingestionId, int severityLevel);

    long countByIngestionIdAndIngestionIngestionTypeName(Long ingestionId, String ingestionTypeId);
    long countBySubmissionId(Long submissionId);

    @Query("""
            SELECT COUNT(er)
            FROM ErrorRecord er
            JOIN er.ingestion i
            JOIN i.ingestionType it
            JOIN i.submission s
            JOIN s.obligation o
            WHERE o.id = :obligationId
            AND it.name = :ingestionTypeName
            """)
    long countErrorRecordsByObligationAndIngestionType(@Param("obligationId") Long obligationId, @Param("ingestionTypeName") String ingestionTypeName);


    @Query("""
            SELECT COUNT(er)
            FROM ErrorRecord er
            JOIN er.ingestion i
            JOIN i.ingestionType it
            JOIN i.submission s
            WHERE s.id = :submissionId
            AND it.name = :ingestionTypeName
            """)
    long countErrorRecordsBySubmissionAndIngestionType(
            @Param("submissionId") Long submissionId, @Param("ingestionTypeName") String ingestionTypeName);

    @Query("""
            SELECT COUNT(er)
            FROM ErrorRecord er
            JOIN er.ingestion i
            JOIN i.submission s
            WHERE s.id IN :submissionIds
            """)
    Long countErrorRecordsBySubmissionIds(@Param("submissionIds") List<Long> submissionIds);

    @Query("""
            SELECT COUNT(er)
            FROM ErrorRecord er
            JOIN er.ingestion i
            JOIN i.ingestionType it
            JOIN i.submission s
            WHERE s.id IN :submissionIds
            AND it.name = :ingestionTypeName
            """)
    Long countErrorRecordsBySubmissionIdsAndIngestionType(@Param("submissionIds") List<Long> submissionIds, @Param("ingestionTypeName") String ingestionTypeName);

    @Query("""
            SELECT COUNT(er)
            FROM ErrorRecord er
            JOIN er.ingestion i
            WHERE i.id = :ingestionId
            AND i.ingestionType.name = :ingestionTypeName
            """)
    Long countErrorRecordsByIngestionIdAndIngestionTypeName(@Param("ingestionId") Long ingestionId, @Param("ingestionTypeName") String ingestionTypeName);

    List<ErrorRecord> findByIngestionId(@Param("ingestionId") Long ingestionId);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM ErrorRecord er
            WHERE er.ingestion.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);

    @Query("SELECT er FROM ErrorRecord er " +
            "JOIN er.errorCauses ec " +
            "WHERE er.ingestion.id = :ingestionId " +
            "GROUP BY er.id " +
            "HAVING COUNT(ec.id) = 1 " +
            "AND SUM(CASE WHEN ec.errorType.errorCode = :errorCode THEN 1 ELSE 0 END) = 1")
    List<ErrorRecord> findRecordsWithExactlyOneCauseOfType(
            @Param("ingestionId") Long ingestionId,
            @Param("errorCode") String errorCode);

    @Query("SELECT DISTINCT er " +
            "FROM ErrorRecord er " +
            "JOIN er.errorCauses ec " +
            "JOIN ec.errorType et " +
            "JOIN er.ingestion i " +
            "JOIN i.submission s " +
            "WHERE s.id = :submissionId " +
            "AND et.errorCode = :errorCode")
    List<ErrorRecord> findRecordsByErrorCodeAndSubmissionId(
            @Param("submissionId") Long submissionId,
            @Param("errorCode") String errorCode);
}



