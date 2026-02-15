package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.ErrorType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository for ErrorType entity.
 */
@Repository
public interface ErrorTypeRepository extends JpaRepository<ErrorType, Long> {

    @Query("""
            select distinct ec.errorType
            from ErrorCause ec
            join ec.errorRecord er
            join er.ingestion i
            join i.submission s
            where s.id in :submissionIds
             """)
    List<ErrorType> findDistinctErrorTypesBySubmissionIds(@Param("submissionIds") List<Long> submissionIds);

    @Query("""
            select distinct ec.errorType
            from ErrorCause ec
            join ec.errorRecord er
            join er.ingestion i
            join i.ingestionType it
            join i.submission s
            where s.id in :submissionIds
            and it.name = :ingestionTypeName
             """)
    List<ErrorType> findDistinctErrorTypesBySubmissionIdsAndIngestionType(
            @Param("submissionIds") List<Long> submissionIds,
            @Param("ingestionTypeName") String ingestionTypeName);

    @Query("""
            select et
            from ErrorType et
            join et.errorCauses ec
            where ec.submission.id = :submissionId
            AND et.severityLevel = :severityLevel
             """)
    List<ErrorType> findBySubmissionIdAndSeverity(
            @Param("submissionId") Long submissionId,
            @Param("severityLevel") Integer severityLevel

    );

    Optional<ErrorType> findByErrorCode(String errorCode);
    Optional<ErrorType> findByName(String name);
}



