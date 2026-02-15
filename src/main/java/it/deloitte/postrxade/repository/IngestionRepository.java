package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Ingestion;
import it.deloitte.postrxade.entity.IngestionStatus;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Repository for Ingestion entity.
 */
@Repository
public interface IngestionRepository extends JpaRepository<Ingestion, Long> {

    /**
     * Find all ingestions ordered by ingestion date (most recent first).
     *
     * @return List of ingestions ordered by ingestedAt descending
     */
    List<Ingestion> findAllByOrderByIngestedAtDesc();

    /**
     * Find all ingestions ordered by ingestion date (oldest first).
     *
     * @return List of ingestions ordered by ingestedAt ascending
     */
    List<Ingestion> findAllByOrderByIngestedAtAsc();

    /**
     * Find ingestions by submission ID.
     *
     * @param submissionId the submission ID
     * @return List of ingestions for the given submission
     */
    List<Ingestion> findBySubmission_Id(Long submissionId);

    @Query("""
                SELECT i
                FROM Ingestion i
                JOIN i.ingestionType it
                JOIN i.submission s
                WHERE it.name = :ingestionTypeName
                AND s.id = :submissionId
            """)
    Optional<Ingestion> findBySubmissionIdAndIngestionTypeName(
            @Param("submissionId") Long submissionId,
            @Param("ingestionTypeName") String ingestionTypeName
    );

    Optional<Ingestion> findFirstBySubmission_IdAndIngestionType_Name(
            Long submissionId,
            String ingestionTypeName
    );

    /**
     * Find ingestions by ingestion type.
     *
     * @param ingestionTypeId the ingestion type ID
     * @return List of ingestions for the given type
     */
    List<Ingestion> findByIngestionType_Id(Long ingestionTypeId);

    /**
     * Find ingestions by ingestion status.
     *
     * @param ingestionStatusId the ingestion status ID
     * @return List of ingestions for the given status
     */
    List<Ingestion> findByIngestionStatus_Id(Long ingestionStatusId);

    /**
     * Find ingestions between two dates.
     *
     * @param startDate the start date
     * @param endDate   the end date
     * @return List of ingestions between the given dates
     */
    List<Ingestion> findByIngestedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    @Modifying
    @Transactional
    @Query("UPDATE Ingestion i SET i.ingestionStatus = :status WHERE i.id = :id")
    void updateIngestionStatus(@Param("id") Long id, @Param("status") IngestionStatus status);
}
