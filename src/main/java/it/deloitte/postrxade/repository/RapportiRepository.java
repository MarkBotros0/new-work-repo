package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Output;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import it.deloitte.postrxade.entity.Rapporti;

import java.util.List;

/**
 * Repository for Rapporti (Relationships/Accounts) entity.
 */
@Repository
public interface RapportiRepository extends JpaRepository<Rapporti, Long>, RapportiRepositoryCustom {
    long countByIngestionId(Long ingestionId);

    @Modifying
    @Query("UPDATE Rapporti c SET c.output = :output WHERE c.id IN :ids")
    void updateOutputForeignKey(@Param("ids") List<Long> rapportiIds, @Param("output") Output output);

    @Modifying
    @Query("UPDATE Rapporti r SET r.output = :output " +
            "WHERE r.chiaveRapporto IN :chiavi AND r.submission.id = :submissionId")
    void updateOutputForeignKeyByChiavi(@Param("chiavi") List<String> chiavi,
                                        @Param("submissionId") Long submissionId,
                                        @Param("output") Output output);

    List<Rapporti> findByOutputId(Long outputId);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM Rapporti r
            WHERE r.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);

    @Query("""
              SELECT COUNT(t)
              FROM Rapporti r
              WHERE r.submission.id = :submissionId
            """)
    long countBySubmissionId(@Param("submissionId") Long submissionId);
}
