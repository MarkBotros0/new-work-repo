package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Output;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import it.deloitte.postrxade.entity.Soggetti;

import java.util.List;

/**
 * Repository for Soggetti (Subject Master) entity.
 */
@Repository
public interface SoggettiRepository extends JpaRepository<Soggetti, Long>, SoggettiRepositoryCustom {
    long countByIngestionId(Long ingestionId);

    @Modifying
    @Query("UPDATE Soggetti c SET c.output = :output WHERE c.id IN :ids")
    void updateOutputForeignKey(@Param("ids") List<Long> soggettiIds, @Param("output") Output output);

    @Modifying
    @Query("UPDATE Soggetti s SET s.output = :output " +
            "WHERE s.ndg IN :ndgs AND s.submission.id = :submissionId")
    void updateOutputForeignKeyByNdgs(@Param("ndgs") List<String> ndgs,
                                      @Param("submissionId") Long submissionId,
                                      @Param("output") Output output);

    List<Soggetti> findByOutputId(Long outputId);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM Soggetti s
            WHERE s.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);

    long countBySubmissionId(Long submissionId);

    @Query("SELECT COUNT(s) FROM Soggetti s WHERE s.submission.id IN :submissionIds")
    long countBySubmissionIn(@Param("submissionIds") List<Long> submissionIds);
}
