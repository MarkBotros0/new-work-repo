package it.deloitte.postrxade.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import it.deloitte.postrxade.entity.CambioNdg;
import jakarta.transaction.Transactional;

/**
 * Repository for CambioNdg (NDG Changes) entity.
 */
@Repository
public interface CambioNdgRepository extends JpaRepository<CambioNdg, Long>, CambioNdgRepositoryCustom {
    long countByIngestionId(Long ingestionId);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM CambioNdg c
            WHERE c.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);
}
