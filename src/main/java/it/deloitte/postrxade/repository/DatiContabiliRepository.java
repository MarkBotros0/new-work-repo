package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Output;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import it.deloitte.postrxade.entity.DatiContabili;

import java.util.List;

/**
 * Repository for DatiContabili (Accounting Data) entity.
 */
@Repository
public interface DatiContabiliRepository extends JpaRepository<DatiContabili, Long>, DatiContabiliRepositoryCustom {
    @Modifying
    @Query("UPDATE DatiContabili c SET c.output = :output WHERE c.id IN :ids")
    void updateOutputForeignKey(@Param("ids") List<Long> datiContabiliIds, @Param("output") Output output);

    @Modifying
    @Query("UPDATE DatiContabili d SET d.output = :output " +
            "WHERE d.chiaveRapporto IN :chiavi AND d.submission.id = :submissionId")
    void updateOutputForeignKeyByChiavi(@Param("chiavi") List<String> chiavi,
                                        @Param("submissionId") Long submissionId,
                                        @Param("output") Output output);

    List<DatiContabili> findByOutputId(Long outputId);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM DatiContabili dc
            WHERE dc.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);
}
