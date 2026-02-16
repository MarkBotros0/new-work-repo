package it.deloitte.postrxade.repository;

import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import it.deloitte.postrxade.entity.Collegamenti;
import it.deloitte.postrxade.entity.Output;
import it.deloitte.postrxade.records.RapportoRowDTO;
import jakarta.transaction.Transactional;

/**
 * Repository for Collegamenti (Links) entity.
 */
@Repository
public interface CollegamentiRepository extends JpaRepository<Collegamenti, Long>, CollegamentiRepositoryCustom {
    long countByIngestionId(Long ingestionId);

    @Modifying
    @Query("UPDATE Collegamenti c SET c.output = :output WHERE c.id IN :ids")
    void updateOutputForeignKey(@Param("ids") List<Long> collegamentiIds, @Param("output") Output output);

    @Query("""
            SELECT c FROM Collegamenti c 
            LEFT JOIN FETCH c.soggetto 
            LEFT JOIN FETCH c.rapporto 
            LEFT JOIN FETCH c.datiContabili
            WHERE c.submission.id = :submissionId
            """)
    List<Collegamenti> findCollegamentiBySubmissionId(@Param("submissionId") Long submissionId);

//    @Query("""
//            SELECT new map(c.id as id, c.ndg as ndg, c.chiaveRapporto as chiaveRapporto)
//            FROM Collegamenti c
//            WHERE c.submission.id = :submissionId
//            """)
//    List<Map<String, Object>> findCollegamentiMetadataBySubmissionId(@Param("submissionId") Long submissionId);

    List<Collegamenti> findByOutputId(Long outputId);

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM Collegamenti c
            WHERE c.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);

    @Query("""
                SELECT new it.deloitte.postrxade.records.RapportoRowDTO(
                    c.chiaveRapporto,
                    c.ndg,
                    c.ruolo,
                    s.codiceFiscale,
                    s.cognome,
                    s.nome,
                    r.dataInizioRapporto,
                    r.dataFineRapporto
                )
                FROM Collegamenti c
                JOIN Rapporti r 
                     ON r.chiaveRapporto = c.chiaveRapporto
                    AND r.submission.id = :submissionId
                JOIN Soggetti s 
                     ON s.ndg = c.ndg
                    AND s.submission.id = :submissionId
                WHERE c.submission.id = :submissionId
            """)
    List<RapportoRowDTO> buildOutputRowsBySubmission(Long submissionId);

    @Query("""
              SELECT COUNT(t)
              FROM Collegamenti c
              WHERE c.submission.id = :submissionId
            """)
    long countBySubmissionId(@Param("submissionId") Long submissionId);
}
