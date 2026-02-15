package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.Output;
import it.deloitte.postrxade.entity.Transaction;
import it.deloitte.postrxade.records.TransactionDateCount;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, Long>, TransactionRepositoryCustom {
    long countByIngestionId(Long ingestionId);

    @Modifying
    @Query("UPDATE Transaction t SET t.output = :output WHERE t.id IN :ids")
    void updateOutputForeignKey(@Param("ids") List<Long> transactionIds, @Param("output") Output output);

    @Query("""
              SELECT COUNT(t)
              FROM Transaction t
              JOIN t.ingestion i
              JOIN i.submission s
              JOIN s.obligation o
              WHERE o.id = :obligationId
            """)
    long countTransactionsByObligation(@Param("obligationId") Long obligationId);

    @Query("""
              SELECT COUNT(t)
              FROM Transaction t
              JOIN t.ingestion i
              JOIN i.submission s
              WHERE s.id = :submissionId
            """)
    long countTransactionsBySubmission(@Param("submissionId") Long submissionId);

    @Query("""
             SELECT COUNT(t)
             FROM Transaction t
             JOIN t.ingestion i
             JOIN i.submission s
             WHERE s.id IN :submissionIds
            """)
    Long countTransactionsBySubmissionIds(@Param("submissionIds") List<Long> submissionIds);

    @Query("""
             SELECT COUNT(t)
             FROM Transaction t
             JOIN t.ingestion i
             JOIN i.submission s
             WHERE s.id IN :submissionIds
             AND t.tipoPag = :tipoPag
            """)
    Long countTransactionsByObligationAndTipoPag(
            @Param("submissionIds") List<Long> submissionIds,
            @Param("tipoPag") String tipoPag);

    @Query("""
             select new it.deloitte.postrxade.records.TransactionDateCount(
                 t.dtOpe,
                 count(t)
              )
              from Transaction t
              join t.ingestion i
              join i.submission s
              where s.id in :submissionIds
              group by t.dtOpe
              order by t.dtOpe
            """)
    List<TransactionDateCount> countTransactionsGroupedByDate(
            @Param("submissionIds") List<Long> submissionIds
    );

    @Modifying
    @Transactional
    @Query("""
            DELETE FROM Transaction t
            WHERE t.submission.id = :submissionId
            """)
    int deleteBySubmissionId(@Param("submissionId") Long submissionId);
}
