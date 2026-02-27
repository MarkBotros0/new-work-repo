package it.deloitte.postrxade.repository;


import it.deloitte.postrxade.entity.Log;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface LogRepository extends JpaRepository<Log,Integer> {

    @Query(
            value = """
            select log from Log log
            """
    )
    Page<Log> getAllLogsUsingPagination(Pageable pageable);
}
