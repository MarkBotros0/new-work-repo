package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.UserAuthority;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface UserAuthorityRepository extends JpaRepository<UserAuthority, UserAuthority.UserAuthorityId> {

    @Query("SELECT ua FROM UserAuthority ua WHERE ua.id.fkUser = :userId")
    List<UserAuthority> findByUserId(@Param("userId") Long userId);

    @Query("SELECT ua FROM UserAuthority ua WHERE ua.id.fkAuthority = :authorityName")
    List<UserAuthority> findByAuthorityName(@Param("authorityName") String authorityName);

    @Query("DELETE FROM UserAuthority ua WHERE ua.id.fkUser = :userId")
    void deleteByUserId(@Param("userId") Long userId);

    @Query("DELETE FROM UserAuthority ua WHERE ua.id.fkAuthority = :authorityName")
    void deleteByAuthorityId(@Param("authorityName") String authorityName);
}

