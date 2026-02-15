package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;


/**
 * Entity representing a User Authority in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "USER_AUTHORITY")
@EqualsAndHashCode
@ToString
public class UserAuthority implements Serializable {

    @EmbeddedId
    private UserAuthorityId id;

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("fkUser")
    @JoinColumn(name = "fk_user")
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    @MapsId("fkAuthority")
    @JoinColumn(name = "fk_authority")
    private Authority authority;

    @Embeddable
    @Data
    @EqualsAndHashCode
    @ToString
    public static class UserAuthorityId implements Serializable {
        private String fkUser;
        private String fkAuthority;
    }
}

