package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.proxy.HibernateProxy;

import java.util.Objects;
import java.util.Set;

/**
 * Entity representing an Authority in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "AUTHORITY")
public class Authority {

    @Id
    @Column(name = "pk_authority", length = 50)
    private String id;

    @Column(name = "description")
    private String description;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @ManyToMany(mappedBy = "authorities", fetch = FetchType.EAGER)
    private Set<User> users;

    public Authority(String id, String description) {
        this.id = id;
        this.description = description;
    }

}
