package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Set;

/**
 * Entity representing a User in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "USER")
public class User implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
//    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_user")
    private String id;

    @Column(name = "first_name")
    private String firstName;

    @Column(name = "last_name")
    private String lastName;

    @Column(name = "email")
    private String email;

    @Column(name = "company")
    private String company;

    @Column(name = "office")
    private String office;

    @Column(name = "last_logged_in")
    private LocalDateTime lastLoggedIn;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "USER_AUTHORITY",
        joinColumns = @JoinColumn(name = "fk_user"),
        inverseJoinColumns = @JoinColumn(name = "fk_authority")
    )
    private Set<Authority> authorities;

    public User(String id, String firstName, String lastName, String email, String company, String office) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.company = company;
        this.office = office;
    }
}
