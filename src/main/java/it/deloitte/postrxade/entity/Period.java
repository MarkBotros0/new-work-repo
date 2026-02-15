package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * Entity representing a Period in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "PERIOD")
public class Period {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_period")
    private Long id;

    @Column(name = "name", length = 50)
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "order")
    private Integer order;

    @Column(name = "is_active")
    private Boolean isActive;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "period", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Obligation> obligations;

    public Period(Long id, String name, String description, Integer order, Boolean isActive) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.order = order;
        this.isActive = isActive;
    }
}
