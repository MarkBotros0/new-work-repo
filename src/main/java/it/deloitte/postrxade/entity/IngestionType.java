package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * Entity representing an Ingestion Type in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "INGESTION_TYPE")
public class IngestionType {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_ingestion_type")
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "order")
    private Integer order;

    @Column(name = "is_active")
    private Boolean isActive;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestionType", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Ingestion> ingestions;

}
