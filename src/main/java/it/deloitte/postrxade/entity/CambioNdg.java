package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import java.time.LocalDateTime;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "MERCHANT_CAMBIO_NDG")
public class CambioNdg {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_cambio_ndg")
    private Long id;

    @ManyToOne()
    @JoinColumn(name = "fk_ingestion")
    private Ingestion ingestion;

    @ManyToOne()
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @Column(name = "intermediario", length = 11)
    String intermediario;

    @Column(name = "ndg_vecchio", length = 16)
    String ndgVecchio;

    @Column(name = "ndg_nuovo", length = 16)
    String ndgNuovo;

    @Column(name = "controllo_di_fine_riga", length = 1)
    String controlloDiFineRiga;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_output")
    private Output output;

    @Transient
    private String rawRow;
}