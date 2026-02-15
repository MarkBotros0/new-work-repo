package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Entity representing an Ingestion in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "INGESTION")
public class Ingestion {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_ingestion")
    private Long id;

    @ManyToOne()
    @JoinColumn(name = "fk_ingestion_type")
    private IngestionType ingestionType;

    @ManyToOne()
    @JoinColumn(name = "fk_ingestion_status")
    private IngestionStatus ingestionStatus;

    @ManyToOne()
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @ManyToOne()
    @JoinColumn(name = "fk_ingestion_error")
    private IngestionError ingestionError;

    @Column(name = "ingested_at")
    private LocalDateTime ingestedAt;

    @Column(name = "full_path")
    private String fullPath;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<Transaction> transactions;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<ResolvedTransaction> resolvedTransactions;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<Merchant> merchants;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<Soggetti> soggettiList;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<Rapporti> rapportiList;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<DatiContabili> datiContabiliList;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<Collegamenti> collegamentiList;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<CambioNdg> cambioNdgList;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "ingestion", cascade = CascadeType.ALL)
    private List<ErrorRecord> errorRecords;

    public Ingestion(Long id, IngestionType ingestionType, IngestionStatus ingestionStatus, Submission submission) {
        this.id = id;
        this.ingestionType = ingestionType;
        this.ingestionStatus = ingestionStatus;
        this.submission = submission;
        this.ingestedAt = LocalDateTime.now();
    }
}
