package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Entity representing a Merchant in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "MERCHANT")
@ToString
public class Merchant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_merchant")
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_ingestion")
    private Ingestion ingestion;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @Column(name = "tp_rec", length = 1)
    private String tpRec;

    @Column(name = "id_intermediario", length = 11)
    private String idIntermediario;

    @Column(name = "id_esercente", length = 30)
    private String idEsercente;

    @Column(name = "cod_fiscale", length = 16)
    private String codFiscale;

    @Column(name = "partita_iva", length = 11)
    private String partitaIva;

    @Column(name = "id_salmov", length = 50)
    private String idSalmov;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "merchant", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<Transaction> transactions;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "merchant", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<ResolvedTransaction> resolvedTransactions;

    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Transient
    private String rawRow;

    public Merchant(Long id, Ingestion ingestion, String tpRec, String idIntermediario, String idEsercente, String codFiscale, String partitaIva) {
        this.id = id;
        this.ingestion = ingestion;
        this.tpRec = tpRec;
        this.idIntermediario = idIntermediario;
        this.idEsercente = idEsercente;
        this.codFiscale = codFiscale;
        this.partitaIva = partitaIva;
    }
}

