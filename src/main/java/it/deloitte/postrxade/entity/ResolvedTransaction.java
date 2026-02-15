package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Entity representing a Transaction in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "RESOLVED_TRANSACTION")
public class ResolvedTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_resolved_transaction")
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_ingestion")
    private Ingestion ingestion;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_current_submission")
    private Submission currentSubmission;

    @Column(name = "tp_rec", length = 1)
    private String tpRec;

    @Column(name = "id_intermediario", length = 11)
    private String idIntermediario;

    @Column(name = "id_esercente", length = 30)
    private String idEsercente;

    @Column(name = "chiave_banca", length = 50)
    private String chiaveBanca;

    @Column(name = "id_pos", length = 30)
    private String idPos;

    @Column(name = "tipo_ope", length = 2)
    private String tipoOpe;

    @Column(name = "dt_ope")
    private String dtOpe;

    @Column(name = "divisa_ope", length = 3)
    private String divisaOpe;

    @Column(name = "tipo_pag", length = 2)
    private String tipoPag;

    @Column(name = "imp_ope", precision = 14, scale = 2)
    private BigDecimal impOpe;

    @Column(name = "tot_ope")
    private Integer totOpe;

    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Transient
    private String rawRow;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_output")
    private Output output;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumns({
        @JoinColumn(name = "id_esercente", referencedColumnName = "id_esercente", insertable = false, updatable = false),
        @JoinColumn(name = "id_intermediario", referencedColumnName = "id_intermediario", insertable = false, updatable = false)
    })
    private Merchant merchant;

    public ResolvedTransaction(Long id, Ingestion ingestion, String tpRec, String idIntermediario, String idEsercente, String chiaveBanca,
                               String idPos, String tipoOpe, String dtOpe, String divisaOpe,
                               String tipoPag, BigDecimal impOpe, Integer totOpe) {
        this.id = id;
        this.ingestion = ingestion;
        this.tpRec = tpRec;
        this.idIntermediario = idIntermediario;
        this.idEsercente = idEsercente;
        this.chiaveBanca = chiaveBanca;
        this.idPos = idPos;
        this.tipoOpe = tipoOpe;
        this.dtOpe = dtOpe;
        this.divisaOpe = divisaOpe;
        this.tipoPag = tipoPag;
        this.impOpe = impOpe;
        this.totOpe = totOpe;
    }
}
