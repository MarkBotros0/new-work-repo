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
@Table(name = "MERCHANT_COLLEGAMENTI")
public class Collegamenti {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_collegamenti")
    private Long id;

    @ManyToOne()
    @JoinColumn(name = "fk_ingestion")
    private Ingestion ingestion;

    @ManyToOne()
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @Column(name = "intermediario", length = 11)
    String intermediario;

    @Column(name = "chiave_rapporto", length = 50)
    String chiaveRapporto;

    @Column(name = "ndg", length = 16)
    String ndg;

    @Column(name = "ruolo", length = 1)
    String ruolo;

    @Column(name = "data_inizio_collegamento", length = 8)
    String dataInizioCollegamento;

    @Column(name = "data_fine_collegamento", length = 8)
    String dataFineCollegamento;

    @Column(name = "ruolo_interno", length = 3)
    String ruoloInterno;

    @Column(name = "flag_stato_collegamento", length = 1)
    String flagStatoCollegamento;

    @Column(name = "data_predisposizione_flusso", length = 8)
    String dataPredisposizioneFlusso;

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

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "ndg", referencedColumnName = "ndg", insertable = false, updatable = false)
    private Soggetti soggetto;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "chiave_rapporto", referencedColumnName = "chiave_rapporto", insertable = false, updatable = false)
    private DatiContabili datiContabili;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "chiave_rapporto", referencedColumnName = "chiave_rapporto", insertable = false, updatable = false)
    private Rapporti rapporto;
}