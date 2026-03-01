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
@Table(name = "MERCHANT_RAPPORTI")
public class Rapporti {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_rapporti")
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

    @Column(name = "tipo_rapporto_interno", length = 3)
    String tipoRapportoInterno;

    @Column(name = "forma_tecnica", length = 5)
    String formaTecnica;

    @Column(name = "filiale", length = 5)
    String filiale;

    @Column(name = "cab", length = 5)
    String cab;

    @Column(name = "numero_conto", length = 27)
    String numeroConto;

    @Column(name = "cin", length = 2)
    String cin;

    @Column(name = "divisa", length = 3)
    String divisa;

    @Column(name = "data_inizio_rapporto", length = 8)
    String dataInizioRapporto;

    @Column(name = "data_fine_rapporto", length = 8)
    String dataFineRapporto;

    @Column(name = "note", length = 24)
    String note;

    @Column(name = "flag_stato_rapporto", length = 1)
    String flagStatoRapporto;

    @Column(name = "data_predisposizione", length = 8)
    String dataPredisposizione;

    @Column(name = "controllo_di_fine_riga", length = 1)
    String controlloDiFineRiga;

    @Column(name = "ADE_RAPPORTO_IDENTIFIER", length = 50)
    String adeRapportoIdentifier;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_output")
    private Output output;

    @Transient
    private String rawRow;
}