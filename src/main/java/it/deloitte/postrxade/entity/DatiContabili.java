package it.deloitte.postrxade.entity;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "MERCHANT_DATI_CONTABILI")
public class DatiContabili {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_dati_contabili")
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

    @Column(name = "anno_di_riferimento", length = 4)
    String annoDiRiferimento;

    @Column(name = "periodicita", length = 3)
    String periodicita;

    @Column(name = "progressivo_periodicita", length = 3)
    String progressivoPeriodicita;

    @Column(name = "divisa", length = 3)
    String divisa;

    @Column(name = "data_inizio_riferimento", length = 8)
    String dataInizioRiferimento;

    @Column(name = "data_fine_riferimento", length = 8)
    String dataFineRiferimento;

    @Column(name = "importo_saldo_iniziale", length = 18)
    String importoSaldoIniziale;

    @Column(name = "importo_saldo_finale", length = 18)
    String importoSaldoFinale;

    @Column(name = "totale_operazioni_attive", length = 18)
    String totaleOperazioniAttive;

    @Column(name = "totale_operazioni_passive", length = 18)
    String totaleOperazioniPassive;

    @Column(name = "giacenza_media", length = 18)
    String giacenzaMedia;

    @Column(name = "flag_soglia_saldo_iniziale", length = 1)
    String flagSogliaSaldoIniziale;

    @Column(name = "flag_soglia_saldo_finale", length = 1)
    String flagSogliaSaldoFinale;

    @Column(name = "flag_soglia_operazioni_attive", length = 1)
    String flagSogliaOperazioniAttive;

    @Column(name = "flag_soglia_operazioni_passive", length = 1)
    String flagSogliaOperazioniPassive;

    @Column(name = "flag_soglia_giacenza_media", length = 1)
    String flagSogliaGiacenzaMedia;

    @Column(name = "altre_informazioni", length = 18)
    String altreInformazioni;

    @Column(name = "flag_stato_importo", length = 1)
    String flagStatoImporto;

    @Column(name = "data_predisposizione", length = 8)
    String dataPredisposizione;

    @Column(name = "tipo_rapporto_interno", length = 3)
    String tipoRapportoInterno;

    @Column(name = "forma_tecnica", length = 5)
    String formaTecnica;

    @Column(name = "flag_soglia_altre_informazioni", length = 1)
    String flagSogliaAltreInformazioni;

    @Column(name = "controllo_di_fine_riga", length = 1)
    String controlloDiFineRiga;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_output")
    private Output output;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "chiave_rapporto", referencedColumnName = "chiave_rapporto", insertable = false, updatable = false)
    private Rapporti rapporto;

    @Transient
    private String rawRow;
}