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
@Table(name = "MERCHANT_SOGGETTI")
public class Soggetti {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_soggetti")
    private Long id;

    @ManyToOne()
    @JoinColumn(name = "fk_ingestion")
    private Ingestion ingestion;

    @ManyToOne()
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @Column(name = "intermediario", length = 11)
    String intermediario;

    @Column(name = "ndg", length = 16)
    String ndg;

    @Column(name = "data_censimento_anagrafico", length = 8)
    String dataCensimentoAnagrafico;

    @Column(name = "data_estinzione_anagrafica", length = 8)
    String dataEstinzioneAnagrafica;

    @Column(name = "filiale_censimento_anagrafico", length = 5)
    String filialeCensimentoAnagrafico;

    @Column(name = "tipo_soggetto", length = 1)
    String tipoSoggetto;

    @Column(name = "natura_giuridica", length = 5)
    String naturaGiuridica;

    @Column(name = "sesso", length = 1)
    String sesso;

    @Column(name = "codice_fiscale", length = 16)
    String codiceFiscale;

    @Column(name = "cognome", length = 75)
    String cognome;

    @Column(name = "nome", length = 75)
    String nome;

    @Column(name = "data_nascita", length = 8)
    String dataNascita;

    @Column(name = "comune", length = 40)
    String comune;

    @Column(name = "provincia", length = 2)
    String provincia;

    @Column(name = "nazione", length = 40)
    String nazione;

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
}