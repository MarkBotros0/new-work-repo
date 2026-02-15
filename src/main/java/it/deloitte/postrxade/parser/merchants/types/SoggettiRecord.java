package it.deloitte.postrxade.parser.merchants.types;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class SoggettiRecord {
    String intermediario;
    String ndg;
    String dataCensimentoAnagrafico;
    String dataEstinzioneAnagrafico;
    String filler1;
    String filialeCensimentoAnagrafico;
    String tipoSoggetto;
    String naturaGiuridica;
    String sesso;
    String codiceFiscale;
    String cognome;
    String nome;
    String dataNascita;
    String comune;
    String provincia;
    String nazione;
    String filler2;
    String dataPredisposizioneFlusso;
    String filler3;
    String controlloDiFineRiga;
}
