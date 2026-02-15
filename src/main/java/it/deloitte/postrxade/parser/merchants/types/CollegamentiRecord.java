package it.deloitte.postrxade.parser.merchants.types;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class CollegamentiRecord {
     String intermediario;
     String chiaveRapporto;
     String ndg;
     String ruolo;
     String dataInizioCollegamento;
     String dataFineCollegamento;
     String ruoloInterno;
     String flagStatoCollegamento;
     String dataPredisposizioneFlusso;
     String filler;
     String controlloDiFineRiga;
}
