package it.deloitte.postrxade.parser.merchants.types;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RapportiRecord {
    String intermediario;
    String chiaveRapporto;
    String tipoRapportoInterno;
    String formaTecnica;
    String filiale;
    String cab;
    String numeroConto;
    String cin;
    String divisa;
    String dataInizioRapporto;
    String dataFineRapporto;
    String note;
    String flagStatoRapporto;
    String dataPredisposizione;
    String filler;
    String controlloDiFineRiga;
}
