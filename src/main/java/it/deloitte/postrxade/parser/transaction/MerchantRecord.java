package it.deloitte.postrxade.parser.transaction;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MerchantRecord {
    String tipoRecord;
    String intermediario;
    String idEsercente;
    String codiceFiscaleEsercente;
    String partitaIva;
    String idSalmov;
    String tipoSoggetto;
    String denominazione;
    String dataDiPredisposizioneFlusso;
    String filler;
    String carattereDiControllo;
}
