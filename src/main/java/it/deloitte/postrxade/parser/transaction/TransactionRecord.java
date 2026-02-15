package it.deloitte.postrxade.parser.transaction;

import lombok.*;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TransactionRecord {
    String tipoRecord;
    String intermediario;
    String idEsercente;
    String tipoOperazione;
    String importoTotaleOperazioni;
    String chiaveRapportoBanca;
    String terminalId;
    String dataOperazione;
    String divisaOperazioni;
    String tipoPagamento;
    String numeroOperazioniGiorno;
    String statoOperazioni;
    String dataPredisposizioneFlusso;
    String filler;
    String carattereDiControllo;
}
