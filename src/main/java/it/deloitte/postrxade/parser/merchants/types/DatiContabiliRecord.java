package it.deloitte.postrxade.parser.merchants.types;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DatiContabiliRecord {
    String intermediario;
    String chiaveRapporto;
    String annoDiRiferimento;
    String periodicita;
    String progressivoPeriodicita;
    String divisa;
    String dataInizioRiferimento;
    String dataFineRiferimento;
    String importoSaldoIniziale;
    String importoSaldoFinale;
    String totaleOperazioniAttive;
    String totaleOperazioniPassive;
    String giacenzaMedia;
    String flagSogliaSaldoIniziale;
    String flagSogliaSaldoFinale;
    String flagSogliaOperazioniAttive;
    String flagSogliaOperazioniPassive;
    String flagSogliaGiacenzaMedia;
    String altreInformazioni;
    String flagStatoImporto;
    String dataPredisposizione;
    String filler1;
    String tipoRapportoInterno;
    String formaTecnica;
    String filler2;
    String flagSogliaAltreInformazioni;
    String filler3;
    String controlloDiFineRiga;
}
