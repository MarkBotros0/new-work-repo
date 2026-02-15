package it.deloitte.postrxade.records;

import java.math.BigDecimal;
import java.util.Date;

public record MerchantOutputData(
        String ruolo,
        Integer annoDiRiferimento,
        String divisa,
        BigDecimal importoSaldoIniziale,
        BigDecimal importoSaldoFinale,
        BigDecimal totaleOperazioniAttive,
        BigDecimal totaleOperazioniPassive,
        BigDecimal giacenzaMedia,
        Boolean flagSogliaSaldoIniziale,
        Boolean flagSogliaSaldoFinale,
        Boolean flagSogliaOperazioniAttive,
        Boolean flagSogliaOperazioniPassive,
        Boolean flagSogliaGiacenzaMedia,
        String altreInformazioni,
        Boolean flagSogliaAltreInformazioni,
        String naturaValuta,
        BigDecimal importoTitoliDiStato,
        Boolean flagSogliaTitoliDiStato,
        String cab,
        Date dataInizioRapporto,
        Date dataFineRapporto,
        String sesso,
        String codiceFiscale,
        String cognome,
        String nome,
        Date dataNascita,
        String comune,
        String provincia,
        long pk_dati_contabili,
        long pk_soggetti,
        long pk_rapporti,
        long pk_collegamenti
) {
}
