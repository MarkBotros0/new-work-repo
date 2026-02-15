package it.deloitte.postrxade.formatter;

import it.deloitte.postrxade.entity.*;
import lombok.NoArgsConstructor;

import java.time.format.DateTimeFormatter;

@NoArgsConstructor
public final class OutputFileFormatter {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");
    private static String os = System.getProperty("os.name").toLowerCase();

    public static String createHeader() {
        StringBuilder sb = new StringBuilder(398);

        sb.append("0");                         // 1  Tipo record
        sb.append("ARU00");                     // 2  Codice fornitura
        sb.append("1");                         // 3  Tipologia invio
        sb.append("1");                         // 4  Tipologia comunicazione
        sb.append("0000");                      // 5  Anno riferimento
        sb.append("00");                        // 6  Mese riferimento
        sb.append(" ".repeat(24));              // 7  Protocollo da annullare

        sb.append(rightPad("04107060966", 16, ' ')); // 8  Codice fiscale

        sb.append(" ".repeat(26));              // 9  Cognome
        sb.append(" ".repeat(25));              // 10 Nome
        sb.append(" ");                         // 11 Sesso
        sb.append(" ".repeat(8));               // 12 Data nascita
        sb.append(" ".repeat(40));              // 13 Comune nascita
        sb.append(" ".repeat(2));               // 14 Provincia nascita

        sb.append(rightPad("Nexi Payments SpA", 70, ' ')); // 15 Denominazione
        sb.append(rightPad("Milano", 40, ' '));            // 16 Comune sede
        sb.append("03");                                   // 17 Provincia sede

        sb.append(" ".repeat(16));              // 18 CF società incorporata
        sb.append("0");                         // 19 Flag cessazione
        sb.append(" ".repeat(8));               // 20 Data chiusura generalizzata
        sb.append(" ".repeat(104));             // 21 Filler

        sb.append("A");                         // 22 Carattere controllo

        sb.append(getEndOfLine());              // 23 CRLF / LF

        return sb.toString();
    }


    public static String createFooter(int numTipo1, int numTipo2, int numTipo3) {
        StringBuilder sb = new StringBuilder();

        // 1. Tipo record (position 1, always "9")
        sb.append("9");

        // 2. Numero record di tipo 1 (positions 2-10, 9 chars, zero-padded)
        sb.append(String.format("%09d", numTipo1));

        // 3. Numero record di tipo 2 (positions 11-19, 9 chars, zero-padded)
        sb.append(String.format("%09d", numTipo2));

        // 4. Numero record di tipo 3 (positions 20-28, 9 chars, zero-padded)
        sb.append(String.format("%09d", numTipo3));

        // 5. Numero record di tipo 4 (positions 29-37, 9 chars, always "000000000")
        sb.append("000000000");

        // 6. Filler (positions 38-397, 360 chars, spaces)
        sb.append(" ".repeat(360));

        // 7. Carattere di controllo (position 398, always "A")
        sb.append("A");

        // 8. End-of-line (platform-dependent)
        sb.append(getEndOfLine());

        return sb.toString();
    }

    public static String toRapportiOutputString(Rapporti rapporti) {
        StringBuilder sb = new StringBuilder();

        sb.append("1");

        sb.append(" ".repeat(50));

        sb.append(rightPad(rapporti.getTipoRapportoInterno(), 2, ' '));

        sb.append(" ".repeat(24));

        if (rapporti.getDataInizioRapporto().isEmpty()) {
            sb.append("01010001");
        } else {
            sb.append(rightPad(rapporti.getDataInizioRapporto(), 8, ' '));
        }

        if (rapporti.getDataFineRapporto().isEmpty()) {
            sb.append("01010001");
        } else {
            sb.append(rightPad(rapporti.getDataFineRapporto(), 8, ' '));
        }

        sb.append(rightPad(rapporti.getCab(), 2, ' '));

        sb.append(" ".repeat(13));

        sb.append(" ".repeat(286));

        sb.append('A');

        sb.append(getEndOfLine());

        return sb.toString();
    }


    public static String toCollegamentiOutputString(Collegamenti collegamenti) {
        StringBuilder sb = new StringBuilder();

        sb.append("2");
        sb.append(rightPad(collegamenti.getChiaveRapporto(), 50, ' '));
        sb.append(rightPad(collegamenti.getRapporto().getDataInizioRapporto(), 9, ' '));

        if (collegamenti.getRapporto().getDataFineRapporto() != null && !collegamenti.getRapporto().getDataFineRapporto().isEmpty()) {
            sb.append(rightPad(collegamenti.getRapporto().getDataFineRapporto(), 8, ' '));
        } else {
            sb.append("01010001");
        }

        if (!collegamenti.getSoggetto().getCodiceFiscale().isEmpty()) {
            sb.append("0");
            sb.append(rightPad(collegamenti.getSoggetto().getCodiceFiscale(), 16, ' '));
        } else {
            sb.append("1");
            sb.append(" ".repeat(16));
        }

        sb.append(rightPad(collegamenti.getRuolo(), 1, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getCognome(), 26, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getNome(), 25, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getSesso(), 1, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getDataNascita(), 8, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getComune(), 40, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getProvincia(), 2, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getCognome(), 60, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getComune(), 40, ' '));

        sb.append(rightPad(collegamenti.getSoggetto().getProvincia(), 2, ' '));

        sb.append(" ".repeat(99));

        sb.append("A");

        sb.append(getEndOfLine());

        return sb.toString();
    }

    public static String toCollegamentiOutputString2(Collegamenti collegamenti) {
        StringBuilder sb = new StringBuilder();

        sb.append("3");
        sb.append(rightPad(collegamenti.getChiaveRapporto(), 50, ' '));
        sb.append(rightPad(collegamenti.getRapporto().getTipoRapportoInterno(), 2, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getAnnoDiRiferimento(), 4, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getImportoSaldoFinale(), 4, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getFlagSogliaSaldoFinale(), 1, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getImportoSaldoIniziale(), 17, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getFlagSogliaSaldoIniziale(), 1, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getTotaleOperazioniAttive(), 17, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getFlagSogliaOperazioniAttive(), 1, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getTotaleOperazioniPassive(), 17, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getFlagSogliaOperazioniPassive(), 1, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getAltreInformazioni(), 17, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getGiacenzaMedia(), 17, ' '));

        sb.append(" ");

        sb.append(rightPad(collegamenti.getDatiContabili().getGiacenzaMedia(), 17, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getFlagSogliaGiacenzaMedia(), 1, ' '));

        sb.append(rightPad(collegamenti.getDatiContabili().getDivisa(), 3, ' '));

        sb.append(10);

        sb.append(" ".repeat(17));

        sb.append(1);

        sb.append(" ".repeat(196));

        sb.append("A");

        sb.append(getEndOfLine());

        return sb.toString();
    }

    public static String toDatiContabiliOutputString(DatiContabili datiContabili) {
        StringBuilder sb = new StringBuilder();

        return sb.toString();
    }

    public static String toOutputFileString(ResolvedTransaction transaction) {
        Merchant merchant = transaction.getMerchant();
        StringBuilder sb = new StringBuilder();

        String tipoRecord = transaction.getTpRec();
        sb.append(tipoRecord);

        String codiceFiscale = "04107060966";
        sb.append(rightPad(codiceFiscale, 16, ' '));

        sb.append(transaction.getTipoOpe());

        sb.append(transaction.getDtOpe().toString());

        String totOpeStr = transaction.getTotOpe().toString();
        sb.append(rightPad(totOpeStr, 9, '0'));

        String impOpe = Integer.toString(transaction.getImpOpe().intValue());
        sb.append(rightPad(impOpe, 9, '0'));

        String currency = transaction.getDivisaOpe();
        sb.append(currency);

        sb.append(rightPad(nullSafe(transaction.getIdEsercente()), 30, ' '));

        sb.append(rightPad(nullSafe(transaction.getIdPos()), 30, ' '));

        sb.append(transaction.getTipoPag());

        String codFiscaleEsercente = merchant.getCodFiscale();
        sb.append(rightPad(codFiscaleEsercente, 16, ' '));

        String partitaIvaEsercente = merchant.getPartitaIva();
        sb.append(rightPad(partitaIvaEsercente, 11, ' '));

        String idSalmov = merchant.getIdSalmov();
        sb.append(rightPad(idSalmov, 50, ' '));

        sb.append(" ".repeat(160));

        sb.append('A');

        sb.append(getEndOfLine());

        return sb.toString();
    }

    private static String nullSafe(String value) {
        return value != null ? value : "";
    }

    private static String leftPad(String value, int length, char padChar) {
        if (value == null) {
            value = "";
        }
        if (value.length() >= length) {
            return value.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(length);
        for (int i = value.length(); i < length; i++) {
            sb.append(padChar);
        }
        sb.append(value);
        return sb.toString();
    }

    public static String getEndOfLine() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            return "\r\n";
        } else {
            return "\n";
        }
    }

    private static String rightPad(String value, int length, char padChar) {
        if (value == null) {
            value = "";
        }
        if (value.length() >= length) {
            return value.substring(0, length);
        }
        StringBuilder sb = new StringBuilder(length);
        sb.append(value);
        for (int i = value.length(); i < length; i++) {
            sb.append(padChar);
        }
        return sb.toString();
    }
}


