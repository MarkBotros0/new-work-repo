package it.deloitte.postrxade.parser.transaction;

import it.deloitte.postrxade.records.RawRecordSlice;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class FileLineParser {
    private String sliceString(String line, RawRecordSlice s) {
        if (line == null || line.length() <= s.start()) {
            return "";
        }
        int end = Math.min(line.length(), s.end());
        return line.substring(s.start(), end).trim();
    }
    private String parseDate(String line, RawRecordSlice s) {
        String input = sliceString(line, s);
        DateTimeFormatter inFmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter outFmt = DateTimeFormatter.ofPattern("ddMMyyyy");

        return LocalDate.parse(input, inFmt).format(outFmt);
    }

    public TransactionRecord parseTransaction(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null transaction row");
        }

        TransactionRecord trRec = new TransactionRecord();
        trRec.setTipoRecord(sliceString(line, TransactionSliceLayout.TIPO_RECORD));
        trRec.setIntermediario(sliceString(line, TransactionSliceLayout.INTERMEDIARIO));
        trRec.setIdEsercente(sliceString(line, TransactionSliceLayout.ID_ESERCENTE));
        trRec.setChiaveRapportoBanca(sliceString(line, TransactionSliceLayout.CHIAVE_RAPPORTO_BANCA));
        trRec.setTerminalId(sliceString(line, TransactionSliceLayout.TERMINAL_ID));
        trRec.setTipoOperazione(sliceString(line, TransactionSliceLayout.TIPO_OPERAZIONE));
        trRec.setDataOperazione(sliceString(line, TransactionSliceLayout.DATA_OPERAZIONE));
        trRec.setDivisaOperazioni(sliceString(line, TransactionSliceLayout.DIVISA_OPERAZIONI));
        trRec.setTipoPagamento(sliceString(line, TransactionSliceLayout.TIPO_PAGAMENTO));
        trRec.setImportoTotaleOperazioni(sliceString(line, TransactionSliceLayout.IMPORTO_TOTALE_OPERAZIONI));
        trRec.setNumeroOperazioniGiorno(sliceString(line, TransactionSliceLayout.NUMERO_OPERAZIONI_GIORNO));
        trRec.setStatoOperazioni(sliceString(line, TransactionSliceLayout.STATO_OPERAZIONI));
        trRec.setDataPredisposizioneFlusso(sliceString(line, TransactionSliceLayout.DATA_PREDISPOSIZIONE_FLUSSO));
        trRec.setFiller(sliceString(line, TransactionSliceLayout.FILLER));
        trRec.setCarattereDiControllo(sliceString(line, TransactionSliceLayout.CARATTERE_DI_CONTROLLO));

        return trRec;
    }

    public MerchantRecord parseMerchant(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null transaction row");
        }

        MerchantRecord merRec = new MerchantRecord();
        merRec.setTipoRecord(sliceString(line, MerchantSliceLayout.TIPO_RECORD));
        merRec.setIntermediario(sliceString(line, MerchantSliceLayout.INTERMEDIARIO));
        merRec.setIdEsercente(sliceString(line, MerchantSliceLayout.ID_ESERCENTE));
        merRec.setPartitaIva(sliceString(line, MerchantSliceLayout.PARTITA_IVA));
        merRec.setIdSalmov(sliceString(line, MerchantSliceLayout.ID_SALMOV));
        merRec.setCodiceFiscaleEsercente(sliceString(line, MerchantSliceLayout.CODICE_FISCALE_ESERCENTE));
        merRec.setFiller(sliceString(line, MerchantSliceLayout.FILLER));
        merRec.setCarattereDiControllo(sliceString(line, MerchantSliceLayout.CARATTERE_DI_CONTROLLO));

        return merRec;
    }
}
