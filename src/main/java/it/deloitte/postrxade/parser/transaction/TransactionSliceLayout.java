package it.deloitte.postrxade.parser.transaction;

import it.deloitte.postrxade.records.RawRecordSlice;

public class TransactionSliceLayout {
    public static final RawRecordSlice TIPO_RECORD = new RawRecordSlice(0, 1);
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(1, 12);
    public static final RawRecordSlice CHIAVE_RAPPORTO_BANCA = new RawRecordSlice(12, 42);
    public static final RawRecordSlice ID_ESERCENTE = new RawRecordSlice(42, 92);
    public static final RawRecordSlice TERMINAL_ID = new RawRecordSlice(92, 122);
    public static final RawRecordSlice TIPO_OPERAZIONE = new RawRecordSlice(122, 124);
    public static final RawRecordSlice DATA_OPERAZIONE = new RawRecordSlice(124, 132);
    public static final RawRecordSlice DIVISA_OPERAZIONI = new RawRecordSlice(132, 135);
    public static final RawRecordSlice TIPO_PAGAMENTO = new RawRecordSlice(135, 137);
    public static final RawRecordSlice IMPORTO_TOTALE_OPERAZIONI = new RawRecordSlice(137, 151);
    public static final RawRecordSlice NUMERO_OPERAZIONI_GIORNO = new RawRecordSlice(151, 161);
    public static final RawRecordSlice STATO_OPERAZIONI = new RawRecordSlice(161, 162);
    public static final RawRecordSlice DATA_PREDISPOSIZIONE_FLUSSO = new RawRecordSlice(162, 170);
    public static final RawRecordSlice FILLER = new RawRecordSlice(170, 249);
    public static final RawRecordSlice CARATTERE_DI_CONTROLLO = new RawRecordSlice(249, 250);
}