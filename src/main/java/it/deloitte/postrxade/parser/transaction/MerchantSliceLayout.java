package it.deloitte.postrxade.parser.transaction;

import it.deloitte.postrxade.records.RawRecordSlice;

public class MerchantSliceLayout {
    public static final RawRecordSlice TIPO_RECORD = new RawRecordSlice(0, 1);
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(1, 12);
    public static final RawRecordSlice ID_ESERCENTE = new RawRecordSlice(12, 42);
    public static final RawRecordSlice CODICE_FISCALE_ESERCENTE = new RawRecordSlice(42, 58);
    public static final RawRecordSlice PARTITA_IVA = new RawRecordSlice(58, 69);
    public static final RawRecordSlice ID_SALMOV = new RawRecordSlice(69, 119);
    public static final RawRecordSlice TIPO_SOGGETTO = new RawRecordSlice(119, 120);
    public static final RawRecordSlice DENOMINAZIONE = new RawRecordSlice(120, 190);
    public static final RawRecordSlice DATA_DI_PREDISPOSIZIONE_FLUSSO = new RawRecordSlice(190, 198);
    public static final RawRecordSlice FILLER = new RawRecordSlice(198, 249);
    public static final RawRecordSlice CARATTERE_DI_CONTROLLO = new RawRecordSlice(249, 250);
}