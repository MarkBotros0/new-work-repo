package it.deloitte.postrxade.parser.merchants.slices;

import it.deloitte.postrxade.records.RawRecordSlice;

public class CollegamentiSliceLayout {
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(0, 11);
    public static final RawRecordSlice CHIAVE_RAPPORTO = new RawRecordSlice(11, 61);
    public static final RawRecordSlice NDG = new RawRecordSlice(61, 77);
    public static final RawRecordSlice RUOLO = new RawRecordSlice(77, 78);
    public static final RawRecordSlice DATA_INIZIO_COLLEGAMENTO = new RawRecordSlice(78, 86);
    public static final RawRecordSlice DATA_FINE_COLLEGAMENTO = new RawRecordSlice(86, 94);
    public static final RawRecordSlice RUOLO_INTERNO = new RawRecordSlice(94, 97);
    public static final RawRecordSlice FLAG_STATO_COLLEGAMENTO = new RawRecordSlice(97, 98);
    public static final RawRecordSlice DATA_PREDISPOSIZIONE_FLUSSO = new RawRecordSlice(98, 106);
    public static final RawRecordSlice FILLER = new RawRecordSlice(106, 129);
    public static final RawRecordSlice CONTROLLO_DI_FINE_RIGA = new RawRecordSlice(129, 130);
}