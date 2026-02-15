package it.deloitte.postrxade.parser.merchants.slices;

import it.deloitte.postrxade.records.RawRecordSlice;

public class RapportiSliceLayout {
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(0, 11);
    public static final RawRecordSlice CHIAVE_RAPPORTO = new RawRecordSlice(11, 61);
    public static final RawRecordSlice TIPO_RAPPORTO_INTERNO = new RawRecordSlice(61, 64);
    public static final RawRecordSlice FORMA_TECNICA = new RawRecordSlice(64, 69);
    public static final RawRecordSlice FILIALE = new RawRecordSlice(69, 74);
    public static final RawRecordSlice CAB = new RawRecordSlice(74, 79);
    public static final RawRecordSlice NUMERO_CONTO = new RawRecordSlice(79, 106);
    public static final RawRecordSlice CIN = new RawRecordSlice(106, 108);
    public static final RawRecordSlice DIVISA = new RawRecordSlice(108, 111);
    public static final RawRecordSlice DATA_INIZIO_RAPPORTO = new RawRecordSlice(111, 119);
    public static final RawRecordSlice DATA_FINE_RAPPORTO = new RawRecordSlice(119, 127);
    public static final RawRecordSlice NOTE = new RawRecordSlice(127, 151);
    public static final RawRecordSlice FLAG_STATO_RAPPORTO = new RawRecordSlice(151, 152);
    public static final RawRecordSlice DATA_PREDISPOSIZIONE = new RawRecordSlice(152, 160);
    public static final RawRecordSlice FILLER = new RawRecordSlice(160, 279);
    public static final RawRecordSlice CONTROLLO_DI_FINE_RIGA = new RawRecordSlice(279, 280);
}