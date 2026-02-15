package it.deloitte.postrxade.parser.merchants.slices;

import it.deloitte.postrxade.records .RawRecordSlice;

public class CambioNdgSliceLayout {
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(0, 11);
    public static final RawRecordSlice NDG_VECCHIO = new RawRecordSlice(11, 27);
    public static final RawRecordSlice NDG_NUOVO = new RawRecordSlice(27, 43);
    public static final RawRecordSlice FILLER = new RawRecordSlice(43, 99);
    public static final RawRecordSlice CONTROLLO_DI_FINE_RIGA = new RawRecordSlice(99, 100);
}