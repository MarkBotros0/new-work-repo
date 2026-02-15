package it.deloitte.postrxade.parser.merchants.slices;

import it.deloitte.postrxade.records.RawRecordSlice;

public class SoggettiSliceLayout {
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(0, 11);
    public static final RawRecordSlice NDG = new RawRecordSlice(11, 27);
    public static final RawRecordSlice DATA_CENSIMENTO_ANAGRAFICO = new RawRecordSlice(27, 35);
    public static final RawRecordSlice DATA_ESTINZIONE_ANAGRAFICO = new RawRecordSlice(35, 43);
    public static final RawRecordSlice FILLER_1 = new RawRecordSlice(43, 63);
    public static final RawRecordSlice FILIALE_CENSIMENTO_ANAGRAFICO = new RawRecordSlice(63, 68);
    public static final RawRecordSlice TIPO_SOGGETTO = new RawRecordSlice(68, 69);
    public static final RawRecordSlice NATURA_GIURIDICA = new RawRecordSlice(69, 74);
    public static final RawRecordSlice SESSO = new RawRecordSlice(74, 75);
    public static final RawRecordSlice CODICE_FISCALE = new RawRecordSlice(75, 91);
    public static final RawRecordSlice COGNOME = new RawRecordSlice(91, 166);
    public static final RawRecordSlice NOME = new RawRecordSlice(166, 241);
    public static final RawRecordSlice DATA_NASCITA = new RawRecordSlice(241, 249);
    public static final RawRecordSlice COMUNE = new RawRecordSlice(249, 289);
    public static final RawRecordSlice PROVINCIA = new RawRecordSlice(289, 291);
    public static final RawRecordSlice NAZIONE = new RawRecordSlice(291, 331);
    public static final RawRecordSlice FILLER_2 = new RawRecordSlice(331, 332);
    public static final RawRecordSlice DATA_PREDISPOSIZIONE_FLUSSO = new RawRecordSlice(332, 340);
    public static final RawRecordSlice FILLER_3 = new RawRecordSlice(340, 349);
    public static final RawRecordSlice CONTROLLO_DI_FINE_RIGA = new RawRecordSlice(349, 350);
}