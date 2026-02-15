package it.deloitte.postrxade.parser.merchants.slices;

import it.deloitte.postrxade.records.RawRecordSlice;

public class DatiContabiliSliceLayout {
    public static final RawRecordSlice INTERMEDIARIO = new RawRecordSlice(0, 11);
    public static final RawRecordSlice CHIAVE_RAPPORTO = new RawRecordSlice(11, 61);
    public static final RawRecordSlice ANNO_DI_RIFERIMENTO = new RawRecordSlice(61, 65);
    public static final RawRecordSlice PERIODICITA = new RawRecordSlice(65, 68);
    public static final RawRecordSlice PROGRESSIVO_PERIODICITA = new RawRecordSlice(68, 71);
    public static final RawRecordSlice DIVISA = new RawRecordSlice(71, 74);
    public static final RawRecordSlice DATA_INIZIO_RIFERIMENTO = new RawRecordSlice(74, 82);
    public static final RawRecordSlice DATA_FINE_RIFERIMENTO = new RawRecordSlice(82, 90);
    public static final RawRecordSlice IMPORTO_SALDO_INIZIALE = new RawRecordSlice(90, 108);
    public static final RawRecordSlice IMPORTO_SALDO_FINALE = new RawRecordSlice(108, 126);
    public static final RawRecordSlice TOTALE_OPERAZIONI_ATTIVE = new RawRecordSlice(126, 144);
    public static final RawRecordSlice TOTALE_OPERAZIONI_PASSIVE = new RawRecordSlice(144, 162);
    public static final RawRecordSlice GIACENZA_MEDIA = new RawRecordSlice(162, 180);
    public static final RawRecordSlice FLAG_SOGLIA_SALDO_INIZIALE = new RawRecordSlice(180, 181);
    public static final RawRecordSlice FLAG_SOGLIA_SALDO_FINALE = new RawRecordSlice(181, 182);
    public static final RawRecordSlice FLAG_SOGLIA_OPERAZIONI_ATTIVE = new RawRecordSlice(182, 183);
    public static final RawRecordSlice FLAG_SOGLIA_OPERAZIONI_PASSIVE = new RawRecordSlice(183, 184);
    public static final RawRecordSlice FLAG_SOGLIA_GIACENZA_MEDIA = new RawRecordSlice(184, 185);
    public static final RawRecordSlice ALTRE_INFORMAZIONI = new RawRecordSlice(185, 203);
    public static final RawRecordSlice FLAG_STATO_IMPORTO = new RawRecordSlice(203, 204);
    public static final RawRecordSlice DATA_PREDISPOSIZIONE = new RawRecordSlice(204, 212);
    public static final RawRecordSlice FILLER_1 = new RawRecordSlice(212, 215);
    public static final RawRecordSlice TIPO_RAPPORTO_INTERNO = new RawRecordSlice(215, 218);
    public static final RawRecordSlice FORMA_TECNICA = new RawRecordSlice(218, 223);
    public static final RawRecordSlice FILLER_2 = new RawRecordSlice(223, 228);
    public static final RawRecordSlice FLAG_SOGLIA_ALTRE_INFORMAZIONI = new RawRecordSlice(228, 229);
    public static final RawRecordSlice FILLER_3 = new RawRecordSlice(229, 249);
    public static final RawRecordSlice CONTROLLO_DI_FINE_RIGA = new RawRecordSlice(249, 250);
}