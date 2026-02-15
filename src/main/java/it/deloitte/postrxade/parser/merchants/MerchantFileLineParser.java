package it.deloitte.postrxade.parser.merchants;

import it.deloitte.postrxade.parser.merchants.slices.*;
import it.deloitte.postrxade.parser.merchants.types.*;
import it.deloitte.postrxade.records.RawRecordSlice;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MerchantFileLineParser {
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

    public RapportiRecord parseRapportoLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null rapporto merchant row");
        }

        RapportiRecord rapporto = new RapportiRecord();
        rapporto.setIntermediario(sliceString(line, RapportiSliceLayout.INTERMEDIARIO));
        rapporto.setChiaveRapporto(sliceString(line, RapportiSliceLayout.CHIAVE_RAPPORTO));
        rapporto.setTipoRapportoInterno(sliceString(line, RapportiSliceLayout.TIPO_RAPPORTO_INTERNO));
        rapporto.setFormaTecnica(sliceString(line, RapportiSliceLayout.FORMA_TECNICA));
        rapporto.setFiliale(sliceString(line, RapportiSliceLayout.FILIALE));
        rapporto.setCab(sliceString(line, RapportiSliceLayout.CAB));
        rapporto.setNumeroConto(sliceString(line, RapportiSliceLayout.NUMERO_CONTO));
        rapporto.setCin(sliceString(line, RapportiSliceLayout.CIN));
        rapporto.setDivisa(sliceString(line, RapportiSliceLayout.DIVISA));
        rapporto.setDataInizioRapporto(sliceString(line, RapportiSliceLayout.DATA_INIZIO_RAPPORTO));
        rapporto.setDataFineRapporto(sliceString(line, RapportiSliceLayout.DATA_FINE_RAPPORTO));
        rapporto.setNote(sliceString(line, RapportiSliceLayout.NOTE));
        rapporto.setFlagStatoRapporto(sliceString(line, RapportiSliceLayout.FLAG_STATO_RAPPORTO));
        rapporto.setDataPredisposizione(sliceString(line, RapportiSliceLayout.DATA_PREDISPOSIZIONE));
        rapporto.setFiller(sliceString(line, RapportiSliceLayout.FILLER));
        rapporto.setControlloDiFineRiga(sliceString(line, RapportiSliceLayout.CONTROLLO_DI_FINE_RIGA));

        return rapporto;
    }

    public SoggettiRecord parseSoggettiLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null soggetti row");
        }

        SoggettiRecord soggetti = new SoggettiRecord();
        soggetti.setIntermediario(sliceString(line, SoggettiSliceLayout.INTERMEDIARIO));
        soggetti.setNdg(sliceString(line, SoggettiSliceLayout.NDG));
        soggetti.setDataCensimentoAnagrafico(sliceString(line, SoggettiSliceLayout.DATA_CENSIMENTO_ANAGRAFICO));
        soggetti.setDataEstinzioneAnagrafico(sliceString(line, SoggettiSliceLayout.DATA_ESTINZIONE_ANAGRAFICO));
        soggetti.setFiller1(sliceString(line, SoggettiSliceLayout.FILLER_1));
        soggetti.setFilialeCensimentoAnagrafico(sliceString(line, SoggettiSliceLayout.FILIALE_CENSIMENTO_ANAGRAFICO));
        soggetti.setTipoSoggetto(sliceString(line, SoggettiSliceLayout.TIPO_SOGGETTO));
        soggetti.setNaturaGiuridica(sliceString(line, SoggettiSliceLayout.NATURA_GIURIDICA));
        soggetti.setSesso(sliceString(line, SoggettiSliceLayout.SESSO));
        soggetti.setCodiceFiscale(sliceString(line, SoggettiSliceLayout.CODICE_FISCALE));
        soggetti.setCognome(sliceString(line, SoggettiSliceLayout.COGNOME));
        soggetti.setNome(sliceString(line, SoggettiSliceLayout.NOME));
        soggetti.setDataNascita(sliceString(line, SoggettiSliceLayout.DATA_NASCITA));
        soggetti.setComune(sliceString(line, SoggettiSliceLayout.COMUNE));
        soggetti.setProvincia(sliceString(line, SoggettiSliceLayout.PROVINCIA));
        soggetti.setNazione(sliceString(line, SoggettiSliceLayout.NAZIONE));
        soggetti.setFiller2(sliceString(line, SoggettiSliceLayout.FILLER_2));
        soggetti.setDataPredisposizioneFlusso(sliceString(line, SoggettiSliceLayout.DATA_PREDISPOSIZIONE_FLUSSO));
        soggetti.setFiller3(sliceString(line, SoggettiSliceLayout.FILLER_3));
        soggetti.setControlloDiFineRiga(sliceString(line, SoggettiSliceLayout.CONTROLLO_DI_FINE_RIGA));

        return soggetti;
    }

    public DatiContabiliRecord parseDatiContabiliLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null dati contabili row");
        }

        DatiContabiliRecord contabili = new DatiContabiliRecord();
        contabili.setIntermediario(sliceString(line, DatiContabiliSliceLayout.INTERMEDIARIO));
        contabili.setChiaveRapporto(sliceString(line, DatiContabiliSliceLayout.CHIAVE_RAPPORTO));
        contabili.setAnnoDiRiferimento(sliceString(line, DatiContabiliSliceLayout.ANNO_DI_RIFERIMENTO));
        contabili.setPeriodicita(sliceString(line, DatiContabiliSliceLayout.PERIODICITA));
        contabili.setProgressivoPeriodicita(sliceString(line, DatiContabiliSliceLayout.PROGRESSIVO_PERIODICITA));
        contabili.setDivisa(sliceString(line, DatiContabiliSliceLayout.DIVISA));
        contabili.setDataInizioRiferimento(sliceString(line, DatiContabiliSliceLayout.DATA_INIZIO_RIFERIMENTO));
        contabili.setDataFineRiferimento(sliceString(line, DatiContabiliSliceLayout.DATA_FINE_RIFERIMENTO));
        contabili.setImportoSaldoIniziale(sliceString(line, DatiContabiliSliceLayout.IMPORTO_SALDO_INIZIALE));
        contabili.setImportoSaldoFinale(sliceString(line, DatiContabiliSliceLayout.IMPORTO_SALDO_FINALE));
        contabili.setTotaleOperazioniAttive(sliceString(line, DatiContabiliSliceLayout.TOTALE_OPERAZIONI_ATTIVE));
        contabili.setTotaleOperazioniPassive(sliceString(line, DatiContabiliSliceLayout.TOTALE_OPERAZIONI_PASSIVE));
        contabili.setGiacenzaMedia(sliceString(line, DatiContabiliSliceLayout.GIACENZA_MEDIA));
        contabili.setFlagSogliaSaldoIniziale(sliceString(line, DatiContabiliSliceLayout.FLAG_SOGLIA_SALDO_INIZIALE));
        contabili.setFlagSogliaSaldoFinale(sliceString(line, DatiContabiliSliceLayout.FLAG_SOGLIA_SALDO_FINALE));
        contabili.setFlagSogliaOperazioniAttive(sliceString(line, DatiContabiliSliceLayout.FLAG_SOGLIA_OPERAZIONI_ATTIVE));
        contabili.setFlagSogliaOperazioniPassive(sliceString(line, DatiContabiliSliceLayout.FLAG_SOGLIA_OPERAZIONI_PASSIVE));
        contabili.setFlagSogliaGiacenzaMedia(sliceString(line, DatiContabiliSliceLayout.FLAG_SOGLIA_GIACENZA_MEDIA));
        contabili.setAltreInformazioni(sliceString(line, DatiContabiliSliceLayout.ALTRE_INFORMAZIONI));
        contabili.setFlagStatoImporto(sliceString(line, DatiContabiliSliceLayout.FLAG_STATO_IMPORTO));
        contabili.setDataPredisposizione(sliceString(line, DatiContabiliSliceLayout.DATA_PREDISPOSIZIONE));
        contabili.setFiller1(sliceString(line, DatiContabiliSliceLayout.FILLER_1));
        contabili.setTipoRapportoInterno(sliceString(line, DatiContabiliSliceLayout.TIPO_RAPPORTO_INTERNO));
        contabili.setFormaTecnica(sliceString(line, DatiContabiliSliceLayout.FORMA_TECNICA));
        contabili.setFiller2(sliceString(line, DatiContabiliSliceLayout.FILLER_2));
        contabili.setFlagSogliaAltreInformazioni(sliceString(line, DatiContabiliSliceLayout.FLAG_SOGLIA_ALTRE_INFORMAZIONI));
        contabili.setFiller3(sliceString(line, DatiContabiliSliceLayout.FILLER_3));
        contabili.setControlloDiFineRiga(sliceString(line, DatiContabiliSliceLayout.CONTROLLO_DI_FINE_RIGA));

        return contabili;
    }

    public CambioNdgRecord parseCambioNdgLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null cambio NDG row");
        }

        CambioNdgRecord cambio = new CambioNdgRecord();
        cambio.setIntermediario(sliceString(line, CambioNdgSliceLayout.INTERMEDIARIO));
        cambio.setIntermediario(sliceString(line, CambioNdgSliceLayout.INTERMEDIARIO));
        cambio.setNdgVecchio(sliceString(line, CambioNdgSliceLayout.NDG_VECCHIO));
        cambio.setNdgNuovo(sliceString(line, CambioNdgSliceLayout.NDG_NUOVO));
        cambio.setFiller(sliceString(line, CambioNdgSliceLayout.FILLER));
        cambio.setControlloDiFineRiga(sliceString(line, CambioNdgSliceLayout.CONTROLLO_DI_FINE_RIGA));

        return cambio;
    }

    public CollegamentiRecord parseCollegamentiLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot parse a null collegamenti row");
        }

        CollegamentiRecord collegamenti = new CollegamentiRecord();
        collegamenti.setIntermediario(sliceString(line, CollegamentiSliceLayout.INTERMEDIARIO));
        collegamenti.setChiaveRapporto(sliceString(line, CollegamentiSliceLayout.CHIAVE_RAPPORTO));
        collegamenti.setNdg(sliceString(line, CollegamentiSliceLayout.NDG));
        collegamenti.setRuolo(sliceString(line, CollegamentiSliceLayout.RUOLO));
        collegamenti.setDataInizioCollegamento(sliceString(line, CollegamentiSliceLayout.DATA_INIZIO_COLLEGAMENTO));
        collegamenti.setDataFineCollegamento(sliceString(line, CollegamentiSliceLayout.DATA_FINE_COLLEGAMENTO));
        collegamenti.setRuoloInterno(sliceString(line, CollegamentiSliceLayout.RUOLO_INTERNO));
        collegamenti.setFlagStatoCollegamento(sliceString(line, CollegamentiSliceLayout.FLAG_STATO_COLLEGAMENTO));
        collegamenti.setDataPredisposizioneFlusso(sliceString(line, CollegamentiSliceLayout.DATA_PREDISPOSIZIONE_FLUSSO));
        collegamenti.setFiller(sliceString(line, CollegamentiSliceLayout.FILLER));
        collegamenti.setControlloDiFineRiga(sliceString(line, CollegamentiSliceLayout.CONTROLLO_DI_FINE_RIGA));

        return collegamenti;
    }
}
