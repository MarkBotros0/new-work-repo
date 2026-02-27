package it.deloitte.postrxade.parser.merchants;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;

import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.parser.merchants.types.*;
import it.deloitte.postrxade.parser.merchants.types.CambioNdgRecord;
import it.deloitte.postrxade.enums.ErrorTypeCode;
import it.deloitte.postrxade.records.ErrorRecordCause;
import jakarta.validation.ValidationException;

public class MerchantFileLineValidator {

    // Regex Patterns
    private static final Pattern NUMERIC_REGEX = Pattern.compile("^[0-9]+$");
    private static final Pattern ALPHANUMERIC_REGEX = Pattern.compile("^[a-zA-Z0-9]+$");
    private static final Pattern WORD_REGEX = Pattern.compile("^\\w+$");


    // Error Messages
    private static final String INVALID_FORMAT_ERROR_MSG = "%s '%s' does not match expected format '%s'";
    private static final String NULL_VALUE_ERROR_MSG = "%s cannot be null or empty";
    private static final String LENGTH_MISMATCH_ERROR_MSG = "%s expected length %d but got %d";
    private static final String INVALID_VALUE_ERROR_MSG = "%s '%s' does not match expected value [ %s ]";
    private static final String MANDATORY_DATA_IS_MISSING_ERROR_MSG = "%s '%s' does not match expected length (%d)";
    private static final String INVALID_DATE_ERROR_MSG = "%s '%s' does not match expected date format [ %s ]";

    // Error Codes
    private static final String INVALID_FORMAT_ERROR_CODE = ErrorTypeCode.INVALID_FORMAT.getErrorCode();
    private static final String MANDATORY_DATA_IS_MISSING_ERROR_CODE = ErrorTypeCode.MANDATORY_DATA_IS_MISSING.getErrorCode();
    private static final String INVALID_VALUE_ERROR_CODE = ErrorTypeCode.INVALID_VALUE.getErrorCode();
    private static final String INVALID_DATE_ERROR_CODE = ErrorTypeCode.INVALID_DATE_FORMAT.getErrorCode();
    private static final Set<String> VALID_CAB_STRING_CODES = new HashSet<>();
    private static final Set<String> VALID_CAB_NUMBER_CODES = new HashSet<>();

    static {
        VALID_CAB_STRING_CODES.addAll(Arrays.asList(
                "AFG", "ALB", "DZA", "AND", "AGO", "ΑΙΑ", "ATA", "ATG", "ANT", "SAU", "ARG", "ARM", "ABW", "AUS", "AUT", "AZE",
                "BHS", "BHR", "BGD", "BRB", "BEL", "BLZ", "BEN", "BMU", "BLR", "BTN", "BOL", "BIH", "BWA", "BRA", "BRN", "BGR",
                "BFA", "BDI", "KHM", "CMR", "CAN", "CPV", "TCD", "CHL", "CHN", "CYP", "VAT", "COL", "COM", "PRK", "KOR", "CRI",
                "CIV", "HRV", "CUB", "DNK", "DMA", "ECU", "EGY", "IRL", "SLV", "ARE", "ERI", "EST", "ETH", "RUS", "FJI", "PHL",
                "FIN", "FRA", "GAB", "GMB", "GEO", "DEU", "GHA", "JAM", "JPN", "GIB", "DJI", "JOR", "GRC", "GRD", "GRL", "GLP",
                "GUM", "GTM", "GIN", "GNB", "GNQ", "GUY", "GUF", "HTI", "HND", "HKG", "IND", "IDN", "IRN", "IRQ", "BVT", "CXR",
                "HMD", "CYM", "CCK", "COK", "FLK", "FRO", "MHL", "MNP", "UMI", "NFK", "SLB", "TCA", "VIR", "VGB", "ISR", "ISL",
                "ITA", "KAZ", "KEN", "KGZ", "KIR", "KWT", "LAO", "LVA", "LSO", "LBN", "LBR", "LBY", "LIE", "LTU", "LUX", "MAC",
                "MKD", "MDG", "MWI", "MDV", "MYS", "MLI", "MLT", "MAR", "MTQ", "MRT", "MUS", "MYT", "MEX", "MDA", "MCO", "MNG",
                "MSR", "MOZ", "MMR", "NAM", "NRU", "NPL", "NIC", "NER", "NGA", "NIU", "NOR", "NCL", "NZL", "OMN", "NLD", "PAK",
                "PLW", "PAN", "PNG", "PRY", "PER", "PCN", "PYF", "POL", "PRT", "PRI", "QAT", "GBR", "CZE", "CAF", "COG", "COD",
                "DOM", "REU", "ROU", "RWA", "ESH", "KNA", "SPM", "VCT", "WSM", "ASM", "SMR", "SHN", "LCA", "STP", "SEN", "SCG",
                "SYC", "SLE", "SGP", "SYR", "SVK", "SVN", "SOM", "ESP", "LKA", "FSM", "USA", "ZAF", "SGS", "SDN", "SUR", "SJM",
                "SWE", "CHE", "SWZ", "TIK", "THA", "TWN", "TZA", "IOT", "ATF", "PSE", "TLS", "TGO", "TKL", "TON", "TTO", "TUN",
                "TUR", "TKM", "TUV", "UKR", "UGA", "HUN", "URY", "UZB", "VUT", "VEN", "VNM", "WLF", "YEM", "ZMB", "ZWE"
        ));

        VALID_CAB_NUMBER_CODES.addAll(Arrays.asList(
                "004","008","012","020","024","660","010","028","530","682","032","051","533","036","040","031",
                "044","048","050","052","056","084","204","060","112","064","068","070","072","076","096","100",
                "854","108","116","120","124","132","148","152","156","196","336","170","174","408","410","188",
                "384","191","192","208","212","218","818","372","222","784","232","233","231","643","242","608",
                "246","250","266","270","268","276","288","388","392","292","262","400","300","308","304","312",
                "316","320","324","624","226","328","254","332","340","344","356","360","364","368","074","162",
                "334","136","166","184","238","234","584","580","581","574","090","796","850","092","376","352",
                "380","398","404","417","296","414","418","428","426","422","430","434","438","440","442","446",
                "807","450","454","462","458","466","470","504","474","478","480","175","484","498","492","496",
                "500","508","104","516","520","524","558","562","566","570","578","540","554","512","528","586",
                "585","591","598","600","604","612","258","616","620","630","634","826","203","140","178","180",
                "214","638","642","646","732","659","666","670","882","016","674","654","662","678","686","891",
                "690","694","702","760","703","705","706","724","144","583","840","710","239","736","740","744",
                "752","756","748","762","764","158","834","092","260","275","626","768","772","776","780","788",
                "792","795","798","804","800","348","858","860","548","862","704","876","887","894","716"
        ));
    }

    // Date Formats
    DateTimeFormatter inFmt = DateTimeFormatter.ofPattern("yyyyMMdd");
    DateTimeFormatter outFmt = DateTimeFormatter.ofPattern("ddMMyyyy");

    public List<ErrorRecordCause> validateRapporto(RapportiRecord record) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateIntermediario(record.getIntermediario()));
        errors.addAll(validateChiaveRapporto(record.getChiaveRapporto()));
        errors.addAll(validateTipoRapportoInterno(record.getTipoRapportoInterno()));
        errors.addAll(validateFormaTecnica(record.getFormaTecnica()));
        errors.addAll(validateFiliale(record.getFiliale()));
        errors.addAll(validateCab(record.getCab(), record.getTipoRapportoInterno()));
        errors.addAll(validateNumeroConto(record.getNumeroConto()));
        errors.addAll(validateCin(record.getCin()));
        errors.addAll(validateDivisa(record.getDivisa()));

        List<ErrorRecordCause> dateErrors = validateDataInizioRapporto(record.getDataInizioRapporto());
        errors.addAll(dateErrors);
        if (dateErrors.isEmpty())
            record.setDataInizioRapporto(LocalDate.parse(record.getDataInizioRapporto(), inFmt).format(outFmt));


        errors.addAll(validateDataFineRapporto(record.getDataFineRapporto()));
        errors.addAll(validateNote(record.getNote()));
        errors.addAll(validateFlagStatoRapporto(record.getFlagStatoRapporto()));
        errors.addAll(validateDataPredisposizione(record.getDataPredisposizione()));
        errors.addAll(validateControlloFinRiga(record.getControlloDiFineRiga()));

        return errors;
    }

    private List<ErrorRecordCause> validateIntermediario(String intermediario) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Intermediario";
        int expectedLength = 11;

        if (intermediario == null || intermediario.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (intermediario.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, intermediario.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!NUMERIC_REGEX.matcher(intermediario).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, intermediario, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateChiaveRapporto(String chiaveRapporto) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Chiave Rapporto";
        int expectedLength = 50;
        Pattern expectedPattern = Pattern.compile("^[A-Z0-9][A-Z0-9_]* *$");

        if (chiaveRapporto == null || chiaveRapporto.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (chiaveRapporto.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, chiaveRapporto.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!expectedPattern.matcher(chiaveRapporto).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, chiaveRapporto, expectedPattern.pattern()),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateNdg(String ndg) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "NDG";
        int expectedLength = 16;

        if (ndg == null || ndg.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (ndg.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, ndg.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

//        if (!NUMERIC_REGEX.matcher(ndg).matches()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, ndg, "numeric"),
//                    INVALID_FORMAT_ERROR_CODE));
//        }
        return errors;
    }

    private List<ErrorRecordCause> validateNdgVecchio(String ndgVecchio) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "NDG Vecchio";
        int expectedLength = 16;

        if (ndgVecchio == null || ndgVecchio.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (ndgVecchio.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, ndgVecchio.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!WORD_REGEX.matcher(ndgVecchio).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, ndgVecchio, "alphanumeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateNdgNuovo(String ndgNuovo) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "NDG Nuovo";
        int expectedLength = 16;

        if (ndgNuovo == null || ndgNuovo.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (ndgNuovo.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, ndgNuovo.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!WORD_REGEX.matcher(ndgNuovo).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, ndgNuovo, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTipoRapportoInterno(String tipoRapporto) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Tipo Rapporto Interno";
        int expectedLength = 3;

        if (tipoRapporto == null || tipoRapporto.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (tipoRapporto.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, tipoRapporto.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!NUMERIC_REGEX.matcher(tipoRapporto).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, tipoRapporto, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateFormaTecnica(String formaTecnica) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Forma Tecnica";
        int expectedLength = 5;

        if (formaTecnica == null || formaTecnica.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (formaTecnica.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, formaTecnica.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!NUMERIC_REGEX.matcher(formaTecnica).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, formaTecnica, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateFiliale(String filiale) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Filiale";
        int expectedLength = 5;

        if (filiale.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, filiale.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateNumeroConto(String numeroConto) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Numero Conto";
        int expectedLength = 27;

//        if (numeroConto == null || numeroConto.isEmpty()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
//                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
//            return errors;
//        }

        if (numeroConto.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, numeroConto.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateCin(String cin) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "CIN";
        int expectedLength = 2;

        if (cin == null || cin.isEmpty()) {
            // CIN is optional, so return empty if null or empty
            return errors;
        }

        if (cin.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, cin.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDivisa(String divisa) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Divisa";
        int expectedLength = 3;

        if (divisa == null || divisa.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (divisa.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, divisa.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!"EUR".equals(divisa)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, divisa, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataInizioRapporto(String dataInizio) {
        return validateDateField(dataInizio, "Data inizio rapporto");
    }

    private List<ErrorRecordCause> validateDataFineRapporto(String dataFine) {
//        return validateDateField(dataFine, "Data fine rapporto");
        return Collections.emptyList();
    }

    private List<ErrorRecordCause> validateNote(String note) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Note";
        int expectedLength = 24;

        if (note == null || note.isEmpty()) {
            // Note is optional
            return errors;
        }

        if (note.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format("%s exceeds maximum length %d (got %d)", fieldName, expectedLength, note.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateFlagStatoRapporto(String flagStato) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Flag Stato Rapporto";
        Set<String> allowed = Set.of(" ", "C");

        if (flagStato == null || flagStato.isEmpty()) {
            // Optional field
            return errors;
        }

        if (!allowed.contains(flagStato)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, flagStato, "C (cancel) or space"),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataPredisposizione(String dataPredisposizione) {
        return validateDateField(dataPredisposizione, "Data predisposizione");
    }

    private List<ErrorRecordCause> validateDateField(String dateValue, String fieldName) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        int expectedLength = 8;
        Pattern exepectedPattern = NUMERIC_REGEX;

        if (!exepectedPattern.matcher(dateValue).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, dateValue, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, INVALID_FORMAT_ERROR_CODE));
        }
        if (dateValue.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, dateValue, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        DateTimeFormatter inFmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        try {
            LocalDate date = LocalDate.parse(dateValue, inFmt);
        } catch (DateTimeParseException e) {
            // Se il parsing fallisce, è un errore di formato data (WRN2)
            String errorDescription = String.format(INVALID_DATE_ERROR_MSG, fieldName, dateValue, inFmt);
            errors.add(new ErrorRecordCause(errorDescription, INVALID_DATE_ERROR_CODE));
        }
        return errors;

    }

    private List<ErrorRecordCause> validateCab(String cab, String tipoRapporto) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "CAB";
        Integer tipoRapportoParsed = Integer.parseInt(tipoRapporto);

        Set<Integer> validTipoRapporto = new HashSet<>();
        validTipoRapporto.add(1);
        validTipoRapporto.add(2);
        validTipoRapporto.add(3);
        validTipoRapporto.add(12);
        validTipoRapporto.add(13);
        validTipoRapporto.add(96);
        validTipoRapporto.add(97);

        // CAB must be non-empty for specific rapport types
        if (cab == null || cab.isEmpty()) {
            if (validTipoRapporto.contains(tipoRapportoParsed)) {
                errors.add(new ErrorRecordCause(
                        String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, cab, 3),
                        MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            }
            return errors;
        }

        // If CAB is populated, validate it against ISO code tables
        // Check if it's made of letters (ISO country codes)
        if (cab.matches("^[A-Z]+$")) {
            if (!VALID_CAB_STRING_CODES.contains(cab)) {
                errors.add(new ErrorRecordCause(
                        String.format(INVALID_VALUE_ERROR_MSG, fieldName, cab, "[ITA, AFG, ALB, ...]"),
                        INVALID_VALUE_ERROR_CODE));
            }
        }
        // Check if it's made of numbers (ISO numeric codes)
        else if (cab.matches("^[0-9]+$")) {
            if (!VALID_CAB_NUMBER_CODES.contains(cab)) {
                errors.add(new ErrorRecordCause(
                        String.format(INVALID_VALUE_ERROR_MSG, fieldName, cab, "[004, 008, 012, ...]"),
                        INVALID_VALUE_ERROR_CODE));
            }
        }
        // If it's neither all letters nor all numbers, it's invalid
        else {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, cab, "ISO country code (letters or numbers)"),
                    INVALID_FORMAT_ERROR_CODE));
        }

        return errors;
    }

    private List<ErrorRecordCause> validateNumericField(String value, String fieldName, int expectedLength) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        if (value == null || value.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (value.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, value.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!NUMERIC_REGEX.matcher(value).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, value, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    public List<ErrorRecordCause> validateSoggetti(SoggettiRecord record, Set<String> map) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateIntermediario(record.getIntermediario()));
        errors.addAll(validateNdg(record.getNdg()));
        errors.addAll(validateDataCensimentoAnagrafico(record.getDataCensimentoAnagrafico()));
        errors.addAll(validateDataEstinzioneAnagrafica(record.getDataEstinzioneAnagrafico()));
        errors.addAll(validateFilialeCensimentoAnagrafico(record.getFilialeCensimentoAnagrafico()));
        errors.addAll(validateTipoSoggetto(record.getTipoSoggetto()));
        errors.addAll(validateNaturaGiuridica(record.getNaturaGiuridica()));
        errors.addAll(validateSesso(record.getSesso()));
        errors.addAll(validateCodiceFiscale(record.getCodiceFiscale()));
        errors.addAll(validateCognome(record.getCognome()));
        errors.addAll(validateNome(record.getNome()));

        List<ErrorRecordCause> dateErrors = validateDataNascita(record.getDataNascita());
        errors.addAll(dateErrors);
        if (dateErrors.isEmpty())
            record.setDataNascita(LocalDate.parse(record.getDataNascita(), inFmt).format(outFmt));

        errors.addAll(validateComune(record.getComune()));
        errors.addAll(validateProvincia(record.getProvincia()));
        errors.addAll(validateNazione(record.getNazione()));
        errors.addAll(validateDataPredisposizioneFlusso(record.getDataPredisposizioneFlusso()));
        errors.addAll(validateControlloFinRiga(record.getControlloDiFineRiga()));

        return errors;
    }

    private List<ErrorRecordCause> validateDataCensimentoAnagrafico(String dataCensimento) {
        return validateDateField(dataCensimento, "Data censimento anagrafico");
    }

    private List<ErrorRecordCause> validateDataEstinzioneAnagrafica(String dataEstinzione) {
//        return validateDateField(dataEstinzione, "Data estinzione anagrafica");
        return Collections.emptyList();
    }

    private List<ErrorRecordCause> validateFilialeCensimentoAnagrafico(String filiale) {
//        return validateNumericField(filiale, "Filiale censimento anagrafico", 5);
        return Collections.emptyList();
    }

    private List<ErrorRecordCause> validateTipoSoggetto(String tipoSoggetto) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Tipo Soggetto";
        Set<String> allowed = Set.of("F", "G");

        if (tipoSoggetto == null || tipoSoggetto.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (tipoSoggetto.length() != 1) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, 1, tipoSoggetto.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        } else if (!allowed.contains(tipoSoggetto)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, tipoSoggetto, "F (fisica) or G (giuridica)"),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateNaturaGiuridica(String naturaGiuridica) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Natura Giuridica";
        int expectedLength = 5;

        if (naturaGiuridica == null || naturaGiuridica.isEmpty()) {
            // Optional field for persone fisiche
            return errors;
        }

        if (naturaGiuridica.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, naturaGiuridica.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!NUMERIC_REGEX.matcher(naturaGiuridica).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, naturaGiuridica, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateSesso(String sesso) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Sesso";
        Set<String> allowed = Set.of(" ", "M", "F");

        if (sesso == null || sesso.isEmpty()) {
            // Optional field
            return errors;
        }

        if (sesso.length() != 1) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, 1, sesso.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        } else if (!allowed.contains(sesso)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, sesso, "M (maschio), F (femmina), or space"),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateCognome(String cognome) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Cognome";
        int expectedLength = 75;

        if (cognome == null || cognome.isEmpty()) {
            // Optional field
            return errors;
        }

        if (cognome.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format("%s exceeds maximum length %d (got %d)", fieldName, expectedLength, cognome.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateNome(String nome) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Nome";
        int expectedLength = 75;

        if (nome == null || nome.isEmpty()) {
            // Optional field
            return errors;
        }

        if (nome.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format("%s exceeds maximum length %d (got %d)", fieldName, expectedLength, nome.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataNascita(String dataNascita) {
        return validateDateField(dataNascita, "Data Nascita");
    }

    private List<ErrorRecordCause> validateComune(String comune) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Comune";
        int expectedLength = 40;

//        if (comune == null || comune.isEmpty()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
//                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
//            return errors;
//        }

        if (comune.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, comune.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateProvincia(String provincia) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Provincia";
        int expectedLength = 2;

//        if (provincia == null || provincia.isEmpty()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
//                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
//            return errors;
//        }

        if (provincia.isEmpty()) {
            return Collections.emptyList();
        }

        if (provincia.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, provincia.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        } else if (!Pattern.matches("^[A-Z0-9]{2}$", provincia)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, provincia, "^[A-Z0-9]{2}$"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateNazione(String nazione) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Nazione";
        int expectedLength = 40;

        if (nazione == null || nazione.isEmpty()) {
            // Optional field
            return errors;
        }

        if (nazione.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format("%s exceeds maximum length %d (got %d)", fieldName, expectedLength, nazione.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataPredisposizioneFlusso(String dataPredisposizione) {
        return validateDateField(dataPredisposizione, "Data predisposizione flusso");
    }


    private List<ErrorRecordCause> validateRuolo(String ruolo) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Ruolo";
        Set<String> validRoles = Set.of("T", "C", "I", "D", "O", "E", "G", "A");

        if (ruolo == null || ruolo.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (ruolo.length() != 1) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, 1, ruolo.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        } else if (!validRoles.contains(ruolo)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, ruolo, String.join(", ", validRoles)),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataInizioCollegamento(String dataInizio) {
        return validateDateField(dataInizio, "Data inizio collegamento");
    }

    private List<ErrorRecordCause> validateDataFineCollegamento(String dataFine) {
//        return validateDateField(dataFine, "Data fine collegamento");
        return Collections.emptyList();
    }

    private List<ErrorRecordCause> validateRuoloInterno(String ruoloInterno) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Ruolo Interno";
        int expectedLength = 3;

        if (ruoloInterno == null || ruoloInterno.isEmpty()) {
            // Optional field
            return errors;
        }

        if (ruoloInterno.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format("%s exceeds maximum length %d (got %d)", fieldName, expectedLength, ruoloInterno.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateFlagStatoCollegamento(String flagStato) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Flag Stato Collegamento";
        Set<String> allowed = Set.of(" ", "C");

        if (flagStato == null || flagStato.isEmpty()) {
            // Optional field
            return errors;
        }

        if (!allowed.contains(flagStato)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, flagStato, "C (cancel) or space"),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }


    private List<ErrorRecordCause> validateCodiceFiscale(String value) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Codice fiscale";
        int expectedLength = 16;
        Pattern expectedPattern = Pattern.compile("^[a-zA-Z0-9]+$");
//        if (value == null || value.isEmpty()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
//                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
//            return errors;
//        }

        if (value.isEmpty()) return Collections.emptyList();

        if (value.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, value.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!expectedPattern.matcher(value).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, value, expectedPattern.pattern()),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }


    public List<ErrorRecordCause> validateCollegamenti(CollegamentiRecord record, Set<String> map) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateIntermediario(record.getIntermediario()));
        errors.addAll(validateChiaveRapporto(record.getChiaveRapporto()));
        errors.addAll(validateNdg(record.getNdg()));
        errors.addAll(validateRuolo(record.getRuolo()));
        errors.addAll(validateDataInizioCollegamento(record.getDataInizioCollegamento()));
        errors.addAll(validateDataFineCollegamento(record.getDataFineCollegamento()));
        errors.addAll(validateRuoloInterno(record.getRuoloInterno()));
        errors.addAll(validateFlagStatoCollegamento(record.getFlagStatoCollegamento()));
        errors.addAll(validateDataPredisposizioneFlusso(record.getDataPredisposizioneFlusso()));
        errors.addAll(validateControlloFinRiga(record.getControlloDiFineRiga()));

        return errors;
    }

    public List<ErrorRecordCause> validateCambioNdg(CambioNdgRecord record, Set<String> map) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateIntermediario(record.getIntermediario()));
        errors.addAll(validateNdgVecchio(record.getNdgVecchio()));
        errors.addAll(validateNdgNuovo(record.getNdgNuovo()));
        errors.addAll(validateControlloFinRiga(record.getControlloDiFineRiga()));

        return errors;
    }

    public List<ErrorRecordCause> validateDatiContabili(DatiContabiliRecord record, Set<String> map) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateIntermediario(record.getIntermediario()));
        errors.addAll(validateChiaveRapporto(record.getChiaveRapporto()));
        errors.addAll(validatePeriodicita(record.getPeriodicita()));
        errors.addAll(validateProgressivoPeriodicita(record.getProgressivoPeriodicita()));
        errors.addAll(validateDivisa(record.getDivisa()));
        errors.addAll(validateDataInizioRiferimento(record.getDataInizioRiferimento()));
        errors.addAll(validateDataFineRiferimento(record.getDataFineRiferimento()));
        errors.addAll(validateImportoSaldoIniziale(record.getImportoSaldoIniziale()));
        errors.addAll(validateImportoSaldoFinale(record.getImportoSaldoFinale()));
        errors.addAll(validateTotaleOperazioniAttive(record.getTotaleOperazioniAttive()));
        errors.addAll(validateTotaleOperazioniPassive(record.getTotaleOperazioniPassive()));
        errors.addAll(validateGiacenzaMedia(record.getGiacenzaMedia()));
        errors.addAll(validateFlagSogliaPerField(record.getFlagSogliaSaldoIniziale(), "Flag Soglia Saldo Iniziale"));
        errors.addAll(validateFlagSogliaPerField(record.getFlagSogliaSaldoFinale(), "Flag Soglia Saldo Finale"));
        errors.addAll(validateFlagSogliaPerField(record.getFlagSogliaOperazioniAttive(), "Flag Soglia Operazioni Attive"));
        errors.addAll(validateFlagSogliaPerField(record.getFlagSogliaOperazioniPassive(), "Flag Soglia Operazioni Passive"));
        errors.addAll(validateFlagSogliaPerField(record.getFlagSogliaGiacenzaMedia(), "Flag Soglia Giacenza Media"));
        errors.addAll(validateAltreInformazioni(record.getAltreInformazioni()));
        errors.addAll(validateFlagStatoImporto(record.getFlagStatoImporto()));
        errors.addAll(validateDataPredisposizione(record.getDataPredisposizione()));
        errors.addAll(validateTipoRapportoInterno(record.getTipoRapportoInterno()));
        errors.addAll(validateFormaTecnica(record.getFormaTecnica()));
        errors.addAll(validateFlagSogliaPerField(record.getFlagSogliaAltreInformazioni(), "Flag Soglia Altre Informazioni"));
        errors.addAll(validateControlloFinRiga(record.getControlloDiFineRiga()));

        return errors;
    }


    private List<ErrorRecordCause> validatePeriodicita(String periodicita) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Periodicità";
        String expectedValue = "000";

        if (periodicita == null || periodicita.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (!expectedValue.equals(periodicita)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, periodicita, expectedValue),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateProgressivoPeriodicita(String progressivo) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Progressivo Periodicità";
        String expectedValue = "001";

        if (progressivo == null || progressivo.isEmpty()) {
            errors.add(new ErrorRecordCause(
                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
            return errors;
        }

        if (!expectedValue.equals(progressivo)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, progressivo, expectedValue),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataInizioRiferimento(String dataInizio) {
        return validateDateField(dataInizio, "Data inizio riferimento");
    }

    private List<ErrorRecordCause> validateDataFineRiferimento(String dataFine) {
        return validateDateField(dataFine, "Data fine riferimento");
    }

    private List<ErrorRecordCause> validateImportoSaldoIniziale(String importo) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Importo Saldo Iniziale";
        int expectedLength = 18;

        if (importo == null || importo.isEmpty() || importo.trim().isEmpty()) {
            // Optional field
            return errors;
        }

        if (importo.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, importo.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateImportoSaldoFinale(String importo) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Importo Saldo Finale";
        int expectedLength = 18;

        if (importo == null || importo.isEmpty() || importo.trim().isEmpty()) {
            // Optional field
            return errors;
        }

        if (importo.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, importo.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTotaleOperazioniAttive(String totale) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Totale Operazioni Attive";
        int expectedLength = 18;

        if (totale == null || totale.isEmpty() || totale.trim().isEmpty()) {
            // Optional field
            return errors;
        }

        if (totale.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, totale.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTotaleOperazioniPassive(String totale) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Totale Operazioni Passive";
        int expectedLength = 18;

        if (totale == null || totale.isEmpty() || totale.trim().isEmpty()) {
            // Optional field
            return errors;
        }

        if (totale.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, totale.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateGiacenzaMedia(String giacenza) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Giacenza Media";
        int expectedLength = 18;

        if (giacenza == null || giacenza.isEmpty() || giacenza.trim().isEmpty()) {
            // Optional field
            return errors;
        }

        if (giacenza.length() != expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, giacenza.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateFlagSogliaPerField(String flag, String fieldName) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        Set<String> allowed = Set.of("0", "1");

        if (flag == null || flag.isEmpty()) {
            // Optional field
            return errors;
        }

        if (!allowed.contains(flag)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, flag, "0 or 1"),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateAltreInformazioni(String altreInfo) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Altre Informazioni";
        int expectedLength = 18;

        if (altreInfo == null || altreInfo.isEmpty() || altreInfo.trim().isEmpty()) {
            // Optional field
            return errors;
        }

        if (altreInfo.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, altreInfo.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateFlagStatoImporto(String flagStato) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Flag Stato Importo";
        Set<String> allowed = Set.of(" ", "C");

        if (flagStato == null || flagStato.isEmpty()) {
            // Optional field
            return errors;
        }

        if (!allowed.contains(flagStato)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, flagStato, "C (cancel) or space"),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }


    private List<ErrorRecordCause> validateNaturaValuta(String value) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Natura Valuta";
        int expectedLength = 2;


//        if (value == null || value.isEmpty()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
//                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
//            return errors;
//        }

        if (value.length() > expectedLength) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, expectedLength, value.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!NUMERIC_REGEX.matcher(value).matches()) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_FORMAT_ERROR_MSG, fieldName, value, "numeric"),
                    INVALID_FORMAT_ERROR_CODE));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateControlloFinRiga(String controlloDiFineRiga) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Controllo di Fine Riga";
        String expectedValue = "A";

//        if (controlloDiFineRiga == null || controlloDiFineRiga.isEmpty()) {
//            errors.add(new ErrorRecordCause(
//                    String.format(NULL_VALUE_ERROR_MSG, fieldName),
//                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
//            return errors;
//        }

        if (controlloDiFineRiga.length() != 1) {
            errors.add(new ErrorRecordCause(
                    String.format(LENGTH_MISMATCH_ERROR_MSG, fieldName, 1, controlloDiFineRiga.length()),
                    MANDATORY_DATA_IS_MISSING_ERROR_CODE));
        }

        if (!expectedValue.equals(controlloDiFineRiga)) {
            errors.add(new ErrorRecordCause(
                    String.format(INVALID_VALUE_ERROR_MSG, fieldName, controlloDiFineRiga, expectedValue),
                    INVALID_VALUE_ERROR_CODE));
        }
        return errors;
    }


    public void validateHeader(String first, String ingestionType) throws ValidationException {
        String ingestionTypeMsg = getHeaderOrFooterMsgInit(ingestionType);
        List<String> errors = new ArrayList<>();
        DateTimeFormatter inFmt = DateTimeFormatter.ofPattern("yyyyMMdd");

        if (first.length() < 250) {
            errors.add(String.format("%s Header line is too short: expected 250 characters but got %d",
                    ingestionTypeMsg, first.length()));
        } else {
            String tipoRecord = first.substring(0, 1);
            if (!"0".equals(tipoRecord)) {
                errors.add(String.format("%s Header tipo record: expected '0' but found '%s'",
                        ingestionTypeMsg, tipoRecord));
            }

            String startDateStr = first.substring(1, 9).trim();
            if (!startDateStr.isEmpty()) {
                try {
                    LocalDate.parse(startDateStr, inFmt);
                } catch (DateTimeParseException e) {
                    errors.add(String.format("%s Header start date: '%s' is not a valid yyyyMMdd date",
                            ingestionTypeMsg, startDateStr));
                }
            }

            String endDateStr = first.substring(9, 17).trim();
            if (!endDateStr.isEmpty()) {
                try {
                    LocalDate.parse(endDateStr, inFmt);
                } catch (DateTimeParseException e) {
                    errors.add(String.format("%s Header end date: '%s' is not a valid yyyyMMdd date",
                            ingestionTypeMsg, endDateStr));
                }
            }
            String endOfLine = first.substring(249, 250);
            if (!"A".equals(endOfLine)) {
                errors.add(String.format("%s Header end of line: expected 'A' but found '%s'",
                        ingestionTypeMsg, endOfLine));
            }
        }

        if (!errors.isEmpty()) {
            throw new ValidationException(String.join(" | ", errors));
        }
    }

    public void validateFooter(String last, int totalRecordsCount, String ingestionType) throws ValidationException {
        String ingestionTypeMsg = getHeaderOrFooterMsgInit(ingestionType);
        List<String> errors = new ArrayList<>();

        if (last.length() < 250) {
            errors.add(String.format("%s Footer line is too short: expected 250 characters but got %d",
                    ingestionTypeMsg, last.length()));
        } else {
            String tipoRecord = last.substring(0, 1);
            if (!"9".equals(tipoRecord)) {
                errors.add(String.format("%s Footer tipo record: expected '9' but found '%s'",
                        ingestionTypeMsg, tipoRecord));
            }

            String noOfRecordsStr = last.substring(1, 9).trim();
            try {
                int noOfRecords = Integer.parseInt(noOfRecordsStr);
                if (totalRecordsCount != noOfRecords) {
                    errors.add(String.format("%s Footer Record count mismatch: File says %d but found %d",
                            ingestionTypeMsg, noOfRecords, totalRecordsCount));
                }
            } catch (NumberFormatException e) {
                errors.add(String.format("%s Footer record count is not a valid number: '%s'",
                        ingestionTypeMsg, noOfRecordsStr));
            }


            String endOfLine = last.substring(249, 250);
            if (!"A".equals(endOfLine)) {
                errors.add(String.format("%s Footer end of line: expected 'A' but found '%s'",
                        ingestionTypeMsg, endOfLine));
            }
        }
        if (!errors.isEmpty()) {
            throw new ValidationException(String.join(" | ", errors));
        }
    }

    private String getHeaderOrFooterMsgInit(String ingestionType) {
        if (ingestionType.equals(IngestionTypeEnum.SOGGETTI.getLabel())) {
            return "Soggeti File:";
        } else if (ingestionType.equals(IngestionTypeEnum.RAPPORTI.getLabel())) {
            return "Soggeti File:";
        } else if (ingestionType.equals(IngestionTypeEnum.DATI_CONTABILI.getLabel())) {
            return "Soggeti File:";
        } else if (ingestionType.equals(IngestionTypeEnum.COLLEGAMENTI.getLabel())) {
            return "Soggeti File:";
        } else {
            return "Soggeti File:";
        }
    }
}