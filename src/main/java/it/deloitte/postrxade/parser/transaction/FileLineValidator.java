package it.deloitte.postrxade.parser.transaction;

import it.deloitte.postrxade.entity.Obligation;
import it.deloitte.postrxade.enums.ErrorTypeCode;
import it.deloitte.postrxade.enums.IngestionTypeEnum;
import it.deloitte.postrxade.records.ErrorRecordCause;
import jakarta.validation.ValidationException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class FileLineValidator {

    private static final Pattern ALL_INT_REGEX = Pattern.compile("^[0-9]+$");
    private static final Pattern THREE_LETTER_REGEX = Pattern.compile("^[A-Za-z]{3}$");
    private static final Pattern ALPHANUMERIC_REGEX = Pattern.compile("^[A-Za-z0-9]+$");

    private static final String INVALID_FORMAT_ERROR_MSG = "%s '%s' does not match expected format '%s')";
    private static final String MANDATORY_DATA_IS_MISSING_ERROR_MSG = "%s '%s' does not match expected length (%d)";
    private static final String INVALID_VALUE_ERROR_MSG = "%s '%s' does not match expected value [ %s ]";
    private static final String INVALID_DATE_ERROR_MSG = "%s '%s' does not match expected date format [ %s ]";
    private static final String DATE_MISMATCH_ERROR_MSG = "Date %s does not belong to the expected period (Year: %d, Month: %d)";

    private static final String invalidFormatErrorCode = ErrorTypeCode.INVALID_FORMAT.getErrorCode();
    private static final String mandatoryDataIsMissingErrorCode = ErrorTypeCode.MANDATORY_DATA_IS_MISSING.getErrorCode();
    private static final String invalidValueErrorCode = ErrorTypeCode.INVALID_VALUE.getErrorCode();
    private static final String invalidDateErrorCode = ErrorTypeCode.INVALID_DATE_FORMAT.getErrorCode();
    private static final String merchantAlreadyExistsErrorCode = ErrorTypeCode.MERCHANT_ALREADY_EXISTS.getErrorCode();

    public List<ErrorRecordCause> validateTransactionWithError(TransactionRecord record, Obligation obligation) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateTipoRecord(record.getTipoRecord()));
        errors.addAll(validateTipoOperazione(record.getTipoOperazione()));

        List<ErrorRecordCause> dateErrors = validateDataOperazione(record.getDataOperazione(), obligation);
        errors.addAll(dateErrors);

        if (dateErrors.isEmpty()) {
            changeDateFormat(record);
        }

        errors.addAll(validateTotalOperazione(record.getNumeroOperazioniGiorno()));
        errors.addAll(validateImpOpe(record.getImportoTotaleOperazioni()));
        errors.addAll(validateDivisaOpe(record.getDivisaOperazioni()));
        errors.addAll(validateIdEsercente(record.getIdEsercente()));
        errors.addAll(validateTerminalId(record.getTerminalId()));
        errors.addAll(validateTipoPag(record.getTipoPagamento()));
        errors.addAll(validateChiaveBanca(record.getChiaveRapportoBanca()));

        return errors;
    }

    private void changeDateFormat(TransactionRecord record) {
        DateTimeFormatter inFmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        DateTimeFormatter outFmt = DateTimeFormatter.ofPattern("ddMMyyyy");
        record.setDataOperazione(LocalDate.parse(record.getDataOperazione(), inFmt).format(outFmt));
    }

    public List<ErrorRecordCause> validateMerchantWithError(MerchantRecord record, Set<String> map) {
        List<ErrorRecordCause> errors = new ArrayList<>();

        errors.addAll(validateCodFiscale(record.getCodiceFiscaleEsercente()));
        errors.addAll(validatePartitaIva(record.getPartitaIva()));

        return errors;
    }

    private List<ErrorRecordCause> validateTipoRecord(String tipoRecord) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "Tipo Record";
        int expectedLength = 1;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(tipoRecord).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, tipoRecord, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (tipoRecord.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, tipoRecord, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        if (!"1".equals(tipoRecord)) {
            String errorDescription = String.format(INVALID_VALUE_ERROR_MSG, fieldName, tipoRecord, "1");
            errors.add(new ErrorRecordCause(errorDescription, invalidValueErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTipoOperazione(String tipoOperazione) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "TIPO-OPE";
        int expectedLength = 2;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(tipoOperazione).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, tipoOperazione, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (tipoOperazione.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, tipoOperazione, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        if (!"00".equals(tipoOperazione) && !"01".equals(tipoOperazione)) {
            String errorDescription = String.format(INVALID_VALUE_ERROR_MSG, fieldName, tipoOperazione, "00, 01");
            errors.add(new ErrorRecordCause(errorDescription, invalidValueErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateDataOperazione(String dataOperazione, Obligation obligation) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "DT-OPE";
        int expectedLength = 8;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(dataOperazione).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, dataOperazione, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (dataOperazione.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, dataOperazione, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        DateTimeFormatter inFmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        try {
            LocalDate date = LocalDate.parse(dataOperazione, inFmt);

            if (date.getYear() != obligation.getFiscalYear() || date.getMonthValue() != obligation.getPeriod().getOrder()) {
                String errorDescription = String.format(
                        DATE_MISMATCH_ERROR_MSG,
                        dataOperazione, obligation.getFiscalYear(), obligation.getPeriod().getOrder()
                );
                errors.add(new ErrorRecordCause(errorDescription, invalidDateErrorCode));
            }
        } catch (DateTimeParseException e) {
            String errorDescription = String.format(INVALID_DATE_ERROR_MSG, fieldName, dataOperazione, inFmt);
            errors.add(new ErrorRecordCause(errorDescription, invalidDateErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTotalOperazione(String TotOperazione) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "TOT-OPE";
        int expectedLength = 10;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(TotOperazione).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, TotOperazione, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (TotOperazione.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, TotOperazione, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateImpOpe(String impOpe) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "IMP-OPE";
        int expectedLength = 14;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(impOpe).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, impOpe, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (impOpe.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, impOpe, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        return errors;
    }


    private List<ErrorRecordCause> validateDivisaOpe(String divisaOpe) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "DIVISA-OPE";
        int expectedLength = 3;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(divisaOpe).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, divisaOpe, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (divisaOpe.length() != expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, divisaOpe, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        if (!"978".equals(divisaOpe)) {
            String errorDescription = String.format(INVALID_VALUE_ERROR_MSG, fieldName, divisaOpe, "EUR");
            errors.add(new ErrorRecordCause(errorDescription, invalidValueErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateIdEsercente(String idEsercente) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "ID-ESERCENTE";
        int expectedLength = 30;
        Pattern exepectedPattern = ALPHANUMERIC_REGEX;

        if (!exepectedPattern.matcher(idEsercente).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, idEsercente, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (idEsercente.length() > expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, idEsercente, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTerminalId(String terminalId) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "ID-POS";
        int expectedLength = 30;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(terminalId).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, terminalId, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (terminalId.length() > expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, terminalId, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateTipoPag(String tipoPag) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "TIPO-PAG";
        int expectedLength = 2;
        Pattern exepectedPattern = ALL_INT_REGEX;

        if (!exepectedPattern.matcher(tipoPag).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, tipoPag, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (tipoPag.length() < expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, tipoPag, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        if (!"00".equals(tipoPag) && !"01".equals(tipoPag)) {
            String errorDescription = String.format(INVALID_VALUE_ERROR_MSG, fieldName, tipoPag, "00, 01");
            errors.add(new ErrorRecordCause(errorDescription, invalidValueErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateChiaveBanca(String chiaveBanca) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "CHIAVE-BANCA";
        int expectedLength = 50;
        Pattern exepectedPattern = ALPHANUMERIC_REGEX;

        if (!exepectedPattern.matcher(chiaveBanca).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, chiaveBanca, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (chiaveBanca.length() > expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, chiaveBanca, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validateCodFiscale(String chiaveBanca) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "COD-FISCALE";
        int expectedLength = 16;
        Pattern exepectedPattern = ALPHANUMERIC_REGEX;

        if (!exepectedPattern.matcher(chiaveBanca).matches()) {
            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, chiaveBanca, exepectedPattern.pattern());
            errors.add(new ErrorRecordCause(errorDescription, invalidFormatErrorCode));
        }
        if (chiaveBanca.length() > expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, chiaveBanca, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
        }
        return errors;
    }

    private List<ErrorRecordCause> validatePartitaIva(String chiaveBanca) {
        List<ErrorRecordCause> errors = new ArrayList<>();
        String fieldName = "PARTITA-IVA";
        int expectedLength = 11;
//        Pattern exepectedPattern = ALL_INT_REGEX;
//
//        if (!exepectedPattern.matcher(chiaveBanca).matches()) {
//            String errorDescription = String.format(INVALID_FORMAT_ERROR_MSG, fieldName, chiaveBanca, exepectedPattern.pattern());
//            errors.add(new IngestionValidationError(errorDescription, invalidFormatErrorCode));
//        }
        if (chiaveBanca.length() > expectedLength) {
            String errorDescription = String.format(MANDATORY_DATA_IS_MISSING_ERROR_MSG, fieldName, chiaveBanca, expectedLength);
            errors.add(new ErrorRecordCause(errorDescription, mandatoryDataIsMissingErrorCode));
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
            return "Transaction File:";
        } else {
            return "Merchant File:";
        }
    }
}
