package it.deloitte.postrxade.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ErrorTypeCode {
    INVALID_FORMAT("WRN1"),
    INVALID_DATE_FORMAT("WRN2"),
    MANDATORY_DATA_IS_MISSING("WRN3"),
    INVALID_VALUE("WRN4"),
    FOREIGN_KEY_ERROR("ERR1"),
    TRANSACTION_ALREADY_EXISTS("ERR2"),
    MERCHANT_ALREADY_EXISTS("ERR3");

    private final String errorCode;
}
