package it.deloitte.postrxade.enums;

import lombok.Getter;

@Getter
public enum MerchantTypeEnum {
    RAPPORTO("1"),
    ANAGRAFICA("2"),
    SALDI_MOVIMENTI_RAPPORTO("3"),
    CAMBIO_IDENTIFICATIVO("4");

    private final String tipoRecord;

    MerchantTypeEnum(String level) {
        this.tipoRecord = level;
    }
}
