package it.deloitte.postrxade.enums;

import lombok.Getter;

/**
 * Enumeration for Ingestion Types.
 * Replaces magic strings "transato" and "anagrafe".
 */
@Getter
public enum IngestionTypeEnum {

    SOGGETTI("soggetti"),
    RAPPORTI("rapporti"),
    DATI_CONTABILI("datiContabili"),
    COLLEGAMENTI("collegamenti"),
    CAMBIO_NDG("cambioNdg");

    private final String label;

    IngestionTypeEnum(String label) {
        this.label = label;
    }
}