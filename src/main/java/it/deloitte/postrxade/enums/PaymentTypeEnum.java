package it.deloitte.postrxade.enums;

import lombok.Getter;

/**
 * Enumeration for Transaction Payment Types.
 * <p>
 * Handles the mapping between the raw database code (e.g., "00")
 * and the user-friendly label (e.g., "E-commerce").
 */
@Getter
public enum PaymentTypeEnum {

    E_COMMERCE("00", "E-commerce"),
    POS("01", "POS"), // Assuming '01' or other codes map to POS
    UNKNOWN("UNKNOWN", "Unknown");

    private final String code;
    private final String label;

    PaymentTypeEnum(String code, String label) {
        this.code = code;
        this.label = label;
    }

    /**
     * Resolves the label based on the raw code.
     * Contains the specific business logic that was previously hardcoded.
     */
    public static PaymentTypeEnum fromCode(String code) {
        if (code == null) return UNKNOWN;

        // Exact match for E-Commerce
        if (E_COMMERCE.code.equalsIgnoreCase(code)) {
            return E_COMMERCE;
        }

        // Logic from original code: Anything not "00" was treated as POS.
        // You might want to be stricter here in the future.
        return POS;
    }
}
