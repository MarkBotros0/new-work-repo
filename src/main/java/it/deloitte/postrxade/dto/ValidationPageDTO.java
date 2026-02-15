package it.deloitte.postrxade.dto;

/**
 * DTO representing the complete validation summary.
 * (Corresponds to the 'Validation' interface)
 *
 * @param totalTransactionCount The total number of transactions processed.
 * @param errorValidationGroup The group of validation errors.
 * @param warningValidationGroup The group of validation warnings.
 */
public record ValidationPageDTO(
        long totalTransactionCount,
        long totalTransactionsWithErrorCount,
        ValidationGroup errorValidationGroup,
        ValidationGroup warningValidationGroup
) {}

