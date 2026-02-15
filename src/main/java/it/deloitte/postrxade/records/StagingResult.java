package it.deloitte.postrxade.records;

/**
 * Result of staging table processing operations.
 */
public record StagingResult(
        int insertedCount,
        int duplicateCount,
        int missingMerchantCount,
        int errorCount
) {
    /**
     * Constructor for merchant results (no missing merchant count).
     */
    public StagingResult(int insertedCount, int duplicateCount) {
        this(insertedCount, duplicateCount, 0, 0);
    }

    /**
     * Total records processed.
     */
    public int totalProcessed() {
        return insertedCount + duplicateCount + missingMerchantCount + errorCount;
    }
}
