package it.deloitte.postrxade.records;

import it.deloitte.postrxade.entity.ErrorRecord;
import it.deloitte.postrxade.entity.ResolvedTransaction;

import java.util.List;

public record ProcessedFailedRecordBatch(List<ResolvedTransaction> transactions, List<ErrorRecord> errorRecords) {
}