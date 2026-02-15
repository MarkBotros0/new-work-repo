package it.deloitte.postrxade.repository;

import it.deloitte.postrxade.entity.ErrorRecord;

import java.util.List;

public interface ErrorRecordRepositoryCustom {
    void bulkInsert(List<ErrorRecord> errorRecords);
    void bulkInsertRecordsWithCauses(List<ErrorRecord> records, Long ingestionId);
}

