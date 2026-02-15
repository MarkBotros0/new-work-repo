package it.deloitte.postrxade.records;

import it.deloitte.postrxade.entity.*;

import java.util.ArrayList;
import java.util.List;

public record ProcessedRecordBatch(List<Soggetti> soggettiList,
                                   List<Rapporti> rapportiList,
                                   List<Collegamenti> collegamentiList,
                                   List<CambioNdg> cambioNdgList,
                                   List<DatiContabili> datiContabiliList,
                                   List<ErrorRecord> errorRecords) {
}