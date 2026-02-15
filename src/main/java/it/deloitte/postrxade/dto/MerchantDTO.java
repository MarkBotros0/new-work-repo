package it.deloitte.postrxade.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DTO representing a Merchant in the system.
 */
@Setter
@Getter
public class MerchantDTO {

    // Getters and Setters
    private Long id;
    private IngestionDTO ingestion;
    private String tpRec;
    private String idIntermediario;
    private String idEsercente;
    private String codFiscale;
    private String partitaIva;
    private List<TransactionDTO> transactions;

    public MerchantDTO(Long id, IngestionDTO ingestion, String tpRec, String idIntermediario, String idEsercente, String codFiscale, String partitaIva) {
        this.id = id;
        this.ingestion = ingestion;
        this.tpRec = tpRec;
        this.idIntermediario = idIntermediario;
        this.idEsercente = idEsercente;
        this.codFiscale = codFiscale;
        this.partitaIva = partitaIva;
    }

    @Override
    public String toString() {
        return "MerchantDTO{" +
                "id=" + id +
                ", ingestion=" + (ingestion != null ? ingestion.getId() : null) +
                ", tpRec='" + tpRec + '\'' +
                ", idIntermediario='" + idIntermediario + '\'' +
                ", idEsercente='" + idEsercente + '\'' +
                ", codFiscale='" + codFiscale + '\'' +
                ", partitaIva='" + partitaIva + '\'' +
                '}';
    }
}

