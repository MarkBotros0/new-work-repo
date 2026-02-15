package it.deloitte.postrxade.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * DTO representing a Merchant Account in the system.
 */
@Setter
@Getter
public class MerchantAccountDTO {

    // Getters and Setters
    private Long id;
    private IngestionDTO ingestion;
    private String tpRec;
    private String idIntermediario;
    private String chiaveBanca;
    private List<TransactionDTO> transactions;

    public MerchantAccountDTO(Long id, IngestionDTO ingestion, String tpRec, String idIntermediario, String chiaveBanca) {
        this.id = id;
        this.ingestion = ingestion;
        this.tpRec = tpRec;
        this.idIntermediario = idIntermediario;
        this.chiaveBanca = chiaveBanca;
    }

    @Override
    public String toString() {
        return "MerchantAccountDTO{" +
                "id=" + id +
                ", ingestion=" + (ingestion != null ? ingestion.getId() : null) +
                ", tpRec='" + tpRec + '\'' +
                ", idIntermediario='" + idIntermediario + '\'' +
                ", chiaveBanca='" + chiaveBanca + '\'' +
                '}';
    }
}

