package it.deloitte.postrxade.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * DTO representing a Transaction in the system.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TransactionDTO {

    private Long id;
    private IngestionDTO ingestion;
    private String tpRec;
    private String idIntermediario;
    private String idEsercente;
    private String chiaveBanca;
    private String idPos;
    private String tipoOpe;
    private Integer dtOpe;
    private String divisaOpe;
    private String tipoPag;
    private BigDecimal impOpe;
    private Integer totOpe;
    private MerchantDTO merchant;
//    private MerchantAccountDTO merchantAccount;

    @Override
    public String toString() {
        return "TransactionDTO{" +
                "id=" + id +
                ", ingestion=" + (ingestion != null ? ingestion.getId() : null) +
                ", tpRec='" + tpRec + '\'' +
                ", idIntermediario='" + idIntermediario + '\'' +
                ", idEsercente='" + idEsercente + '\'' +
                ", chiaveBanca='" + chiaveBanca + '\'' +
                ", idPos='" + idPos + '\'' +
                ", tipoOpe='" + tipoOpe + '\'' +
                ", dtOpe=" + dtOpe +
                ", divisaOpe='" + divisaOpe + '\'' +
                ", tipoPag='" + tipoPag + '\'' +
                ", impOpe=" + impOpe +
                '}';
    }
}
