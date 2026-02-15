package it.deloitte.postrxade.dto;

import lombok.*;

import java.math.BigDecimal;

/**
 * DTO representing a Monthly Output in the system.
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MonthlyOutputDTO {

    private Long id;
    private String tpRec;
    private String cfSoggObbligato;
    private String tipoOpe;
    private Integer dtOpe;
    private Integer totOpe;
    private BigDecimal impOpe;
    private String divisaOpe;
    private String idEsercente;
    private String idPos;
    private String tipoPag;
    private String codFiscale;
    private String partitaIva;
    private String chiaveBanca;

}





