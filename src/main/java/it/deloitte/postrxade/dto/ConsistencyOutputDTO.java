package it.deloitte.postrxade.dto;

import lombok.*;

import java.math.BigDecimal;

/**
 * DTO representing a Consistency Output in the system.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class ConsistencyOutputDTO {

    // Getters and Setters
    private Long id;
    private String tpRec;
    private String cfSoggObbligato;
    private String tipoOpe;
    private Integer meseOpe;
    private Integer annoOpe;
    private BigDecimal impOpe;
    private Integer totOpe;
    private String idEsercente;
    private String idPos;
    private String tipoPag;
    private String codFiscale;
    private String partitaIva;
    private String chiaveBanca;


}





