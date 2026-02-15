package it.deloitte.postrxade.dto;


import lombok.*;

/**
 * DTO representing an Ingestion Status in the system.
 */
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class IngestionStatusDTO {

    private Long id;
    private String name;
    private String description;
    private Integer order;

}
