package it.deloitte.postrxade.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExcelRowDTO {

    @JsonProperty("raw_record")
    private String rawRecord;

    @JsonProperty("error_level")
    private String type;

    @JsonProperty("error_name")
    private String name;

    @JsonProperty("error_description")
    private String description;

    @JsonProperty("batch_id")
    private String batchId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("file_type")
    private String fileType;
}