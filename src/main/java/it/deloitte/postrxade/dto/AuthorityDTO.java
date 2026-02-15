package it.deloitte.postrxade.dto;

import lombok.Data;

import java.util.Set;

/**
 * DTO representing an Authority in the system.
 */
@Data
public class AuthorityDTO {

    private String id;
    private String description;
//    private Set<UserDTO> users;

    // Constructors
    public AuthorityDTO() {}

    public AuthorityDTO(String id, String description) {
        this.id = id;
        this.description = description;
    }


}
