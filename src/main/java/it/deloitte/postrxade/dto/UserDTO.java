package it.deloitte.postrxade.dto;

import it.deloitte.postrxade.entity.Authority;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Set;

/**
 * DTO representing a User in the system.
 */
@Data
public class UserDTO {

    private String id;
    private String firstName;
    private String lastName;
    private String email;
    private String company;
    private String office;
    private LocalDateTime lastLoggedIn;
    private Set<AuthorityDTO> authorities;

    // Constructors
    public UserDTO() {}

    public UserDTO(String id, String firstName, String lastName, String email, String company, String office) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.company = company;
        this.office = office;
    }


}
