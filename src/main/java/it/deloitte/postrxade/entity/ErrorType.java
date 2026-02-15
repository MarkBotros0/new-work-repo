package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * Entity representing an Error Type in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "ERROR_TYPE")
public class ErrorType {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_error_type")
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "serverity_level")
    private Integer severityLevel;

    @Column(name = "is_active")
    private Boolean isActive;

    @Column(name = "error_code")
    private String errorCode;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "errorType", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<ErrorCause> errorCauses;

}

