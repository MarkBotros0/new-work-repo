package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


/**
 * Entity representing an Error Record in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "ERROR_CAUSE")
public class ErrorCause {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_error_cause")
    private Long id;

    @ManyToOne()
    @JoinColumn(name = "fk_error_record")
    private ErrorRecord errorRecord;

    @ManyToOne()
    @JoinColumn(name = "fk_error_type")
    private ErrorType errorType;

    @Column(name = "error_message", length = 2500)
    private String errorMessage;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_submission")
    private Submission submission;
}
