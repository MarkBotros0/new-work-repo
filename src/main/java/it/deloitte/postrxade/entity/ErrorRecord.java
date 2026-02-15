package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Entity representing an Error Record in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "ERROR_RECORD")
public class ErrorRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_error_record")
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_ingestion")
    private Ingestion ingestion;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @OneToMany(mappedBy = "errorRecord", cascade = CascadeType.ALL)
    private List<ErrorCause> errorCauses;

    @Column(name = "raw_row", length = 250)
    private String rawRow;

    @CreationTimestamp
    @Column(name = "created_at")
    private LocalDateTime createdAt;

}
