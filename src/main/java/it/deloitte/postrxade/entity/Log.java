package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.Date;

/**
 * Entity representing a Log in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "LOG")
@Builder
public class Log {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_log")
    private Long id;

    @Column
    //@Temporal(TemporalType.TIMESTAMP)
    private Instant timestamp;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_sbmission")
    private Submission submission;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_updater")
    private User updater;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_before_sumbission_status")
    private SubmissionStatus beforeSubmissionStatus;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_after_sumbission_status")
    private SubmissionStatus afterSubmissionStatus;

    @Column(name = "message")
    private String message;
}
