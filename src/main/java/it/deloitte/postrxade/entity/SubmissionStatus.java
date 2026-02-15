package it.deloitte.postrxade.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * Entity representing a Submission Status in the system.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "SUBMISSION_STATUS")
public class SubmissionStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_submission_status")
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "description")
    private String description;

    @Column(name = "code")
    private String code;

    @Column(name = "order")
    private Integer order;


    @Column(name = "is_active")
    private Boolean isActive;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @JsonIgnore
    @OneToMany(mappedBy = "currentSubmissionStatus", fetch = FetchType.LAZY)
    private List<Submission> submissions;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @JsonIgnore
    @OneToMany(mappedBy = "beforeSubmissionStatus", fetch = FetchType.LAZY)
    private List<Log> beforeLogs;

    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    @JsonIgnore
    @OneToMany(mappedBy = "afterSubmissionStatus", fetch = FetchType.LAZY)
    private List<Log> afterLogs;

    @JsonIgnore
    @ManyToOne()
    @JoinColumn(name = "fk_submission_status_group")
    private SubmissionStatusGroup submissionStatusGroup;

}
