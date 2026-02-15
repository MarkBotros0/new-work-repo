package it.deloitte.postrxade.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * Entity representing a Submission Status Group in the system.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name ="SUBMISSION_STATUS_GROUP")
public class SubmissionStatusGroup {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_submission_status_group")
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
    @OneToMany(mappedBy = "submissionStatusGroup")
    private List<SubmissionStatus> submissionStatuses;


}
