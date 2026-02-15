package it.deloitte.postrxade.entity;

import jakarta.persistence.*;
import lombok.*;

import java.util.List;

/**
 * Entity representing an Output in the system.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "OUTPUT_FILE")
public class Output {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pk_output")
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "fk_submission")
    private Submission submission;

    @Column(name = "full_path")
    private String fullPath;

    @Column(name = "extention_type")
    private String extensionType;

    @Column(name = "generated_at")
    private String generatedAt;

    @OneToMany(mappedBy = "output", fetch = FetchType.LAZY)
    private List<Transaction> transactions;

}
