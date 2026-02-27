package it.deloitte.postrxade.service;

/**
 * Service for launching ECS tasks.
 */
public interface EcsTaskService {
    
    /**
     * Launches an ECS task for output generation.
     * 
     * @param submissionId the submission ID to generate output for
     * @return the task ARN if successful, null otherwise
     * @throws RuntimeException if task launch fails
     */
    String launchOutputGenerationTask(Long submissionId);
}
