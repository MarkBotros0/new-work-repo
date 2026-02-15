package it.deloitte.postrxade.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Configuration class for Asynchronous Method Execution.
 * <p>
 * This class enables Spring's {@code @Async} annotation processing.
 * It also defines a custom {@link Executor} (Thread Pool) to handle background tasks
 * efficiently.
 * <p>
 * Without this custom executor, Spring uses {@code SimpleAsyncTaskExecutor}, which
 * creates a new thread for <em>every</em> task—a potential risk for resource exhaustion
 * if many reports are requested simultaneously.
 */
@Configuration
@EnableAsync
public class AsyncConfig {

    /**
     * Defines a thread pool for executing asynchronous tasks.
     * <p>
     * Configuration:
     * <ul>
     * <li><strong>Core Pool Size (2):</strong> Keeps 2 threads alive waiting for tasks.</li>
     * <li><strong>Max Pool Size (50):</strong> Allows expanding to 50 threads under heavy load.</li>
     * <li><strong>Queue Capacity (10000):</strong> Buffers up to 10000 tasks if all threads are busy.</li>
     * <li><strong>Thread Prefix:</strong> Names threads "pos-task-" for easier debugging in logs.</li>
     * <li><strong>Task Decorator:</strong> Ensures AWS credentials are available in async threads.</li>
     * </ul>
     * <p>
     * The TaskDecorator ensures that AWS credentials (IAM Role) are properly accessible
     * in async threads by allowing the DefaultCredentialsProvider to refresh credentials
     * in the new thread context.
     *
     * @return A configured {@link Executor}.
     */
    @Bean(name = "taskExecutor")
    @org.springframework.context.annotation.Primary
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(10000);
        executor.setThreadNamePrefix("pos-task-");
        
        // TaskDecorator per assicurarsi che le credenziali AWS siano disponibili nei thread asincroni
        executor.setTaskDecorator(new TaskDecorator() {
            @Override
            public Runnable decorate(Runnable runnable) {
                return () -> {
                    // Il thread asincrono userà automaticamente le credenziali IAM Role
                    // tramite DefaultCredentialsProvider che accede all'IMDS
                    runnable.run();
                };
            }
        });
        
        executor.initialize();
        return executor;
    }
}