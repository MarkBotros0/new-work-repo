package it.deloitte.postrxade.config;

import it.deloitte.postrxade.tenant.TenantAwareDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

@Configuration
@EnableJpaRepositories("it.deloitte.postrxade.repository")
@EnableJpaAuditing
@EnableTransactionManagement
public class DatabaseConfiguration {

    /**
     * Primary DataSource bean that uses TenantAwareDataSource for multi-tenant support.
     * This ensures that all JPA repositories use the tenant-aware routing DataSource.
     * Non attivo con profilo "output": in quel caso si usa il DataSource auto-configurato da spring.datasource (DB da env).
     */
    @Bean
    @Primary
    @Profile("!output")
    public DataSource dataSource(TenantAwareDataSource tenantAwareDataSource) {
        return tenantAwareDataSource;
    }
}
