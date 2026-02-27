package it.deloitte.postrxade.tenant;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-tenant aware DataSource that routes database connections based on the current tenant.
 * 
 * This implementation uses Spring's AbstractRoutingDataSource to dynamically select
 * the appropriate DataSource for each tenant at runtime.
 * Non attivo con profilo "output": il task ECS output usa un unico DB da variabili d'ambiente (spring.datasource).
 */
@Component
@Profile("!output")
public class TenantAwareDataSource extends AbstractRoutingDataSource {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TenantAwareDataSource.class);
    
    private final TenantConfiguration tenantConfiguration;
    private final Map<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();
    
    public TenantAwareDataSource(TenantConfiguration tenantConfiguration) {
        this.tenantConfiguration = tenantConfiguration;
        setTargetDataSources(new HashMap<>());
        setDefaultTargetDataSource(null);
    }
    
    @Override
    protected Object determineCurrentLookupKey() {
        String tenantId = TenantContext.getTenantId();
        if (tenantId == null) {
            tenantId = tenantConfiguration.getBootstrapTenantResolved();
        }
        if (tenantId == null) {
            tenantId = tenantConfiguration.getBootstrapTenant();
        }
        // Chiave normalizzata (aziendaa→nexi) per cache coerente
        tenantId = TenantConfiguration.resolveTenantAlias(tenantId);
        LOGGER.debug("Determining DataSource for tenant: {}", tenantId);
        return tenantId;
    }

    @Override
    protected DataSource determineTargetDataSource() {
        String tenantId = TenantContext.getTenantId();
        // At startup (e.g. session schema init) there is no request context: use bootstrap tenant
        if (tenantId == null) {
            tenantId = tenantConfiguration.getBootstrapTenantResolved();
            if (tenantId == null) {
                tenantId = tenantConfiguration.getBootstrapTenant();
            }
            LOGGER.debug("No tenant context, using bootstrap tenant: {}", tenantId);
        }
        // Normalizza aziendaa→nexi, aziendab→amex per cache e lookup (retrocompatibilità ECS)
        if (tenantId != null) {
            tenantId = TenantConfiguration.resolveTenantAlias(tenantId);
        }

        // Check cache first
        DataSource dataSource = dataSourceCache.get(tenantId);
        if (dataSource != null) {
            LOGGER.debug("Using cached DataSource for tenant: {}", tenantId);
            return dataSource;
        }
        
        // Get tenant configuration
        TenantConfiguration.TenantProperties tenantProps = tenantConfiguration.getTenantProperties(tenantId);
        if (tenantProps == null) {
            LOGGER.error("No configuration found for tenant: {}", tenantId);
            throw new IllegalStateException("Tenant not configured: " + tenantId);
        }
        
        // Create new DataSource for this tenant
        LOGGER.info("Creating new DataSource for tenant: {} with database: {}", tenantId, tenantProps.getDatabaseName());
        dataSource = createDataSourceForTenant(tenantId, tenantProps);
        
        // Cache it
        dataSourceCache.put(tenantId, dataSource);
        
        return dataSource;
    }
    
    /**
     * Creates a HikariDataSource for a specific tenant.
     */
    private DataSource createDataSourceForTenant(String tenantId, TenantConfiguration.TenantProperties tenantProps) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(tenantProps.getDatabaseUrl());
        config.setUsername(tenantProps.getDatabaseUsername());
        config.setPassword(tenantProps.getDatabasePassword());
        config.setDriverClassName("org.mariadb.jdbc.Driver");
        config.setPoolName("Hikari-" + tenantId);
        config.setAutoCommit(false);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("useSSL", "true");
        config.addDataSourceProperty("serverTimezone", "UTC");
        config.addDataSourceProperty("allowPublicKeyRetrieval", "true");
        
        return new HikariDataSource(config);
    }
    
    /**
     * Clears the DataSource cache. Useful for testing or when tenant configuration changes.
     */
    public void clearCache() {
        LOGGER.info("Clearing DataSource cache");
        dataSourceCache.clear();
    }
}
