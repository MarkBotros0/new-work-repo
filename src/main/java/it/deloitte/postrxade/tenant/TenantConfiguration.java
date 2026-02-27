package it.deloitte.postrxade.tenant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Configuration class that holds tenant-specific settings.
 * This is populated from application-dev.yml.
 */
@Configuration
@ConfigurationProperties(prefix = "multi-tenant")
public class TenantConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TenantConfiguration.class);

    /** Dominio base per host tenant (es. testpos-noprod.com o dominio prod). Da multi-tenant.tenant-host-base-domain / CORS_TENANT_BASE_DOMAIN. */
    private String tenantHostBaseDomain = "testpos-noprod.com";

    /** Tenant used when no request context is available (e.g. startup, session schema init). */
    private String bootstrapTenant = "nexi";

    public String getTenantHostBaseDomain() {
        return tenantHostBaseDomain;
    }

    public void setTenantHostBaseDomain(String tenantHostBaseDomain) {
        this.tenantHostBaseDomain = tenantHostBaseDomain != null && !tenantHostBaseDomain.isBlank() ? tenantHostBaseDomain : "testpos-noprod.com";
    }

    /** Display name for each SSO provider id: oidc=Nexi, oidc-amex=Amex, oidc-deloitte=Deloitte. */
    private Map<String, String> providerDisplayNames = new HashMap<>();
    
    private Map<String, TenantProperties> tenants = new HashMap<>();
    
    public Map<String, String> getProviderDisplayNames() {
        return providerDisplayNames;
    }
    
    public void setProviderDisplayNames(Map<String, String> providerDisplayNames) {
        this.providerDisplayNames = providerDisplayNames;
    }
    
    /** Returns the display name for a provider id, or the id itself if not configured. */
    public String getProviderDisplayName(String providerId) {
        return providerDisplayNames != null && providerDisplayNames.containsKey(providerId)
            ? providerDisplayNames.get(providerId)
            : providerId;
    }
    
    public String getBootstrapTenant() {
        return bootstrapTenant;
    }
    
    public void setBootstrapTenant(String bootstrapTenant) {
        this.bootstrapTenant = bootstrapTenant;
    }
    
    public Map<String, TenantProperties> getTenants() {
        return tenants;
    }
    
    public void setTenants(Map<String, TenantProperties> tenants) {
        this.tenants = tenants;
        LOGGER.info("Loaded configuration for {} tenants", tenants.size());
    }
    
    /**
     * Mappa alias tenant → id target (aziendaa→nexi, aziendab→amex).
     * <p>
     * <b>WORKAROUND TEMPORANEO</b>: in test gli URL sono ancora aziendab/aziendaa mentre JWT e config
     * usano amex/nexi. Questa mappa permette di far coincidere host (aziendab-be) con tenant_id (amex).
     * <b>Rimuovere</b> quando gli URL di test saranno sostituiti da quelli target (amex-be, nexi-be).
     */
    public static String resolveTenantAlias(String tenantId) {
        if (tenantId == null) return null;
        return switch (tenantId.toLowerCase()) {
            case "aziendaa" -> "nexi";
            case "aziendab" -> "amex";
            default -> tenantId;
        };
    }

    /**
     * Gets the configuration for a specific tenant.
     * Accetta anche i vecchi id (aziendaa→nexi, aziendab→amex) per retrocompatibilità con task ECS.
     *
     * @param tenantId the tenant identifier
     * @return the tenant properties, or null if not found
     */
    public TenantProperties getTenantProperties(String tenantId) {
        if (tenantId == null) return null;
        String resolved = resolveTenantAlias(tenantId);
        return tenants.get(resolved);
    }

    /**
     * Checks if a tenant is configured.
     * 
     * @param tenantId the tenant identifier
     * @return true if the tenant is configured, false otherwise
     */
    public boolean isTenantConfigured(String tenantId) {
        if (tenantId == null) return false;
        String resolved = resolveTenantAlias(tenantId);
        return tenants.containsKey(resolved);
    }

    /**
     * Restituisce il bootstrap tenant normalizzato (aziendaa→nexi, aziendab→amex).
     * Utile quando la task definition ECS ha ancora TENANT_ID=aziendaa.
     */
    public String getBootstrapTenantResolved() {
        String raw = getBootstrapTenant();
        return resolveTenantAlias(raw);
    }
    
    /**
     * Derives the tenant identifier from the request host.
     * Used to ensure SSO tenant matches the URL (e.g. nexi-be.testpos-noprod.com → nexi, nexi-be.dominio-prod.com → nexi).
     * Il dominio base è configurabile con multi-tenant.tenant-host-base-domain (default e da CORS_TENANT_BASE_DOMAIN).
     *
     * @param host the request hostname
     * @return tenant id (nexi or amex), or null if host is not tenant-specific (e.g. App Runner URL)
     */
    public String getTenantIdFromHost(String host) {
        if (host == null || host.isEmpty()) {
            return null;
        }
        String domain = tenantHostBaseDomain != null && !tenantHostBaseDomain.isBlank() ? tenantHostBaseDomain : "testpos-noprod.com";
        String escapedDomain = Pattern.quote(domain);
        Pattern pattern = Pattern.compile("^([a-z]+)(?:-be)?\\." + escapedDomain + "$");
        Matcher matcher = pattern.matcher(host.toLowerCase());
        if (matcher.matches()) {
            return matcher.group(1);
        }
        if (host.contains("localhost") || host.contains("127.0.0.1")) {
            return "nexi";
        }
        if (host.contains("awsapprunner.com")) {
            return null;
        }
        return null;
    }

    /**
     * Gets the list of SSO providers available for a tenant.
     * Accetta anche i vecchi id (aziendaa→nexi, aziendab→amex) per retrocompatibilità con URL/host.
     *
     * @param tenantId the tenant identifier (from host or session, e.g. nexi, amex, aziendaa, aziendab)
     * @return list of SSO provider IDs, or empty list if tenant not found
     */
    public List<String> getSsoProvidersForTenant(String tenantId) {
        if (tenantId == null) return List.of();
        String resolved = resolveTenantAlias(tenantId);
        TenantProperties props = tenants.get(resolved);
        if (props != null && props.getSso() != null) {
            return props.getSso().getProviders();
        }
        return List.of();
    }
    
    /**
     * Tenant-specific properties.
     */
    public static class TenantProperties {
        private String databaseName;
        private String databaseUrl;
        private String databaseUsername;
        private String databasePassword;
        /** Codice fiscale usato in header/footer dei file di output (dipende dal tenant: Nexi 04107060966, Amex 14778691007). */
        private String outputCodiceFiscale;
        private SsoConfiguration sso;
        
        public String getDatabaseName() {
            return databaseName;
        }
        
        public void setDatabaseName(String databaseName) {
            this.databaseName = databaseName;
        }
        
        public String getDatabaseUrl() {
            return databaseUrl;
        }
        
        public void setDatabaseUrl(String databaseUrl) {
            this.databaseUrl = databaseUrl;
        }
        
        public String getDatabaseUsername() {
            return databaseUsername;
        }
        
        public void setDatabaseUsername(String databaseUsername) {
            this.databaseUsername = databaseUsername;
        }
        
        public String getDatabasePassword() {
            return databasePassword;
        }
        
        public void setDatabasePassword(String databasePassword) {
            this.databasePassword = databasePassword;
        }
        
        public String getOutputCodiceFiscale() {
            return outputCodiceFiscale;
        }
        
        public void setOutputCodiceFiscale(String outputCodiceFiscale) {
            this.outputCodiceFiscale = outputCodiceFiscale;
        }
        
        public SsoConfiguration getSso() {
            return sso;
        }
        
        public void setSso(SsoConfiguration sso) {
            this.sso = sso;
        }
    }
    
    /**
     * SSO configuration for a tenant.
     */
    public static class SsoConfiguration {
        private List<String> providers;
        
        public List<String> getProviders() {
            return providers;
        }
        
        public void setProviders(List<String> providers) {
            this.providers = providers;
        }
    }
}
