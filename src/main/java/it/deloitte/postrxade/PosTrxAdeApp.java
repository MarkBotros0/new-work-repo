package it.deloitte.postrxade;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableAsync;

import it.deloitte.postrxade.config.DefaultProfileUtil;

@SpringBootApplication(exclude = {
    org.springframework.boot.autoconfigure.http.client.HttpClientAutoConfiguration.class,
    org.springframework.boot.autoconfigure.web.client.RestClientAutoConfiguration.class
})
@EnableAsync
public class PosTrxAdeApp implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(PosTrxAdeApp.class);

    private final Environment env;

    public PosTrxAdeApp(Environment env) {
        this.env = env;
    }

    /**
     * Initializes app.
     * <p>
     * Spring profiles can be configured with a program argument
     * --spring.profiles.active=your-active-profile
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        logger.debug("Inizio [hashcode={}]", this.hashCode());

        Collection<String> activeProfiles = Arrays.asList(env.getActiveProfiles());
        String activeProfilesString = Objects.toString(activeProfiles);
        logger.debug("activeProfiles={}", activeProfilesString);

        logger.debug("Fine");
    }

    /**
     * Main method, used to run the application.
     *
     * @param args the command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(PosTrxAdeApp.class);
        DefaultProfileUtil.addDefaultProfile(app);
        Environment env = app.run(args).getEnvironment();
        logApplicationStartup(env);
    }

    private static void logApplicationStartup(Environment env) {
        String protocol = "http";
        if (env.getProperty("server.ssl.key-store") != null) {
            protocol = "https";
        }
        String serverPort = env.getProperty("server.port");
        String contextPath = env.getProperty("server.servlet.context-path");
        if (StringUtils.isBlank(contextPath)) {
            contextPath = "/";
        }
        String hostAddress = "localhost";

        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("The host name could not be determined, using `localhost` as fallback");
        }

        logger.info(
                "\n" + "----------------------------------------------------------\n" + "Application '{}' is running!\n"
                        + "Access URLs:\n" + "  Local:    {}://localhost:{}{}\n" + "  External: {}://{}:{}{}\n" + "Profile(s): {}\n"
                        + "----------------------------------------------------------",
                env.getProperty("spring.application.name"), protocol, serverPort, contextPath, protocol, hostAddress,
                serverPort, contextPath, env.getActiveProfiles());

    }
}
