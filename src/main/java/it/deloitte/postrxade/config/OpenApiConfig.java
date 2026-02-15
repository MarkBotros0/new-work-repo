package it.deloitte.postrxade.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@org.springframework.context.annotation.Profile("!batch")
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("POS Transaction Ade API")
                        .description("Backend API for POS Transaction Ade system")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("Deloitte Development Team")
                                .email("dev@deloitte.com")
                                .url("https://deloitte.com"))
                        .license(new License()
                                .name("Proprietary")
                                .url("https://deloitte.com/license")))
                .servers(List.of(
                        new Server()
                                .url("http://localhost:8082")
                                .description("Development server"),
                        new Server()
                                .url("https://pos-trx-ade.deloitte.com")
                                .description("Production server")
                ));
    }
}
