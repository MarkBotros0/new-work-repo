# POS Transaction Ade Backend

Backend application for POS Transaction Ade system built with Spring Boot.

## Features

- **Spring Boot 3.5.6** with Java 21
- **JPA/Hibernate** for data persistence
- **Orika Mapper** for object mapping
- **Logback** for logging with configurable profiles
- **Spring Security** with OAuth2 support
- **MySQL** (AWS RDS/Aurora), **PostgreSQL** and **H2** database support
- **Docker** containerization
- **RESTful API** with OpenAPI documentation
- **Internationalization** support (IT/EN)

## Project StructureN

```
src/
├── main/
│   ├── java/
│   │   └── it/deloitte/postrxade/
│   │       ├── config/          # Configuration classes
│   │       ├── controller/      # REST controllers
│   │       ├── dto/            # Data Transfer Objects
│   │       ├── entity/         # JPA entities
│   │       ├── exception/      # Custom exceptions
│   │       ├── mapper/         # Orika mappers
│   │       ├── repository/     # JPA repositories
│   │       ├── security/       # Security configuration
│   │       ├── service/        # Business logic
│   │       │   └── impl/       # Service implementations
│   │       ├── util/           # Utility classes
│   │       ├── web/            # Web layer
│   │       │   └── rest/       # REST controllers
│   │       └── PosTrxAdeApp.java
│   └── resources/
│       ├── config/             # Configuration files
│       ├── i18n/              # Internationalization
│       └── application*.yml   # Application properties
└── test/                      # Test classes
```

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.6+
- Docker (optional)

### Running the Application

#### Development Mode (H2 Database)

```bash
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

#### Development Mode (MySQL)

1. Start MySQL database:
```bash
docker-compose up mysql -d
```

2. Run the application:
```bash
mvn spring-boot:run -Dspring-boot.run.profiles=mysql
```

#### Production Mode (MySQL)

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=prod
```

#### AWS RDS/Aurora Mode

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=aws
```

### Docker

#### Build and run with Docker Compose (MySQL)

```bash
docker-compose up --build
```

#### Build and run with Docker Compose (PostgreSQL)

```bash
docker-compose --profile postgres up --build
```

#### Build Docker image

```bash
mvn clean package
docker build -t pos-trx-ade:latest .
```

## API Documentation

Once the application is running, you can access:

- **Swagger UI**: http://localhost:8080/api/docs/index.html
- **API Docs**: http://localhost:8080/api/docs/api-docs
- **REST Docs**: http://localhost:8080/docs/index.html (after running tests)
- **Health Check**: http://localhost:8080/private-monitor/health

## Configuration

### Profiles

- `dev`: Development profile with H2 database (default)
- `mysql`: Development profile with MySQL
- `prod`: Production profile with MySQL
- `aws`: AWS RDS/Aurora MySQL profile
- `docker-compose`: Docker Compose development profile

### Environment Variables

For production deployment, set these environment variables:

#### MySQL/PostgreSQL
```bash
DB_HOST=localhost
DB_PORT=3306  # 5432 for PostgreSQL
DB_NAME=postrxade
DB_USERNAME=root  # postgres for PostgreSQL
DB_PASSWORD=password
```

#### AWS RDS/Aurora MySQL
```bash
DB_HOST=your-rds-endpoint.region.rds.amazonaws.com
DB_PORT=3306
DB_NAME=postrxade
DB_USERNAME=your-username
DB_PASSWORD=your-password
DB_MAX_POOL_SIZE=20
DB_MIN_IDLE=5
```

#### OAuth2 Configuration
```bash
OAUTH2_AUTHORIZATION_URI=http://localhost:8080/oauth2/authorize
OAUTH2_TOKEN_URI=http://localhost:8080/oauth2/token
OAUTH2_JWK_SET_URI=http://localhost:8080/.well-known/jwks.json
OAUTH2_CLIENT_ID=pos-trx-ade-client
OAUTH2_CLIENT_SECRET=pos-trx-ade-secret
OAUTH2_REDIRECT_URI=http://localhost:8080/api/login/oauth2/code/oidc
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4200
```

## Logging

Logging is configured with Logback and supports multiple profiles:

- Console logging for development
- File logging with rotation for production
- Different log levels per profile

Log files are stored in the `log/` directory.

## API Documentation

### Spring REST Docs

The project includes Spring REST Docs for generating comprehensive API documentation. To generate the documentation:

```bash
# Run tests to generate REST Docs
mvn test

# The documentation will be available at:
# target/generated-snippets/
# target/generated-docs/
```

### SpringDoc OpenAPI (Swagger)

Interactive API documentation is available at:
- **Swagger UI**: http://localhost:8080/api/docs/index.html
- **OpenAPI JSON**: http://localhost:8080/api/docs/api-docs

#### Generazione Codice Frontend

Con Swagger UI puoi:

1. **Visualizzare tutte le API** disponibili con documentazione completa
2. **Testare le chiamate** direttamente dall'interfaccia web
3. **Esportare lo schema OpenAPI** per generare codice frontend
4. **Copiare le chiamate cURL** per implementazione
5. **Visualizzare modelli di dati** per DTO e entità

#### Esempio di Utilizzo

1. Avvia l'applicazione: `mvn spring-boot:run`
2. Apri Swagger UI: http://localhost:8080/api/docs/index.html
3. Seleziona un endpoint (es. `GET /api/transactions`)
4. Clicca "Try it out" per testare
5. Copia la richiesta cURL o usa il JSON schema per il frontend

#### Documentation Best Practices

**Recommendation**: Use Swagger annotations for REST APIs and Javadoc for internal code documentation. This approach provides:
- Interactive API documentation in Swagger UI
- Automatic client code generation
- Comprehensive internal code documentation
- Best of both worlds for complete project documentation

## Technologies Used

- **Spring Boot 3.5.6**
- **Spring Data JPA**
- **Spring Security**
- **Spring Web**
- **Hibernate 6.6.0**
- **Orika 1.5.4**
- **MySQL 8.0**
- **PostgreSQL 15**
- **H2 Database**
- **Maven 3.9.4** (or use included Maven wrapper)
- **Docker**
- **Logback**
- **Lombok**
- **SpringDoc OpenAPI 2.7.0**
- **Spring REST Docs** for API documentation
- **Spring Boot Docker Compose** support
- **Thymeleaf Security** integration

## AWS RDS/Aurora Setup

### Prerequisites

1. AWS Account with RDS access
2. MySQL 8.0 RDS instance or Aurora MySQL cluster
3. Security groups configured to allow access from your application

### RDS Configuration

1. **Create RDS Instance**:
   - Engine: MySQL 8.0
   - Instance class: db.t3.micro (for development) or db.r5.large (for production)
   - Storage: 20GB minimum
   - Backup retention: 7 days
   - Multi-AZ: Enabled for production

2. **Security Group**:
   - Allow inbound traffic on port 3306 from your application's security group
   - Or from specific IP addresses

3. **Database Configuration**:
   ```sql
   CREATE DATABASE postrxade CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   CREATE USER 'posuser'@'%' IDENTIFIED BY 'your-secure-password';
   GRANT ALL PRIVILEGES ON postrxade.* TO 'posuser'@'%';
   FLUSH PRIVILEGES;
   ```

### Environment Variables for AWS

Set these environment variables for AWS deployment:

```bash
export SPRING_PROFILES_ACTIVE=aws
export DB_HOST=your-rds-endpoint.region.rds.amazonaws.com
export DB_PORT=3306
export DB_NAME=postrxade
export DB_USERNAME=posuser
export DB_PASSWORD=your-secure-password
export DB_MAX_POOL_SIZE=20
export DB_MIN_IDLE=5
```

### Deployment Options

#### Option 1: ECS with RDS
```bash
# Build and push to ECR
mvn clean package
docker build -t pos-trx-ade:latest .
docker tag pos-trx-ade:latest your-account.dkr.ecr.region.amazonaws.com/pos-trx-ade:latest
docker push your-account.dkr.ecr.region.amazonaws.com/pos-trx-ade:latest

# Deploy to ECS with RDS connection
```

#### Option 2: EC2 with RDS
```bash
# Deploy application to EC2
# Configure environment variables
# Start application
java -jar pos-trx-ade.jar --spring.profiles.active=aws
```

## License

This project is proprietary software of Deloitte.