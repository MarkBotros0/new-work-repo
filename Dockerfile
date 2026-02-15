# Multi-stage build per ottimizzare le dimensioni dell'immagine
FROM maven:3.9.6-eclipse-temurin-21 AS build

# Imposta la directory di lavoro
WORKDIR /app

# Copia i file di configurazione Maven
COPY pom.xml .

# Scarica le dipendenze (questo layer verrà cachato se pom.xml non cambia)
RUN mvn dependency:go-offline -B

# Copia il codice sorgente
COPY src src

# Compila l'applicazione e crea il JAR
RUN mvn clean package -DskipTests -B

# Stage di runtime
FROM eclipse-temurin:21-jre

# Imposta la directory di lavoro
WORKDIR /app

# Copia il JAR dallo stage di build
COPY --from=build /app/target/*.jar app.jar

# Espone la porta dell'applicazione
EXPOSE 8080

# Configura le variabili d'ambiente per AWS App Runner
ENV SPRING_PROFILES_ACTIVE=dev
# Heap sizing for 8GB App Runner instance (can be overridden by environment variable)
# -Xms2g: Initial heap 2GB
# -Xmx5g: Max heap 5GB (leaving ~3GB for OS, metaspace, native memory)
ENV JAVA_OPTS="-Xms2g -Xmx5g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseContainerSupport --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED"

# Comando per avviare l'applicazione
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]