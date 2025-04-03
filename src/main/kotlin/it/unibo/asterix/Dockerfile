FROM debian:latest

# Aggiorna il sistema e installa OpenJDK 17
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Imposta le variabili d'ambiente per Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Crea una directory di lavoro
WORKDIR /app

# Copia i file del progetto (se necessario)
# COPY . /app

# Comando di default: avvia una shell bash
CMD ["/bin/bash"]
