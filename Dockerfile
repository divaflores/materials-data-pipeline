# Imagen base con Java (Spark necesita Java 8+)
FROM openjdk:11

# Instala Python y dependencias básicas
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv

# Define variables de entorno
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONUNBUFFERED=1

# Crea una carpeta de trabajo dentro del contenedor
WORKDIR /app

# Copia todo el proyecto al contenedor
COPY . /app

# Instala dependencias
RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt

# Comando por defecto al ejecutar el contenedor
CMD ["python3", "jobs/main.py", "--config", "config/config.yaml"]