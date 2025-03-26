# Imagen base con Java (Spark necesita Java 8+)
FROM openjdk:11

# Instala Python y dependencias básicas
RUN apt-get update && apt-get install -y python python-pip python-venv

# Define variables de entorno
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python
ENV PYTHONUNBUFFERED=1

# Crea una carpeta de trabajo dentro del contenedor
WORKDIR /app

# Copia todo el proyecto al contenedor
COPY . /app

# Instala dependencias
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Comando por defecto al ejecutar el contenedor
CMD ["python", "-m", "jobs.run_pipeline"]