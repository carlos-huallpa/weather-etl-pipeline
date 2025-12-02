# Weather_ETL_pipeline
## Breve descripción 
Este proyecto tiene como objetivo implementar una pipeline Extrac-Tranform-Load (ETL) de parametros climáticos de interés para la industria agricola. En principio toma valores recopilados de una API de referencia y calcula parametros derivados de los mismos. El flujo de trabajo incluye una serie de validaciones para garantizar la calidad de los datos antes de almacenarlos en un storage cloud (AWS S3). Se prevee aumentar la cantidad  datos derivados calculados, pero para una mayor precisión se requiere la obtención de datos de fuentes más precisas, como sensores o estaciones metereologicas locales

# Características

ETL automatizado: Extracción diaria, transformación con pandas, carga a S3
Calidad de datos: Validación de schema, nulls y lógica de negocio
Resiliencia: Reintentos automáticos, manejo de errores, logging estructurado
Métricas calculadas: Amplitud térmica, estrés térmico, riesgo de heladas

# Configuración
Para obtener datos pertinente a una locación particular, es necesario configurar las constantes correspondientes (asi como la creación de un bucket S3 destino):

LATITUDE = '-33.1237585'   # Coordenadas de la ubicación deseada 
LONGITUDE = '-64.3489782'  
S3_BUCKET = "tu-bucket"    # Nombre del bucket S3 destino

# Tecnologías utilizadas

Orquestación: Apache Airflow 2.0+
Procesamiento: Pandas
Almacenamiento: AWS S3
API: Open-Meteo (gratuita, sin API key)
Contenedorización del proyecto: Docker

# Autor
Carlos Huallpa
GitHub: @carlos-huallpa