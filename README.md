# football-FIFA-big-data
## Proyecto Big Data Mundiales de Futbol FIFA
Proyecto de big data aplicando diversas tecnologías como Hive, Spark y Hadoop sobre un dataset de los mundiales de fútbol de la FIFA.


### JSDAVILAS

### Objetivo
---
1. Desarrollar un proyecto de big data aplicando diversas tecnologías como Hive, Spark y Hadoop. 
2. Analizar los datos de los mundiales de fútbol de la FIFA, aplicar formatos y agrupaciones para obtener reportes.

### Tecnologías a usar

- pySpark: Lenguaje para manipular los dataframe de Spark en memoria
- Hadoop: Permite el almacenamiento distribuido en un clúster
- Hive: motor de base de datos distribuido. 

Se usará una máquina virtual  en  virtualbox con estas tecnologías instaladas 
para la parte de HDFS HIVE . Se usará  Databricks para la parte de pySpark.

### Archivos de Datos 

Se usará los siguientes CSV:

- WorldCups.csv: lista de los mundiales de futbol, año en que se realizaron y países ganadores.
- WorldCupPlayers.csv: lista de los jugadores en cada partido.
- WorldCupMatches: lista de partidos, cantidad de goles anotados y asistentes.


### Archivos de Configuracion 

Se usará los siguientes scripts:

- Script HDFS: Contiene los comandos de creación de carpetas y permisos en HDFS
- Script Hive: Contiene las sentencias para la creación de tablas en Hive.
- etl_proyectobigdata.py : Contiene comandos en python para la creación de los spark Dataframe y su manipulación para generar los reportes . Se incluye también las versiones en notebook y html de este archivo con las salidas de los comandos.


#### Pre requisitos

1. Tener una cuenta en databricks
2. Tener una maquina virtual con Hadoop y Hive instalado.

### Desarrollo
1. Subir los archivos a Linux
2. En el hadoop ejecutar el script HDFS
3. En el Hive ejecutar el Script Hive
4. Subir los archivos al databricks
5. Ejecutar el archivo etl_proyectobigdata.py en databricks
6. Visualizar los reportes en el databricks


### Reportes generados 

1. Reporte de mundiales en los que Perú participó :Con la nueva partipación de Perú en el último mundial  la Federación Peruana de Fútbol  solicitó a la FIFA  un reporte de los últimos mundiales en los que Perú participó para recordar a todos sus jugadores.
2. Reporte de estadística de jugadores:  Este reporte contiene el número de goles, penales, penales fallidos y tarjetas rojas por jugador de los mundiales. Sirve como estadística para otorgar los premios respectivos a los jugadores con mejor desempeño.
