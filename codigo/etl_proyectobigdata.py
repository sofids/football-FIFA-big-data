# Databricks notebook source
sc

# COMMAND ----------

spark

# COMMAND ----------


#leyendo los csv
df_wc=spark.read.csv("/FileStore/tables/WorldCups.csv",header=True)
df_wcm=spark.read.option("delimiter",";").csv("/FileStore/tables/WorldCupMatches.csv",header=True)
df_wcp=spark.read.csv("/FileStore/tables/WorldCupPlayers.csv",header=True)
#Cargando en temporales
df_wc.createOrReplaceTempView("WorldCup")
df_wcm.createOrReplaceTempView("WorldCupMatches")
df_wcp.createOrReplaceTempView("WorldCupPlayers")
  
#Mostrando la data
spark.sql(""" select x.* from WorldCup x""").show()



# COMMAND ----------

#verificando la estructura del dataframe WordlCup
df_wc.printSchema()
print('El dataframe df_wc tiene '+str(df_wc.count())+' registros.')
print('El dataframe df_wc tiene '+str(df_wc.distinct().count())+' registros distintos.')

# COMMAND ----------

#verificando la estructura del dataframe WordlCupPlayers
df_wcp.printSchema()
print('El dataframe df_wcp tiene '+str(df_wcp.count())+' registros.')
print('El dataframe df_wcp tiene '+str(df_wcp.distinct().count())+' registros distintos.')

# COMMAND ----------

#verificando la estructura del dataframe WordlCupMatches
df_wcm.printSchema()
print('El dataframe df_wc tiene '+str(df_wcm.count())+' registros.')
print('El dataframe df_wc tiene '+str(df_wcm.distinct().count())+' registros distintos.')

# COMMAND ----------

from pyspark.sql.functions import desc, asc, col, column, expr, instr,length, substring, regexp_replace,trim,lit,initcap, sum,concat
#
#Transformaciones al datraframe WordlCupPlayers 
# El campo Event guarda un dato del tipo "G43' G87'", lo que significa que el jugador usar dos goles, por lo que necesitamos contar el número de "G"
# De igual manera podemos obtener el número de penales y tarjetas
df_wcp1=df_wcp.withColumn('POSICION_JUGADOR',expr("case when position='C' THEN  'Captain' WHEN position='GK' THEN 'Goalkeeper' ELSE 'Other' end "))\
             .withColumn('NOMBRE_JUGADOR', initcap(regexp_replace('Player Name','�','u')))\
             .withColumn('NUMERO_GOLES',length('Event')-length(trim(regexp_replace('Event','G',''))))\
             .withColumn('NUMERO_PENALES',length('Event')-length(trim(regexp_replace('Event','P',''))))\
             .withColumn('NUMERO_PENALES_FALLADOS',length('Event')-length(trim(regexp_replace('Event','MP',''))))\
             .withColumn('NUMERO_TARJETAS_ROJAS',length('Event')-length(trim(regexp_replace('Event','R',''))))
#Reemplazo los valores nulos con 0                 
df_wcp1=df_wcp1.withColumn('NUMERO_GOLES',expr("case when NUMERO_GOLES is null then 0 else NUMERO_GOLES end "))\
              .withColumn('NUMERO_PENALES',expr("case when NUMERO_PENALES is null then 0 else NUMERO_PENALES end "))\
              .withColumn('NUMERO_PENALES_FALLADOS',expr("case when NUMERO_PENALES_FALLADOS is null then 0 else NUMERO_PENALES_FALLADOS end "))\
              .withColumn('NUMERO_TARJETAS_ROJAS',expr("case when NUMERO_TARJETAS_ROJAS is null then 0 else NUMERO_TARJETAS_ROJAS end "))\
              .withColumnRenamed('Team Initials','INICIALES_PAIS')\
              .drop('Player Name','Position','Shirt Number')

#Sumarizo  por nombre de jugador , posición e iniciales de país
df_wcp_rep=df_wcp1.select('NOMBRE_JUGADOR','POSICION_JUGADOR','INICIALES_PAIS','NUMERO_GOLES','NUMERO_PENALES','NUMERO_PENALES_FALLADOS','NUMERO_TARJETAS_ROJAS')\
.groupby('NOMBRE_JUGADOR','POSICION_JUGADOR','INICIALES_PAIS')\
.sum('NUMERO_GOLES','NUMERO_PENALES','NUMERO_PENALES_FALLADOS','NUMERO_TARJETAS_ROJAS')

#Renombro columnas
df_wcp_rep=df_wcp_rep.withColumnRenamed('sum(NUMERO_PENALES)','NUMERO_PENALES')\
                     .withColumnRenamed('sum(NUMERO_PENALES_FALLADOS)','NUMERO_PENALES_FALLADOS')\
                     .withColumnRenamed('sum(NUMERO_GOLES)','NUMERO_GOLES')\
                     .withColumnRenamed('sum(NUMERO_TARJETAS_ROJAS)','NUMERO_TARJETAS_ROJAS')

df_wcp_rep_final=df_wcp_rep.orderBy(desc('NUMERO_GOLES'))
df_wcp_rep_final.show()


# COMMAND ----------

#guardando en el filestore
df_wcp_rep_final.write.format("csv").mode("overwrite").option("path","/FileStore/tables/reporte_estadistica_jugadores/").option("header","true").save()


# COMMAND ----------

#recuperando el archivo guardado
df_wcp_rep_csv=spark.read.csv("/FileStore/tables/reporte_estadistica_jugadores/",header=True)
df_wcp_rep_csv.show()

# COMMAND ----------

#mostrándolo en formato Pandas
df_pandas_wcp_csv=df_wcp_rep_csv.toPandas()
df_pandas_wcp_csv.sort_values(by=['NUMERO_GOLES'],inplace=True, ascending=False)
df_pandas_wcp_csv.head()



# COMMAND ----------

#Verificando para un jugador
df_pandas_wcp_csv_f=df_pandas_wcp_csv[df_pandas_wcp_csv.NOMBRE_JUGADOR == 'Klose']
df_pandas_wcp_csv_f.head()

# COMMAND ----------

#Realizando transformaciones para el segundo reporte.
df_wcm1=df_wcm.withColumn('FECHA_PARTIDO',substring('Datetime',1,12))\
             .withColumn('NOMBRE_REFEREE', initcap(regexp_replace('Referee','�','u')))\
             .withColumn('ESTADIO', initcap('Stadium'))
df_wcm1=df_wcm1.select('RoundID','MatchID' , 'FECHA_PARTIDO','NOMBRE_REFEREE','ESTADIO','Year',
                      col('HomeTeam Name').alias('EQUIPO DE CASA'),col('AwayTeamName').alias('EQUIPO EXTERNO'))
df_wc1=df_wc.withColumn('MUNDIAL', concat('Country',lit('-'),'Year'))
df_wc1=df_wc1.withColumnRenamed('Year','ANIO')
df_wc1=df_wc1.select('MUNDIAL','ANIO')
#filtrando sólo PERU 
df_wcp1=df_wcp1.filter(col('INICIALES_PAIS')=='PER')





# COMMAND ----------

#revisando el dataframe de worldcupmatches formateado
df_wcm1.show()

# COMMAND ----------

#revisando el dataframe de worldcup formateado
df_wc1.show()

# COMMAND ----------

#revisando el dataframe de worldcuplayer formateado
df_wcp1.show()

# COMMAND ----------

#join del dataframe de worldcup con el de worldcupmatch
df_wc_join_wcm=df_wc1.join(df_wcm1,df_wc1.ANIO==df_wcm1.Year,'inner')
#viendo el resultado del primer join
df_wc_join_wcm.show()


# COMMAND ----------

#uniendo el tercer dataframe(worldcupplayer) al join anterior
df_wc_join_wcm_join_wcp=df_wcp1.join(df_wc_join_wcm,df_wc_join_wcm.MatchID==df_wcp1.MatchID,'inner')
df_mundiales_peruanos_rep=df_wc_join_wcm_join_wcp.select('MUNDIAL','FECHA_PARTIDO','EQUIPO DE CASA',
                               'EQUIPO EXTERNO','NOMBRE_JUGADOR','POSICION_JUGADOR','INICIALES_PAIS','NOMBRE_REFEREE').orderBy('ANIO')
df_mundiales_peruanos_rep.show()

# COMMAND ----------

#guardando el segundo reporte de mundiales peruanos
df_mundiales_peruanos_rep.write.format("csv").mode("overwrite").option("path","/FileStore/tables/reporte_mundiales_peruanos/").option("header","true").save()
#recuperandolo en csv
df_mundiales_peruanos_rep_csv=spark.read.csv("/FileStore/tables/reporte_mundiales_peruanos/",header=True)
df_mundiales_peruanos_rep_csv.show()



# COMMAND ----------

#mostrándolo en formato panda
df_pandas_mundiales_peruanos=df_mundiales_peruanos_rep_csv.toPandas()
df_pandas_mundiales_peruanos.head()
