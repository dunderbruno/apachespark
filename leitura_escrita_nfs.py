from pyspark.sql import SparkSession
from pyspark.sql.functions import col


identificador_tamanho = "300mb"  # Identificador do tamanho do dataset, pode ser alterado conforme necessário

# Inicializa a sessão Spark
spark = SparkSession.builder.appName(f"Processar CSV {identificador_tamanho} rpt3").getOrCreate()

# Caminhos no NFS (acessível por driver e executores)
input_csv  = f"/mnt/spark-data/dataset_{identificador_tamanho}.csv"
output_csv = f"/mnt/spark-data/dataset_{identificador_tamanho}_output_rpt3"

# Lê o arquivo CSV do NFS
df = spark.read.option("header", "true").csv(input_csv)
df = df.repartition(3)

# Converte "Inteiro" para int
df = df.withColumn("Inteiro", col("Inteiro").cast("int"))

# Adiciona nova coluna "dobro"
df = df.withColumn("dobro", col("Inteiro") * 2)

# Escreve o resultado em um único CSV com cabeçalho no NFS
df.write.option("header", "true").mode("overwrite").parquet(output_csv)

print("Processamento finalizado. Arquivo salvo em:", output_csv)

# Finaliza a sessão Spark
spark.stop()
