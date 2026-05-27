# Databricks notebook source
# DBTITLE 0,Backfill: Limite por Categoria ME BR
# MAGIC %md
# MAGIC ## Backfill — category_limit_me_br / customer_top_category_me_br
# MAGIC
# MAGIC Reprocessa todas as safras mensais dentro de um intervalo de datas,
# MAGIC chamando o notebook `01_category_limit_me_br` para cada safra.
# MAGIC
# MAGIC ### Logica de janelas
# MAGIC
# MAGIC | Tabela | Janela | Observacao |
# MAGIC |--------|--------|------------|
# MAGIC | `category_limit_me_br` | 3 meses anteriores a cada safra | Safra 2020-01-01 → dados de out-dez/2019 |
# MAGIC | `customer_top_category_me_br` | 24 meses anteriores a cada safra | Safra 2020-01-01 → dados de jan/2018 a dez/2019 |
# MAGIC
# MAGIC ### Idempotencia
# MAGIC
# MAGIC O notebook filho usa MERGE com chave composta `(reference_quarter, category)` /
# MAGIC `(reference_quarter, customer_code)`. Reexecutar o backfill nao duplica registros,
# MAGIC apenas atualiza safras existentes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Parametros deste notebook
# MAGIC
# MAGIC | Parametro | Default | Descricao |
# MAGIC |-----------|---------|-----------|
# MAGIC | `data_inicio_backfill` | `2020-01-01` | Primeira safra a processar (primeiro dia do mes) |
# MAGIC | `data_fim_backfill` | hoje | Ultima safra a processar |
# MAGIC | `notebook_path` | (relativo) | Caminho do notebook filho |
# MAGIC | `timeout_por_safra` | `600` | Timeout em segundos por execucao |

# COMMAND ----------

# DBTITLE 0,Parametros do backfill
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

dbutils.widgets.text("data_inicio_backfill", "2020-01-01", "Inicio do Backfill (YYYY-MM-DD)")
dbutils.widgets.text("data_fim_backfill",    "",           "Fim do Backfill (YYYY-MM-DD, vazio = hoje)")
dbutils.widgets.text("notebook_path",        "./01b_category_limit_me_br", "Caminho do Notebook Filho")
dbutils.widgets.text("timeout_por_safra",    "600",        "Timeout por Safra (segundos)")

_inicio_str  = dbutils.widgets.get("data_inicio_backfill")
_fim_str     = dbutils.widgets.get("data_fim_backfill")
_nb_path     = dbutils.widgets.get("notebook_path")
_timeout     = int(dbutils.widgets.get("timeout_por_safra"))

# Normaliza para primeiro dia do mes em ambos os extremos
def primeiro_dia_mes(d: date) -> date:
    return d.replace(day=1)

data_inicio = primeiro_dia_mes(date.fromisoformat(_inicio_str))
data_fim    = primeiro_dia_mes(date.fromisoformat(_fim_str) if _fim_str else date.today())

# Gera lista de safras mensais [data_inicio, data_fim] inclusive
safras = []
cursor = data_inicio
while cursor <= data_fim:
    safras.append(cursor.isoformat())
    cursor += relativedelta(months=1)

print(f"Notebook filho  : {_nb_path}")
print(f"Timeout/safra   : {_timeout}s")
print(f"Safra inicial   : {safras[0]}")
print(f"Safra final     : {safras[-1]}")
print(f"Total de safras : {len(safras)}")

# COMMAND ----------

# DBTITLE 0,Execucao do backfill
from datetime import datetime

resultados = []  # lista de dicts para log final

print(f"{'#':>4}  {'Safra':12}  {'Status':8}  {'Tempo(s)':>8}  Detalhe")
print("-" * 70)

for i, safra in enumerate(safras, start=1):
    t0 = datetime.now()
    try:
        dbutils.notebook.run(
            _nb_path,
            timeout_seconds=_timeout,
            arguments={"data_referencia": safra}
        )
        elapsed = (datetime.now() - t0).total_seconds()
        status  = "OK"
        detalhe = ""
    except Exception as e:
        elapsed = (datetime.now() - t0).total_seconds()
        status  = "ERRO"
        detalhe = str(e)[:120]

    resultados.append({
        "safra":    safra,
        "status":   status,
        "tempo_s":  round(elapsed, 1),
        "detalhe":  detalhe
    })
    print(f"{i:>4}  {safra:12}  {status:8}  {elapsed:>8.1f}  {detalhe}")

# COMMAND ----------

# DBTITLE 0,Resumo do backfill
total     = len(resultados)
ok        = sum(1 for r in resultados if r["status"] == "OK")
erros     = total - ok
tempo_total = sum(r["tempo_s"] for r in resultados)

print("=" * 50)
print(f"  Safras processadas : {total}")
print(f"  Sucesso            : {ok}")
print(f"  Erros              : {erros}")
print(f"  Tempo total        : {tempo_total:.0f}s ({tempo_total/60:.1f} min)")
print("=" * 50)

if erros > 0:
    print("\nSafras com erro:")
    for r in resultados:
        if r["status"] == "ERRO":
            print(f"  {r['safra']}  →  {r['detalhe']}")

# COMMAND ----------

# DBTITLE 0,Log persistente (opcional — salva em Delta)
# MAGIC %md
# MAGIC > **Opcional**: descomente o bloco abaixo para persistir o log de execucao em Delta.
# MAGIC > Util para auditar reprocessamentos futuros.

# COMMAND ----------

# from pyspark.sql import Row
# from pyspark.sql.functions import current_timestamp
#
# log_df = spark.createDataFrame([Row(**r) for r in resultados]) \
#               .withColumn("executed_at", current_timestamp())
#
# log_df.write \
#       .format("delta") \
#       .mode("append") \
#       .saveAsTable("teste.backfill_log_category_limit_me_br")
#
# display(log_df.orderBy("safra"))