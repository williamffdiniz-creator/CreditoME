# Databricks notebook source
# DBTITLE 0,Pipeline: Monitoring ME BR
# MAGIC %md
# MAGIC ## Pipeline: Monitoramento do Modelo - ME BR
# MAGIC
# MAGIC Consolida o detalhe cliente x safra em `teste.monitoring_me_br` e gera as
# MAGIC **16 tabelas de monitoramento** no mesmo formato (tidy/long) do padrao
# MAGIC corporativo enviado no Excel `Monitoramento_me`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Adaptacao: modelo logistico -> formula ponderada
# MAGIC
# MAGIC O Excel original monitora um modelo de **regressao logistica** (dummies +
# MAGIC coeficientes + decis). O modelo ME BR atual NAO e logistico: e uma
# MAGIC **formula ponderada pela severidade dos atrasos, peso historico e grupo
# MAGIC economico** (`adjusted_score = C*score + (1-C)*portfolio_score`, onde
# MAGIC `score = Σ(pct_faixa × mediana_faixa)/10000`).
# MAGIC
# MAGIC Mapeamento aplicado (mantem nomes de tabela/coluna onde faz sentido):
# MAGIC
# MAGIC | Conceito logistico | Equivalente na formula ponderada |
# MAGIC |--------------------|----------------------------------|
# MAGIC | Decil (1-10)       | **Banda de score** (1-BAIXO, 2-MEDIO, 3-ALTO) |
# MAGIC | Dummies do modelo  | **Features que entram na formula** (pct_meses_*, peso, prazo, exposicao) |
# MAGIC | Coeficiente        | **Peso efetivo** (mediana do cluster / historical_weight); NULL quando nao aplicavel |
# MAGIC | Risco relativo     | **Lift** = bad_rate(faixa) / bad_rate(geral) |
# MAGIC | IEP                | **PSI** (Indice de Estabilidade Populacional) |
# MAGIC | Performance ROC/KS | ROC/KS/Gini sobre `integrated_score` |
# MAGIC
# MAGIC A coluna fisica continua chamando `decil` para compatibilidade de
# MAGIC dashboards; os valores sao 1/2/3 (banda) e -1 para sem score.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tabelas produzidas (sufixo `_me_br`)
# MAGIC
# MAGIC | Tabela | Conteudo |
# MAGIC |--------|----------|
# MAGIC | `monitoring_me_br` | Detalhe cliente x safra (features + scores + targets) |
# MAGIC | `performance_me_br` | ROC, KS, Gini por periodo |
# MAGIC | `dist_por_decil_me_br` | % populacao por banda |
# MAGIC | `dist_por_decil_diff_pp_me_br` | Diferenca pp vs baseline (Train) |
# MAGIC | `perc_bad_decil_me_br` | Bad rate por banda |
# MAGIC | `perc_bad_decil_diff_pp_me_br` | Diferenca pp do bad rate vs Train |
# MAGIC | `df_dist_bad_decil_me_br` | % dos bads concentrado em cada banda |
# MAGIC | `df_dist_bad_decil_diff_pp_me_br` | Diferenca pp vs Train |
# MAGIC | `decile_migrations_me_br` | Matriz de migracao de banda (mes-1 -> mes) |
# MAGIC | `iep_por_decil_me_br` | PSI da distribuicao de bandas |
# MAGIC | `iep_me_br` | PSI por feature do modelo |
# MAGIC | `iep_vars_me_br` | PSI por feature dentro do grupo |
# MAGIC | `dist_vars_me_br` | % populacao por faixa de cada feature |
# MAGIC | `dist_vars_diff_pp_me_br` | Diferenca pp vs Train |
# MAGIC | `vol_vars_me_br` | Volume (contagem) por faixa de cada feature |
# MAGIC | `risco_relativo_me_br` | Lift por faixa de cada feature |
# MAGIC | `risco_relativo_trigger_me_br` | Gatilho booleano de instabilidade do lift |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Comportamento de ingestao (historico preservado)
# MAGIC
# MAGIC TODAS as tabelas usam `CREATE TABLE IF NOT EXISTS` + `MERGE`.
# MAGIC **Nenhum DROP**: o historico nunca e apagado. Reexecutar uma safra
# MAGIC atualiza apenas as linhas daquela safra (idempotente); safras antigas
# MAGIC permanecem intactas.
# MAGIC
# MAGIC ### Pre-requisitos
# MAGIC
# MAGIC NB1 (abt + portfolio), NB2/NB2b (apply_model), NB3 (targets) executados.

# COMMAND ----------

# DBTITLE 1,Parametros do pipeline
from datetime import date

dbutils.widgets.text("data_referencia", "", "Data de Referencia")
dbutils.widgets.text("train_cutoff", "2024-01-01", "Corte In-Time/OOT (Train < corte)")

data_referencia = dbutils.widgets.get("data_referencia")
train_cutoff = dbutils.widgets.get("train_cutoff") or "2024-01-01"
effective_date = data_referencia if data_referencia else str(date.today())

MARKET_NAME = "me"

spark.sql(
    f"CREATE OR REPLACE TEMP VIEW config_pipeline AS "
    f"SELECT CAST('{effective_date}' AS DATE) AS data_referencia, "
    f"CAST('{train_cutoff}' AS DATE) AS train_cutoff"
)
print(f"Data de referencia: {effective_date}")
print(f"Train cutoff (In-Time < corte; OOT >= corte): {train_cutoff}")

# COMMAND ----------

# DBTITLE 1,Tabela detalhe: monitoring_me_br (CREATE IF NOT EXISTS - historico preservado)
# MAGIC %sql
# MAGIC -- Detalhe cliente x safra. Sem DROP: o historico e preservado.
# MAGIC -- Features/scores sao imutaveis; colunas de target atualizam retroativo.
# MAGIC CREATE TABLE IF NOT EXISTS teste.monitoring_me_br (
# MAGIC   id_customer                    INT,
# MAGIC   customer_name                  STRING,
# MAGIC   country                        STRING,
# MAGIC   reference_month                DATE,
# MAGIC   feature_reference_month        DATE,
# MAGIC   total_amount                   DOUBLE,
# MAGIC   overdue_amount                 DOUBLE,
# MAGIC   overdue_pct                    DOUBLE,
# MAGIC   months_with_billing            INT,
# MAGIC   months_defaulted               INT,
# MAGIC   pct_months_overdue_10_20       DOUBLE,
# MAGIC   pct_months_overdue_20_30       DOUBLE,
# MAGIC   pct_months_overdue_30_50       DOUBLE,
# MAGIC   pct_months_overdue_50_plus     DOUBLE,
# MAGIC   flag_transacted                INT,
# MAGIC   score                          DOUBLE,
# MAGIC   historical_weight              DOUBLE,
# MAGIC   reference_value                DOUBLE,
# MAGIC   reference_value_clp            DOUBLE,
# MAGIC   payment_term                   DOUBLE,
# MAGIC   median_cluster_10              DOUBLE,
# MAGIC   median_cluster_20              DOUBLE,
# MAGIC   median_cluster_30              DOUBLE,
# MAGIC   median_cluster_50              DOUBLE,
# MAGIC   portfolio_score                DOUBLE,
# MAGIC   adjusted_score                 DOUBLE,
# MAGIC   score_band                     STRING,
# MAGIC   integrated_score               DOUBLE,
# MAGIC   integrated_score_band          STRING,
# MAGIC   credit_limit                   DOUBLE,
# MAGIC   credit_limit_clp               DOUBLE,
# MAGIC   credit_limit_end               DOUBLE,
# MAGIC   credit_limit_end_clp           DOUBLE,
# MAGIC   target_percent7mob1            INT,
# MAGIC   target_percent7mob3            INT,
# MAGIC   target_percent7mob6            INT,
# MAGIC   target_percent7mob9            INT,
# MAGIC   target_percent7mob12           INT,
# MAGIC   target_billed_1m               DOUBLE,
# MAGIC   target_overdue_1m              DOUBLE,
# MAGIC   target_overdue_pct_1m          DOUBLE,
# MAGIC   target_billed_3m               DOUBLE,
# MAGIC   target_overdue_3m              DOUBLE,
# MAGIC   target_overdue_pct_3m          DOUBLE,
# MAGIC   target_billed_6m               DOUBLE,
# MAGIC   target_overdue_6m              DOUBLE,
# MAGIC   target_overdue_pct_6m          DOUBLE,
# MAGIC   target_billed_12m              DOUBLE,
# MAGIC   target_overdue_12m             DOUBLE,
# MAGIC   target_overdue_pct_12m         DOUBLE,
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 1,Pre-check: validar tabelas de entrada
# MAGIC %sql
# MAGIC SELECT 'abt_inference_me_br' AS tabela, COUNT(*) AS total_linhas,
# MAGIC   CAST(MIN(reference_month) AS STRING) AS safra_min,
# MAGIC   CAST(MAX(reference_month) AS STRING) AS safra_max
# MAGIC FROM teste.abt_inference_me_br
# MAGIC UNION ALL
# MAGIC SELECT 'portfolio_abt_group_me_br', COUNT(*),
# MAGIC   CAST(MIN(reference_month) AS STRING), CAST(MAX(reference_month) AS STRING)
# MAGIC FROM teste.portfolio_abt_group_me_br
# MAGIC UNION ALL
# MAGIC SELECT 'apply_model_me_br', COUNT(*),
# MAGIC   CAST(MIN(reference_month) AS STRING), CAST(MAX(reference_month) AS STRING)
# MAGIC FROM teste.apply_model_me_br
# MAGIC UNION ALL
# MAGIC SELECT 'targets_me_br', COUNT(*),
# MAGIC   CAST(MIN(reference_month) AS STRING), CAST(MAX(reference_month) AS STRING)
# MAGIC FROM teste.targets_me_br

# COMMAND ----------

# DBTITLE 1,Source view: join apply_model + abt + portfolio + targets
# MAGIC %sql
# MAGIC -- apply_model (m) base; abt e portfolio em m-1; targets em m.
# MAGIC CREATE OR REPLACE TEMP VIEW monitoring_source AS
# MAGIC SELECT
# MAGIC   am.id_customer, am.customer_name, am.country, am.reference_month,
# MAGIC   add_months(am.reference_month, -1) AS feature_reference_month,
# MAGIC   inf.total_amount, inf.overdue_amount, inf.overdue_pct,
# MAGIC   inf.months_with_billing, inf.months_defaulted,
# MAGIC   inf.pct_months_overdue_10_20, inf.pct_months_overdue_20_30,
# MAGIC   inf.pct_months_overdue_30_50, inf.pct_months_overdue_50_plus,
# MAGIC   inf.flag_transacted, inf.score, inf.historical_weight,
# MAGIC   COALESCE(inf.reference_value, 0)     AS reference_value,
# MAGIC   COALESCE(inf.reference_value_clp, 0) AS reference_value_clp,
# MAGIC   COALESCE(inf.payment_term, 1.0)      AS payment_term,
# MAGIC   pag.median_cluster_10, pag.median_cluster_20,
# MAGIC   pag.median_cluster_30, pag.median_cluster_50, pag.portfolio_score,
# MAGIC   am.adjusted_score, am.score_band,
# MAGIC   am.integrated_score, am.integrated_score_band,
# MAGIC   am.credit_limit, am.credit_limit_clp,
# MAGIC   am.credit_limit_end, am.credit_limit_end_clp,
# MAGIC   t.percent7mob1  AS target_percent7mob1,
# MAGIC   t.percent7mob3  AS target_percent7mob3,
# MAGIC   t.percent7mob6  AS target_percent7mob6,
# MAGIC   t.percent7mob9  AS target_percent7mob9,
# MAGIC   t.percent7mob12 AS target_percent7mob12,
# MAGIC   t.billed_1m  AS target_billed_1m,  t.overdue_1m  AS target_overdue_1m,  t.overdue_pct_1m  AS target_overdue_pct_1m,
# MAGIC   t.billed_3m  AS target_billed_3m,  t.overdue_3m  AS target_overdue_3m,  t.overdue_pct_3m  AS target_overdue_pct_3m,
# MAGIC   t.billed_6m  AS target_billed_6m,  t.overdue_6m  AS target_overdue_6m,  t.overdue_pct_6m  AS target_overdue_pct_6m,
# MAGIC   t.billed_12m AS target_billed_12m, t.overdue_12m AS target_overdue_12m, t.overdue_pct_12m AS target_overdue_pct_12m,
# MAGIC   current_timestamp() AS updated_at
# MAGIC FROM teste.apply_model_me_br AS am
# MAGIC LEFT JOIN teste.abt_inference_me_br AS inf
# MAGIC   ON inf.id_customer = am.id_customer
# MAGIC  AND inf.reference_month = add_months(am.reference_month, -1)
# MAGIC LEFT JOIN teste.portfolio_abt_group_me_br AS pag
# MAGIC   ON pag.reference_month = add_months(am.reference_month, -1)
# MAGIC LEFT JOIN teste.targets_me_br AS t
# MAGIC   ON t.id_customer = am.id_customer
# MAGIC  AND t.reference_month = am.reference_month

# COMMAND ----------

# DBTITLE 1,MERGE monitoring_me_br: insere novos + atualiza targets
# MAGIC %sql
# MAGIC MERGE INTO teste.monitoring_me_br AS target
# MAGIC USING monitoring_source AS source
# MAGIC ON target.reference_month = source.reference_month
# MAGIC    AND target.id_customer = source.id_customer
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.integrated_score        = source.integrated_score,
# MAGIC   target.integrated_score_band   = source.integrated_score_band,
# MAGIC   target.credit_limit            = source.credit_limit,
# MAGIC   target.credit_limit_clp        = source.credit_limit_clp,
# MAGIC   target.credit_limit_end        = source.credit_limit_end,
# MAGIC   target.credit_limit_end_clp    = source.credit_limit_end_clp,
# MAGIC   target.target_percent7mob1     = source.target_percent7mob1,
# MAGIC   target.target_percent7mob3     = source.target_percent7mob3,
# MAGIC   target.target_percent7mob6     = source.target_percent7mob6,
# MAGIC   target.target_percent7mob9     = source.target_percent7mob9,
# MAGIC   target.target_percent7mob12    = source.target_percent7mob12,
# MAGIC   target.target_billed_1m        = source.target_billed_1m,
# MAGIC   target.target_overdue_1m       = source.target_overdue_1m,
# MAGIC   target.target_overdue_pct_1m   = source.target_overdue_pct_1m,
# MAGIC   target.target_billed_3m        = source.target_billed_3m,
# MAGIC   target.target_overdue_3m       = source.target_overdue_3m,
# MAGIC   target.target_overdue_pct_3m   = source.target_overdue_pct_3m,
# MAGIC   target.target_billed_6m        = source.target_billed_6m,
# MAGIC   target.target_overdue_6m       = source.target_overdue_6m,
# MAGIC   target.target_overdue_pct_6m   = source.target_overdue_pct_6m,
# MAGIC   target.target_billed_12m       = source.target_billed_12m,
# MAGIC   target.target_overdue_12m      = source.target_overdue_12m,
# MAGIC   target.target_overdue_pct_12m  = source.target_overdue_pct_12m,
# MAGIC   target.updated_at              = source.updated_at
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Sanity check: monitoring_me_br
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br) AS total_linhas,
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM teste.monitoring_me_br) AS total_safras,
# MAGIC   (SELECT CAST(MAX(reference_month) AS STRING) FROM teste.monitoring_me_br) AS safra_max,
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC      SELECT reference_month, id_customer, COUNT(*) c
# MAGIC      FROM teste.monitoring_me_br GROUP BY reference_month, id_customer HAVING c > 1
# MAGIC    )) AS duplicatas,
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br WHERE adjusted_score IS NULL) AS sem_adjusted_score

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabelas de monitoramento (estrutura Excel) — historico preservado
# MAGIC
# MAGIC Todas as 16 tabelas seguem o esquema tidy do Excel. `period` = `'Train'`
# MAGIC (base In-Time, `reference_month < train_cutoff`) ou `'YYYY/MM'` (cada
# MAGIC safra OOT). Baseline de PSI/diff = distribuicao do `'Train'`.

# COMMAND ----------

# DBTITLE 1,DDL: criar as 16 tabelas de monitoramento (IF NOT EXISTS)
spark.sql("""
CREATE TABLE IF NOT EXISTS teste.performance_me_br (
  performance STRING, period STRING, value DOUBLE, market_name STRING,
  metric_key STRING, reference_year INT, reference_month INT, updated_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
""")

# Tabelas por banda (decil = banda 1/2/3)
for tname in [
    "dist_por_decil_me_br", "dist_por_decil_diff_pp_me_br",
    "perc_bad_decil_me_br", "perc_bad_decil_diff_pp_me_br",
    "df_dist_bad_decil_me_br", "df_dist_bad_decil_diff_pp_me_br",
    "iep_por_decil_me_br",
]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS teste.{tname} (
      decil INT, period STRING, value DOUBLE, market_name STRING,
      metric_key STRING, reference_year INT, reference_month INT, updated_at TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
    """)

spark.sql("""
CREATE TABLE IF NOT EXISTS teste.decile_migrations_me_br (
  previous_decile INT, current_decile INT, count INT, period STRING,
  market_name STRING, metric_key STRING, reference_year INT,
  reference_month INT, updated_at TIMESTAMP, update_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS teste.iep_me_br (
  variaveis STRING, period STRING, value DOUBLE, market_name STRING,
  metric_key STRING, reference_year INT, reference_month INT, updated_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
""")

# Tabelas por variavel + grupo (sem coeficiente), value DOUBLE
for tname in ["iep_vars_me_br", "risco_relativo_me_br"]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS teste.{tname} (
      variaveis STRING, grupo STRING, period STRING, value DOUBLE,
      market_name STRING, metric_key STRING, reference_year INT,
      reference_month INT, updated_at TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
    """)

# risco_relativo_trigger: value e STRING ('true'/'false'), igual ao padrao Excel
spark.sql("""
CREATE TABLE IF NOT EXISTS teste.risco_relativo_trigger_me_br (
  variaveis STRING, grupo STRING, period STRING, value STRING,
  market_name STRING, metric_key STRING, reference_year INT,
  reference_month INT, updated_at TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
""")

# Tabelas por variavel + grupo + coeficiente
for tname in ["dist_vars_me_br", "dist_vars_diff_pp_me_br", "vol_vars_me_br"]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS teste.{tname} (
      variaveis STRING, grupo STRING, coeficientes DOUBLE, period STRING,
      value DOUBLE, market_name STRING, metric_key STRING,
      reference_year INT, reference_month INT, updated_at TIMESTAMP
    ) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite'='true')
    """)

print("16 tabelas de monitoramento garantidas (IF NOT EXISTS).")

# COMMAND ----------

# DBTITLE 1,Carga e configuracao
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import roc_auc_score
from scipy.stats import ks_2samp
from pyspark.sql import functions as F

# ---- Configuracao (alinhada com bivariate_analysis_me_br) ----
SCORE_COL = "integrated_score"
BAND_COL = "integrated_score_band"
TARGET_COL = "target_percent7mob1"
BAND_MAP = {"1-BAIXO": 1, "2-MEDIO": 2, "3-ALTO": 3}

# Features que compoem a formula ponderada (equivalente aos "dummies").
# grupo: agrupamento de negocio. coef: peso efetivo no score quando aplicavel.
FEATURES = [
    ("pct_months_overdue_10_20", "severidade_atraso"),
    ("pct_months_overdue_20_30", "severidade_atraso"),
    ("pct_months_overdue_30_50", "severidade_atraso"),
    ("pct_months_overdue_50_plus", "severidade_atraso"),
    ("overdue_pct",              "severidade_atraso"),
    ("months_with_billing",      "peso_historico"),
    ("months_defaulted",         "peso_historico"),
    ("historical_weight",        "peso_historico"),
    ("reference_value",          "exposicao"),
    ("payment_term",             "exposicao"),
    ("portfolio_score",          "benchmark"),
    ("score",                    "score_individual"),
]

cutoff = spark.sql("SELECT train_cutoff FROM config_pipeline").collect()[0][0]
TRAIN_CUTOFF = pd.Timestamp(cutoff)

pdf = spark.sql(f"""
    SELECT id_customer, reference_month, {SCORE_COL}, {BAND_COL},
           {TARGET_COL}, total_amount, overdue_amount,
           pct_months_overdue_10_20, pct_months_overdue_20_30,
           pct_months_overdue_30_50, pct_months_overdue_50_plus,
           overdue_pct, months_with_billing, months_defaulted,
           historical_weight, reference_value, payment_term,
           portfolio_score, score,
           median_cluster_10, median_cluster_20,
           median_cluster_30, median_cluster_50
    FROM teste.monitoring_me_br
    WHERE {SCORE_COL} IS NOT NULL
""").toPandas()

pdf["reference_month"] = pd.to_datetime(pdf["reference_month"])
pdf["band_num"] = pdf[BAND_COL].map(BAND_MAP).fillna(-1).astype(int)
pdf["period"] = np.where(
    pdf["reference_month"] < TRAIN_CUTOFF,
    "Train",
    pdf["reference_month"].dt.strftime("%Y/%m"),
)

# Coeficiente efetivo: media da mediana do cluster correspondente (peso real
# da faixa no score = Σ(pct×mediana)/10000). Para as demais features, NULL.
COEF_MAP = {
    "pct_months_overdue_10_20": float(pdf["median_cluster_10"].mean()) if pdf["median_cluster_10"].notna().any() else None,
    "pct_months_overdue_20_30": float(pdf["median_cluster_20"].mean()) if pdf["median_cluster_20"].notna().any() else None,
    "pct_months_overdue_30_50": float(pdf["median_cluster_30"].mean()) if pdf["median_cluster_30"].notna().any() else None,
    "pct_months_overdue_50_plus": float(pdf["median_cluster_50"].mean()) if pdf["median_cluster_50"].notna().any() else None,
}

train_pdf = pdf[pdf["period"] == "Train"].copy()
periods = ["Train"] + sorted([p for p in pdf["period"].unique() if p != "Train"])
NOW = datetime.now()

print(f"Registros: {len(pdf):,} | Train: {len(train_pdf):,} | Periodos OOT: {len(periods)-1}")
if len(train_pdf) == 0:
    raise ValueError(
        f"Sem dados Train (reference_month < {TRAIN_CUTOFF.date()}). "
        f"Ajuste o widget train_cutoff."
    )
print(f"Range: {pdf['reference_month'].min().date()} a {pdf['reference_month'].max().date()}")

# COMMAND ----------

# DBTITLE 1,Funcoes auxiliares (binning, PSI, lift)
def fit_bins(series, max_bins=5):
    """Cortes por quantil aprendidos no Train. Categoricos/poucos valores
    viram bins discretos. Retorna lista de edges ou None p/ categorico."""
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.nunique() <= 6:
        return None  # tratar como categorico
    qs = np.unique(np.quantile(s, np.linspace(0, 1, max_bins + 1)))
    qs[0], qs[-1] = -np.inf, np.inf
    return list(qs)


def apply_bins(series, edges):
    """Aplica edges; NaN -> 'Missing'. Categorico -> valor como string."""
    s = pd.to_numeric(series, errors="coerce")
    if edges is None:
        out = series.where(series.notna(), "Missing").astype(str)
        return out
    lbl = pd.cut(s, bins=edges, include_lowest=True, duplicates="drop").astype(str)
    lbl = pd.Series(lbl, index=series.index)
    lbl[s.isna()] = "Missing"
    return lbl


def psi(base_counts, cur_counts, eps=1e-4):
    """PSI entre duas distribuicoes (dict label->share)."""
    keys = set(base_counts) | set(cur_counts)
    val = 0.0
    for k in keys:
        b = base_counts.get(k, 0.0) + eps
        c = cur_counts.get(k, 0.0) + eps
        val += (c - b) * np.log(c / b)
    return round(float(val), 6)


def norm_counts(s):
    vc = s.value_counts(normalize=True)
    return {str(k): float(v) for k, v in vc.items()}


# Cortes aprendidos no Train (mesmos cortes em todos os periodos)
BIN_EDGES = {f: fit_bins(train_pdf[f]) for f, _ in FEATURES}
TRAIN_FEAT_DIST = {}
for f, _ in FEATURES:
    TRAIN_FEAT_DIST[f] = norm_counts(apply_bins(train_pdf[f], BIN_EDGES[f]))

TRAIN_BAND_DIST = norm_counts(train_pdf["band_num"].astype(str))

# Bad rate baseline (Train) por banda e geral
_t = train_pdf[train_pdf[TARGET_COL].notna()]
TRAIN_BAD_OVERALL = float(_t[TARGET_COL].mean()) if len(_t) else None
TRAIN_BAD_BY_BAND = (
    _t.groupby("band_num")[TARGET_COL].mean().to_dict() if len(_t) else {}
)

# COMMAND ----------

# DBTITLE 1,Calcular metricas por periodo
rows = {k: [] for k in [
    "performance", "dist_por_decil", "dist_por_decil_diff_pp",
    "perc_bad_decil", "perc_bad_decil_diff_pp",
    "df_dist_bad_decil", "df_dist_bad_decil_diff_pp",
    "decile_migrations", "iep_por_decil", "iep", "iep_vars",
    "dist_vars", "dist_vars_diff_pp", "vol_vars",
    "risco_relativo", "risco_relativo_trigger",
]}


def ry_rm(period, sub):
    """reference_year / reference_month. Train herda a ultima safra In-Time."""
    if period == "Train":
        m = sub["reference_month"].max()
    else:
        m = pd.Timestamp(period.replace("/", "-") + "-01")
    return int(m.year), int(m.month)


# Baseline de bad rate por banda no Train (% de bads concentrado por banda)
def bad_share_by_band(sub):
    st = sub[sub[TARGET_COL].notna()]
    tot_bad = st[TARGET_COL].sum()
    if tot_bad == 0:
        return {}
    return (st.groupby("band_num")[TARGET_COL].sum() / tot_bad).to_dict()


TRAIN_BADSHARE = bad_share_by_band(train_pdf)

prev_sub = None
for period in periods:
    sub = pdf[pdf["period"] == period].copy()
    if len(sub) == 0:
        continue
    ry, rm = ry_rm(period, sub)
    sub_t = sub[sub[TARGET_COL].notna()].copy()

    # ---------- performance: ROC, KS, Gini ----------
    if len(sub_t) and sub_t[TARGET_COL].nunique() == 2:
        y = sub_t[TARGET_COL].astype(int).values
        sc = sub_t[SCORE_COL].astype(float).values
        auc = roc_auc_score(y, sc)
        ks = ks_2samp(sc[y == 0], sc[y == 1]).statistic
        for pm, pv in [("ROC", auc), ("KS", ks), ("Gini", 2 * auc - 1)]:
            rows["performance"].append(
                (pm, period, round(float(pv), 6), MARKET_NAME,
                 "performance", ry, rm, NOW)
            )

    # ---------- distribuicao por banda + diff pp + PSI ----------
    band_dist = norm_counts(sub["band_num"].astype(str))
    for b in sorted(set(list(band_dist) + list(TRAIN_BAND_DIST))):
        bi = int(float(b))
        v = band_dist.get(b, 0.0) * 100
        rows["dist_por_decil"].append(
            (bi, period, round(v, 6), MARKET_NAME, "dist_por_decil", ry, rm, NOW))
        diff = v - TRAIN_BAND_DIST.get(b, 0.0) * 100
        rows["dist_por_decil_diff_pp"].append(
            (bi, period, round(diff, 6), MARKET_NAME,
             "dist_por_decil_diff_pp", ry, rm, NOW))
    # Per-band PSI contribution + total (decil=100), mirroring estrutura Excel.
    _eps = 1e-4
    _keys_all = sorted(set(TRAIN_BAND_DIST) | set(band_dist))
    _total_psi = 0.0
    for _k in _keys_all:
        _bi = int(float(_k))
        _t_share = TRAIN_BAND_DIST.get(_k, 0.0) + _eps
        _c_share = band_dist.get(_k, 0.0) + _eps
        _contrib = (_c_share - _t_share) * np.log(_c_share / _t_share)
        _total_psi += _contrib
        rows["iep_por_decil"].append(
            (_bi, period, round(float(_contrib), 6), MARKET_NAME,
             "iep_por_decil", ry, rm, NOW))
    rows["iep_por_decil"].append(
        (100, period, round(_total_psi, 6), MARKET_NAME,
         "iep_por_decil", ry, rm, NOW))

    # ---------- bad rate por banda + diff pp ----------
    if len(sub_t):
        br = sub_t.groupby("band_num")[TARGET_COL].mean().to_dict()
        for bi, val in br.items():
            rows["perc_bad_decil"].append(
                (int(bi), period, round(float(val) * 100, 6), MARKET_NAME,
                 "perc_bad_decil", ry, rm, NOW))
            dpp = (float(val) - TRAIN_BAD_BY_BAND.get(bi, 0.0)) * 100
            rows["perc_bad_decil_diff_pp"].append(
                (int(bi), period, round(dpp, 6), MARKET_NAME,
                 "perc_bad_decil_diff_pp", ry, rm, NOW))
        # ---------- % dos bads concentrado por banda + diff ----------
        bs = bad_share_by_band(sub)
        for bi, val in bs.items():
            rows["df_dist_bad_decil"].append(
                (int(bi), period, round(float(val) * 100, 6), MARKET_NAME,
                 "df_dist_bad_decil", ry, rm, NOW))
            dpp = (float(val) - TRAIN_BADSHARE.get(bi, 0.0)) * 100
            rows["df_dist_bad_decil_diff_pp"].append(
                (int(bi), period, round(dpp, 6), MARKET_NAME,
                 "df_dist_bad_decil_diff_pp", ry, rm, NOW))

    # ---------- migracao de banda (periodo anterior -> atual) ----------
    if prev_sub is not None:
        m = prev_sub[["id_customer", "band_num"]].rename(
            columns={"band_num": "prev"}
        ).merge(
            sub[["id_customer", "band_num"]].rename(columns={"band_num": "cur"}),
            on="id_customer", how="right",
        )
        m["prev"] = m["prev"].fillna(-1).astype(int)
        g = m.groupby(["prev", "cur"]).size().reset_index(name="n")
        for _, r in g.iterrows():
            rows["decile_migrations"].append(
                (int(r["prev"]), int(r["cur"]), int(r["n"]), period,
                 MARKET_NAME, "decile_migrations", ry, rm, NOW, NOW))

    # ---------- features: dist, vol, PSI, risco relativo ----------
    for f, grupo in FEATURES:
        coef = COEF_MAP.get(f)
        binned = apply_bins(sub[f], BIN_EDGES[f])
        dist = norm_counts(binned)
        vol = binned.value_counts().to_dict()

        # PSI da feature (iep / iep_vars)
        p = psi(TRAIN_FEAT_DIST[f], dist)
        rows["iep"].append(
            (f, period, p, MARKET_NAME, "iep", ry, rm, NOW))
        rows["iep_vars"].append(
            (f, grupo, period, p, MARKET_NAME, "iep_vars", ry, rm, NOW))

        for lbl in sorted(set(list(dist) + list(TRAIN_FEAT_DIST[f]))):
            share = dist.get(lbl, 0.0) * 100
            rows["dist_vars"].append(
                (f"{f}={lbl}", grupo, coef, period, round(share, 6),
                 MARKET_NAME, "dist_vars", ry, rm, NOW))
            dpp = share - TRAIN_FEAT_DIST[f].get(lbl, 0.0) * 100
            rows["dist_vars_diff_pp"].append(
                (f"{f}={lbl}", grupo, coef, period, round(dpp, 6),
                 MARKET_NAME, "dist_vars_diff_pp", ry, rm, NOW))
            rows["vol_vars"].append(
                (f"{f}={lbl}", grupo, coef, period,
                 float(vol.get(lbl, 0)), MARKET_NAME, "vol_vars", ry, rm, NOW))

        # Risco relativo (lift) = bad_rate(faixa) / bad_rate(geral)
        if len(sub_t) and TRAIN_BAD_OVERALL and TRAIN_BAD_OVERALL > 0:
            bf = apply_bins(sub_t[f], BIN_EDGES[f])
            base = sub_t[TARGET_COL].mean()
            if base and base > 0:
                grp = sub_t.assign(_b=bf).groupby("_b")[TARGET_COL].mean()
                for lbl, brate in grp.items():
                    rr = float(brate) / float(base)
                    rows["risco_relativo"].append(
                        (f"{f}={lbl}", grupo, period, round(rr, 6),
                         MARKET_NAME, "risco_relativo", ry, rm, NOW))
                    # trigger: lift fora de [0.5, 2.0] sinaliza instabilidade
                    trig = "true" if (rr < 0.5 or rr > 2.0) else "false"
                    rows["risco_relativo_trigger"].append(
                        (f"{f}={lbl}", grupo, period, trig,
                         MARKET_NAME, "risco_relativo_trigger", ry, rm, NOW))

    prev_sub = sub

for k, v in rows.items():
    print(f"  {k}: {len(v)} linhas")

# COMMAND ----------

# DBTITLE 1,MERGE de cada tabela (idempotente, historico preservado)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

SCHEMAS = {
    "performance": (["performance", "period", "value", "market_name",
                     "metric_key", "reference_year", "reference_month", "updated_at"],
                    ["s", "s", "d", "s", "s", "i", "i", "t"],
                    ["performance", "period", "reference_year", "reference_month"]),
    "decile_migrations": (["previous_decile", "current_decile", "count", "period",
                           "market_name", "metric_key", "reference_year",
                           "reference_month", "updated_at", "update_at"],
                          ["i", "i", "i", "s", "s", "s", "i", "i", "t", "t"],
                          ["previous_decile", "current_decile", "period",
                           "reference_year", "reference_month"]),
    "iep": (["variaveis", "period", "value", "market_name", "metric_key",
             "reference_year", "reference_month", "updated_at"],
            ["s", "s", "d", "s", "s", "i", "i", "t"],
            ["variaveis", "period", "reference_year", "reference_month"]),
}
# tabelas por banda
for t in ["dist_por_decil", "dist_por_decil_diff_pp", "perc_bad_decil",
          "perc_bad_decil_diff_pp", "df_dist_bad_decil",
          "df_dist_bad_decil_diff_pp", "iep_por_decil"]:
    SCHEMAS[t] = (["decil", "period", "value", "market_name", "metric_key",
                   "reference_year", "reference_month", "updated_at"],
                  ["i", "s", "d", "s", "s", "i", "i", "t"],
                  ["decil", "period", "reference_year", "reference_month"])
# tabelas var+grupo (value DOUBLE)
for t in ["iep_vars", "risco_relativo"]:
    SCHEMAS[t] = (["variaveis", "grupo", "period", "value", "market_name",
                   "metric_key", "reference_year", "reference_month", "updated_at"],
                  ["s", "s", "s", "d", "s", "s", "i", "i", "t"],
                  ["variaveis", "grupo", "period", "reference_year", "reference_month"])
# risco_relativo_trigger: value STRING ('true'/'false')
SCHEMAS["risco_relativo_trigger"] = (
    ["variaveis", "grupo", "period", "value", "market_name",
     "metric_key", "reference_year", "reference_month", "updated_at"],
    ["s", "s", "s", "s", "s", "s", "i", "i", "t"],
    ["variaveis", "grupo", "period", "reference_year", "reference_month"]
)
# tabelas var+grupo+coef
for t in ["dist_vars", "dist_vars_diff_pp", "vol_vars"]:
    SCHEMAS[t] = (["variaveis", "grupo", "coeficientes", "period", "value",
                   "market_name", "metric_key", "reference_year",
                   "reference_month", "updated_at"],
                  ["s", "s", "d", "s", "d", "s", "s", "i", "i", "t"],
                  ["variaveis", "grupo", "period", "reference_year", "reference_month"])

_TYPE = {"s": StringType(), "d": DoubleType(), "i": IntegerType(), "t": TimestampType()}


def merge_table(metric, data):
    cols, types, keys = SCHEMAS[metric]
    tname = f"teste.{metric}_me_br"
    if not data:
        print(f"  {tname}: 0 linhas, skip")
        return
    schema = StructType([StructField(c, _TYPE[t], True) for c, t in zip(cols, types)])
    sdf = spark.createDataFrame(data, schema=schema)
    sdf.createOrReplaceTempView("_src")
    on = " AND ".join([f"tgt.{k} = src.{k}" for k in keys])
    upd = ", ".join([f"tgt.{c} = src.{c}" for c in cols if c not in keys])
    spark.sql(f"""
        MERGE INTO {tname} AS tgt USING _src AS src ON {on}
        WHEN MATCHED THEN UPDATE SET {upd}
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  {tname}: {len(data)} linhas mergeadas")


for metric, data in rows.items():
    merge_table(metric, data)

print("\nMonitoramento gravado. Historico preservado (MERGE, sem DROP).")

# COMMAND ----------

# DBTITLE 1,Sanity checks: tabelas de monitoramento
# MAGIC %sql
# MAGIC SELECT 'performance_me_br' AS tabela, COUNT(*) AS linhas,
# MAGIC   COUNT(DISTINCT period) AS periodos FROM teste.performance_me_br
# MAGIC UNION ALL SELECT 'dist_por_decil_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.dist_por_decil_me_br
# MAGIC UNION ALL SELECT 'perc_bad_decil_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.perc_bad_decil_me_br
# MAGIC UNION ALL SELECT 'df_dist_bad_decil_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.df_dist_bad_decil_me_br
# MAGIC UNION ALL SELECT 'decile_migrations_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.decile_migrations_me_br
# MAGIC UNION ALL SELECT 'iep_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.iep_me_br
# MAGIC UNION ALL SELECT 'iep_por_decil_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.iep_por_decil_me_br
# MAGIC UNION ALL SELECT 'iep_vars_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.iep_vars_me_br
# MAGIC UNION ALL SELECT 'dist_vars_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.dist_vars_me_br
# MAGIC UNION ALL SELECT 'vol_vars_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.vol_vars_me_br
# MAGIC UNION ALL SELECT 'risco_relativo_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.risco_relativo_me_br
# MAGIC UNION ALL SELECT 'risco_relativo_trigger_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.risco_relativo_trigger_me_br
# MAGIC UNION ALL SELECT 'dist_por_decil_diff_pp_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.dist_por_decil_diff_pp_me_br
# MAGIC UNION ALL SELECT 'perc_bad_decil_diff_pp_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.perc_bad_decil_diff_pp_me_br
# MAGIC UNION ALL SELECT 'df_dist_bad_decil_diff_pp_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.df_dist_bad_decil_diff_pp_me_br
# MAGIC UNION ALL SELECT 'dist_vars_diff_pp_me_br', COUNT(*), COUNT(DISTINCT period) FROM teste.dist_vars_diff_pp_me_br

# COMMAND ----------

# DBTITLE 1,Inspecao: performance e PSI da safra mais recente
# MAGIC %sql
# MAGIC SELECT performance, period, ROUND(value, 4) AS value
# MAGIC FROM teste.performance_me_br
# MAGIC WHERE period <> 'Train'
# MAGIC ORDER BY reference_year DESC, reference_month DESC, performance
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PSI por feature na ultima safra (>0.25 = mudanca significativa)
# MAGIC SELECT variaveis, period, ROUND(value, 4) AS psi,
# MAGIC   CASE WHEN value < 0.10 THEN 'Estavel'
# MAGIC        WHEN value < 0.25 THEN 'Moderado'
# MAGIC        ELSE 'Significativo' END AS classificacao
# MAGIC FROM teste.iep_me_br
# MAGIC WHERE period = (SELECT MAX(period) FROM teste.iep_me_br WHERE period <> 'Train')
# MAGIC ORDER BY value DESC
