# Databricks notebook source

# MAGIC %md
# MAGIC ## Pipeline: Monitoramento ME BR — v2 (consolidado)
# MAGIC
# MAGIC Este notebook gera **5 tabelas** que substituem as 16 tabelas Excel do padrão antigo + as 2 tabelas wide do v1.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tabelas produzidas
# MAGIC
# MAGIC | Tabela | Granularidade | Comportamento | Substitui |
# MAGIC |---|---|---|---|
# MAGIC | `monitoring_me_br` | cliente × safra | MISTO (INSERT + UPDATE seletivo) | — (detalhe) |
# MAGIC | `monitoring_metrics_me_br` | 1 linha por safra (wide) | OVERWRITE | — (resumo) |
# MAGIC | `monitoring_band_me_br` | safra × banda | OVERWRITE | dist_por_decil, dist_por_decil_diff_pp, perc_bad_decil, perc_bad_decil_diff_pp, df_dist_bad_decil, df_dist_bad_decil_diff_pp, iep_por_decil |
# MAGIC | `monitoring_features_me_br` | safra × feature × bin | OVERWRITE | dist_vars, dist_vars_diff_pp, vol_vars, iep, iep_vars, risco_relativo, risco_relativo_trigger |
# MAGIC | `monitoring_band_migrations_me_br` | safra × prev_band × curr_band | OVERWRITE | decile_migrations |
# MAGIC
# MAGIC **Resultado: 16 tabelas Excel → 3 tabelas tidy (+ 2 já existentes).**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Mapeamento dos KPIs (todos preservados)
# MAGIC
# MAGIC **`monitoring_band_me_br`** (1 linha por banda):
# MAGIC - `pct_pop` — % população na banda (= dist_por_decil)
# MAGIC - `pct_pop_diff_pp` — diferença pp vs Train (= dist_por_decil_diff_pp)
# MAGIC - `bad_rate` — % de bads na banda (= perc_bad_decil)
# MAGIC - `bad_rate_diff_pp` — diferença pp vs Train (= perc_bad_decil_diff_pp)
# MAGIC - `pct_bad_share` — % dos bads concentrado na banda (= df_dist_bad_decil)
# MAGIC - `pct_bad_share_diff_pp` — diferença pp vs Train (= df_dist_bad_decil_diff_pp)
# MAGIC - `psi_contribution` — contribuição da banda ao PSI total (= iep_por_decil)
# MAGIC
# MAGIC **`monitoring_features_me_br`** (1 linha por feature × bin):
# MAGIC - `pct_pop` — % da população na faixa da feature (= dist_vars)
# MAGIC - `pct_pop_diff_pp` — diferença pp vs Train (= dist_vars_diff_pp)
# MAGIC - `volume` — contagem absoluta na faixa (= vol_vars)
# MAGIC - `lift` — bad_rate(faixa) / bad_rate(geral) (= risco_relativo)
# MAGIC - `lift_trigger` — 'true'/'false' se lift fora [0.5, 2.0] (= risco_relativo_trigger)
# MAGIC - `psi_feature` — PSI da feature inteira (= iep)
# MAGIC - `psi_within_group` — PSI da feature dentro do grupo (= iep_vars)
# MAGIC
# MAGIC **`monitoring_band_migrations_me_br`** (1 linha por par de bandas):
# MAGIC - `count`, `pct_of_pop` — contagem e % de clientes na transição (= decile_migrations)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Configuração
# MAGIC
# MAGIC - **Score**: `integrated_score`
# MAGIC - **Banda**: `integrated_score_band` (1-BAIXO, 2-MEDIO, 3-ALTO)
# MAGIC - **Target**: `target_percent7mob1`
# MAGIC - **Train cutoff**: `2024-01-01` (configurável via widget)
# MAGIC - **Período**: 'Train' (todas as safras pré-cutoff agregadas) ou 'YYYY/MM' (cada safra OOT individualmente)
# MAGIC - **PSI baseline**: distribuição do período 'Train'
# MAGIC - **diff_pp baseline**: período 'Train'
# MAGIC
# MAGIC ### Pré-requisitos
# MAGIC NB1, NB1b, NB2, NB2b, NB3 executados (alimentam abt_inference, apply_model, targets, portfolio_abt_group).

# COMMAND ----------

# Widgets de configuracao
from datetime import date

dbutils.widgets.text("data_referencia", "", "Data de Referencia (yyyy-mm-dd)")
dbutils.widgets.text("train_cutoff",   "2024-01-01", "Train cutoff (yyyy-mm-dd)")

data_referencia = dbutils.widgets.get("data_referencia") or str(date.today())
train_cutoff    = dbutils.widgets.get("train_cutoff") or "2024-01-01"

print(f"Data de referencia: {data_referencia}")
print(f"Train cutoff      : {train_cutoff}  (safras < cutoff = 'Train', >= cutoff = 'YYYY/MM')")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabela 1: detalhe cliente x safra. Sem DROP: historico preservado.
# MAGIC CREATE TABLE IF NOT EXISTS ds_catalog_dev.credit_engine.monitoring_me_br (
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

# MAGIC %sql
# MAGIC -- View source: apply_model (m) + abt (m-1) + portfolio (m-1) + targets (m)
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
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br AS am
# MAGIC LEFT JOIN ds_catalog_dev.credit_engine.abt_inference_me_br AS inf
# MAGIC   ON inf.id_customer = am.id_customer
# MAGIC  AND inf.reference_month = add_months(am.reference_month, -1)
# MAGIC LEFT JOIN ds_catalog_dev.credit_engine.portfolio_abt_group_me_br AS pag
# MAGIC   ON pag.reference_month = add_months(am.reference_month, -1)
# MAGIC LEFT JOIN ds_catalog_dev.credit_engine.targets_me_br AS t
# MAGIC   ON t.id_customer = am.id_customer
# MAGIC  AND t.reference_month = am.reference_month

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.credit_engine.monitoring_me_br AS target
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

# MAGIC %md
# MAGIC ## DDL das 4 tabelas de métricas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabela 2: metricas agregadas por safra (1 linha por reference_month)
# MAGIC DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_metrics_me_br;
# MAGIC CREATE TABLE ds_catalog_dev.credit_engine.monitoring_metrics_me_br (
# MAGIC   reference_month                DATE,
# MAGIC   period                         STRING,
# MAGIC   total_clients                  INT,
# MAGIC   pop_baixo                      INT,
# MAGIC   pop_medio                      INT,
# MAGIC   pop_alto                       INT,
# MAGIC   pct_baixo                      DOUBLE,
# MAGIC   pct_medio                      DOUBLE,
# MAGIC   pct_alto                       DOUBLE,
# MAGIC   avg_score                      DOUBLE,
# MAGIC   median_score                   DOUBLE,
# MAGIC   avg_credit_limit               DOUBLE,
# MAGIC   median_credit_limit            DOUBLE,
# MAGIC   total_credit_limit             DOUBLE,
# MAGIC   avg_credit_limit_clp           DOUBLE,
# MAGIC   median_credit_limit_clp        DOUBLE,
# MAGIC   total_credit_limit_clp         DOUBLE,
# MAGIC   total_billed_usd               DOUBLE,
# MAGIC   overdue_usd                    DOUBLE,
# MAGIC   overdue_pct                    DOUBLE,
# MAGIC   bad_rate_overall               DOUBLE,
# MAGIC   bad_rate_baixo                 DOUBLE,
# MAGIC   bad_rate_medio                 DOUBLE,
# MAGIC   bad_rate_alto                  DOUBLE,
# MAGIC   lift_baixo                     DOUBLE,
# MAGIC   lift_medio                     DOUBLE,
# MAGIC   lift_alto                      DOUBLE,
# MAGIC   ks                             DOUBLE,
# MAGIC   roc_auc                        DOUBLE,
# MAGIC   gini                           DOUBLE,
# MAGIC   psi_vs_train                   DOUBLE,
# MAGIC   psi_vs_train_classification    STRING,
# MAGIC   psi_rolling                    DOUBLE,
# MAGIC   psi_rolling_classification     STRING,
# MAGIC   pct_improved                   DOUBLE,
# MAGIC   pct_maintained                 DOUBLE,
# MAGIC   pct_worsened                   DOUBLE,
# MAGIC   market_name                    STRING,
# MAGIC   metric_key                     STRING,
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabela 3: metricas por banda (long format) — substitui 7 tabelas Excel
# MAGIC DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_band_me_br;
# MAGIC CREATE TABLE ds_catalog_dev.credit_engine.monitoring_band_me_br (
# MAGIC   reference_month                DATE,
# MAGIC   period                         STRING,           -- 'Train' ou 'YYYY/MM'
# MAGIC   band                           INT,              -- 1, 2, 3, -1 (missing), 100 (total)
# MAGIC   band_name                      STRING,           -- '1-BAIXO', '2-MEDIO', '3-ALTO', 'Missing', 'TOTAL'
# MAGIC   pct_pop                        DOUBLE,           -- = dist_por_decil
# MAGIC   pct_pop_diff_pp                DOUBLE,           -- = dist_por_decil_diff_pp
# MAGIC   bad_rate                       DOUBLE,           -- = perc_bad_decil (%)
# MAGIC   bad_rate_diff_pp               DOUBLE,           -- = perc_bad_decil_diff_pp
# MAGIC   pct_bad_share                  DOUBLE,           -- = df_dist_bad_decil
# MAGIC   pct_bad_share_diff_pp          DOUBLE,           -- = df_dist_bad_decil_diff_pp
# MAGIC   psi_contribution               DOUBLE,           -- = iep_por_decil (contribuicao da banda; band=100 e o total)
# MAGIC   market_name                    STRING,
# MAGIC   metric_key                     STRING,
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabela 4: metricas por feature x bin (long format) — substitui 7 tabelas Excel
# MAGIC DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_features_me_br;
# MAGIC CREATE TABLE ds_catalog_dev.credit_engine.monitoring_features_me_br (
# MAGIC   reference_month                DATE,
# MAGIC   period                         STRING,
# MAGIC   feature_name                   STRING,           -- ex: 'pct_months_overdue_10_20', 'portfolio_score', 'score'
# MAGIC   grupo                          STRING,           -- 'severidade_atraso', 'peso_historico', 'exposicao', 'benchmark', 'score_individual'
# MAGIC   coeficiente                    DOUBLE,           -- peso efetivo (media mediana_cluster); NULL se nao aplicavel
# MAGIC   bin_label                      STRING,           -- ex: '(-inf, 0.5]', '(0.5, inf]', 'Missing'
# MAGIC   pct_pop                        DOUBLE,           -- = dist_vars
# MAGIC   pct_pop_diff_pp                DOUBLE,           -- = dist_vars_diff_pp
# MAGIC   volume                         DOUBLE,           -- = vol_vars (contagem absoluta)
# MAGIC   lift                           DOUBLE,           -- = risco_relativo (bad_rate_bin / bad_rate_geral)
# MAGIC   lift_trigger                   STRING,           -- = risco_relativo_trigger ('true' se fora [0.5, 2.0])
# MAGIC   psi_feature                    DOUBLE,           -- = iep (PSI da feature inteira; repetido em cada bin)
# MAGIC   psi_within_group               DOUBLE,           -- = iep_vars (PSI da feature dentro do grupo)
# MAGIC   market_name                    STRING,
# MAGIC   metric_key                     STRING,
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabela 5: matriz de migracao mes-1 -> mes (long format) — substitui decile_migrations
# MAGIC DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br;
# MAGIC CREATE TABLE ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br (
# MAGIC   reference_month                DATE,
# MAGIC   period                         STRING,
# MAGIC   previous_band                  INT,              -- -1 = novo cliente (nao havia banda no mes anterior)
# MAGIC   current_band                   INT,
# MAGIC   count                          INT,
# MAGIC   pct_of_pop                     DOUBLE,
# MAGIC   market_name                    STRING,
# MAGIC   metric_key                     STRING,
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo unificado das 4 tabelas de métricas

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.metrics import roc_auc_score
from scipy.stats import ks_2samp
from pyspark.sql.types import (
    StructType, StructField, DateType, IntegerType,
    DoubleType, StringType, TimestampType
)

# ============================================================
# Configuracao
# ============================================================
TARGET_COL  = 'target_percent7mob1'
SCORE_COL   = 'integrated_score'
BAND_COL    = 'integrated_score_band'
BAND_MAP    = {'1-BAIXO': 1, '2-MEDIO': 2, '3-ALTO': 3}
BAND_NAMES  = {1: '1-BAIXO', 2: '2-MEDIO', 3: '3-ALTO', -1: 'Missing', 100: 'TOTAL'}
MARKET      = 'me'
TRAIN_CUTOFF = pd.Timestamp(train_cutoff)

# Features e seus grupos / coeficientes efetivos
FEATURES = [
    ('pct_months_overdue_10_20',   'severidade_atraso', 'median_cluster_10'),
    ('pct_months_overdue_20_30',   'severidade_atraso', 'median_cluster_20'),
    ('pct_months_overdue_30_50',   'severidade_atraso', 'median_cluster_30'),
    ('pct_months_overdue_50_plus', 'severidade_atraso', 'median_cluster_50'),
    ('overdue_pct',                'severidade_atraso', None),
    ('months_with_billing',        'peso_historico',    None),
    ('months_defaulted',           'peso_historico',    None),
    ('historical_weight',          'peso_historico',    None),
    ('reference_value',            'exposicao',         None),
    ('payment_term',               'exposicao',         None),
    ('portfolio_score',            'benchmark',         None),
    ('score',                      'score_individual',  None),
]

# ============================================================
# Funcoes auxiliares
# ============================================================
def calc_psi(base_dist, current_dist, epsilon=1e-4):
    keys = set(base_dist) | set(current_dist)
    total = 0.0
    for k in keys:
        b = base_dist.get(k, 0.0) + epsilon
        c = current_dist.get(k, 0.0) + epsilon
        total += (c - b) * np.log(c / b)
    return float(total)

def psi_contribution(base_share, curr_share, epsilon=1e-4):
    b = base_share + epsilon
    c = curr_share + epsilon
    return float((c - b) * np.log(c / b))

def classify_psi(val):
    if val is None: return None
    if val < 0.10:  return 'Estavel'
    if val < 0.25:  return 'Moderado'
    return 'Significativo'

def safe_round(val, decimals=6):
    if val is None: return None
    if isinstance(val, float) and np.isnan(val): return None
    return round(float(val), decimals)

def fit_quantile_bins(series, n_bins=5):
    s = pd.to_numeric(series, errors='coerce').dropna()
    if s.nunique() <= 6:
        return None  # tratar como discreto
    edges = np.unique(np.quantile(s, np.linspace(0, 1, n_bins + 1)))
    edges[0], edges[-1] = -np.inf, np.inf
    return list(edges)

def apply_bins(series, edges):
    s = pd.to_numeric(series, errors='coerce')
    if edges is None:
        return series.where(series.notna(), 'Missing').astype(str)
    labels = pd.cut(s, bins=edges, include_lowest=True, duplicates='drop').astype(str)
    labels = pd.Series(labels, index=series.index)
    labels[s.isna()] = 'Missing'
    return labels

def norm_vc(series):
    vc = series.value_counts(normalize=True)
    return {str(k): float(v) for k, v in vc.items()}

def safe_sum(series):
    non_null = series.dropna()
    return float(non_null.sum()) if len(non_null) > 0 else None

# ============================================================
# Carga
# ============================================================
df = spark.sql("""
    SELECT id_customer, reference_month, integrated_score, integrated_score_band,
           total_amount, overdue_amount, credit_limit_end, credit_limit_end_clp,
           target_percent7mob1,
           pct_months_overdue_10_20, pct_months_overdue_20_30,
           pct_months_overdue_30_50, pct_months_overdue_50_plus,
           overdue_pct, months_with_billing, months_defaulted,
           historical_weight, reference_value, payment_term,
           portfolio_score, score,
           median_cluster_10, median_cluster_20,
           median_cluster_30, median_cluster_50
    FROM ds_catalog_dev.credit_engine.monitoring_me_br
    WHERE integrated_score IS NOT NULL
""").toPandas()

df['reference_month'] = pd.to_datetime(df['reference_month'])
df = df.sort_values('reference_month').reset_index(drop=True)
df['band'] = df[BAND_COL].map(BAND_MAP).fillna(-1).astype(int)
df['period'] = np.where(
    df['reference_month'] < TRAIN_CUTOFF,
    'Train',
    df['reference_month'].dt.strftime('%Y/%m')
)

train_df = df[df['period'] == 'Train']
periods  = ['Train'] + sorted(df.loc[df['period'] != 'Train', 'period'].unique().tolist())

if len(train_df) == 0:
    raise ValueError(f"Sem dados em 'Train' (reference_month < {TRAIN_CUTOFF.date()}). Ajuste o widget train_cutoff.")

print(f"Registros : {len(df):,}")
print(f"Periodos  : Train ({len(train_df):,} linhas) + {len(periods)-1} safras OOT")
print(f"Range     : {df['reference_month'].min().date()} a {df['reference_month'].max().date()}")

# ============================================================
# Coeficientes efetivos (media mediana_cluster_X)
# ============================================================
COEF_MAP = {}
for fname, _, coef_col in FEATURES:
    if coef_col and coef_col in df.columns:
        m = df[coef_col].mean()
        COEF_MAP[fname] = float(m) if not pd.isna(m) else None
    else:
        COEF_MAP[fname] = None

# ============================================================
# Baselines do Train
# ============================================================
TRAIN_BAND_DIST  = norm_vc(train_df['band'].astype(str))
train_t          = train_df[train_df[TARGET_COL].notna()]
TRAIN_BAD_OVERALL = float(train_t[TARGET_COL].mean()) if len(train_t) else None
TRAIN_BAD_BY_BAND = train_t.groupby('band')[TARGET_COL].mean().to_dict() if len(train_t) else {}

def bad_share_by_band(sub):
    st = sub[sub[TARGET_COL].notna()]
    tot = st[TARGET_COL].sum()
    if tot == 0: return {}
    return (st.groupby('band')[TARGET_COL].sum() / tot).to_dict()

TRAIN_BADSHARE   = bad_share_by_band(train_df)
BIN_EDGES        = {f: fit_quantile_bins(train_df[f]) for f, _, _ in FEATURES}
TRAIN_FEAT_DIST  = {f: norm_vc(apply_bins(train_df[f], BIN_EDGES[f])) for f, _, _ in FEATURES}

# ============================================================
# Iteracao por periodo e coleta
# ============================================================
metrics_rows   = []
band_rows      = []
feature_rows   = []
migration_rows = []
NOW = datetime.now()
prev_sub = None

for i, period in enumerate(periods):
    sub = df[df['period'] == period].copy()
    if len(sub) == 0:
        continue
    ref_m = sub['reference_month'].max()
    sub_t = sub[sub[TARGET_COL].notna()].copy()
    total = len(sub)

    # ===== Band distributions for this period =====
    band_dist = norm_vc(sub['band'].astype(str))
    bs        = bad_share_by_band(sub)
    is_train  = (period == 'Train')

    # ============================================================
    # monitoring_metrics_me_br (1 linha por periodo)
    # ============================================================
    pop_baixo = int((sub['band'] == 1).sum())
    pop_medio = int((sub['band'] == 2).sum())
    pop_alto  = int((sub['band'] == 3).sum())
    pct_baixo = safe_round(pop_baixo / total * 100, 4) if total > 0 else None
    pct_medio = safe_round(pop_medio / total * 100, 4) if total > 0 else None
    pct_alto  = safe_round(pop_alto  / total * 100, 4) if total > 0 else None

    avg_score    = safe_round(sub[SCORE_COL].mean())
    median_score = safe_round(sub[SCORE_COL].median())

    avg_cl    = safe_round(sub['credit_limit_end'].mean(), 2)
    med_cl    = safe_round(sub['credit_limit_end'].median(), 2)
    tot_cl    = safe_round(safe_sum(sub['credit_limit_end']), 2)
    avg_clp   = safe_round(sub['credit_limit_end_clp'].mean(), 2)
    med_clp   = safe_round(sub['credit_limit_end_clp'].median(), 2)
    tot_clp   = safe_round(safe_sum(sub['credit_limit_end_clp']), 2)

    tot_billed  = safe_round(safe_sum(sub['total_amount']), 2)
    tot_overdue = safe_round(safe_sum(sub['overdue_amount']), 2)
    overdue_pct_period = safe_round(tot_overdue / tot_billed * 100, 4) if (tot_billed is not None and tot_billed > 0) else None

    bad_rate_overall = bad_rate_baixo = bad_rate_medio = bad_rate_alto = None
    lift_baixo = lift_medio = lift_alto = None
    ks_val = roc_val = gini_val = None
    if len(sub_t) > 0:
        sub_t[TARGET_COL] = sub_t[TARGET_COL].astype(int)
        y = sub_t[TARGET_COL].values
        s = sub_t[SCORE_COL].values
        bad_rate_overall = safe_round(float(y.mean()) * 100)   # stored as % (0-100)
        for b, attr in [(1, 'bad_rate_baixo'), (2, 'bad_rate_medio'), (3, 'bad_rate_alto')]:
            mask = sub_t['band'] == b
            if mask.any():
                v = float(sub_t.loc[mask, TARGET_COL].mean()) * 100   # %
                if attr == 'bad_rate_baixo': bad_rate_baixo = safe_round(v)
                elif attr == 'bad_rate_medio': bad_rate_medio = safe_round(v)
                elif attr == 'bad_rate_alto':  bad_rate_alto  = safe_round(v)
        if bad_rate_overall and bad_rate_overall > 0:
            if bad_rate_baixo is not None: lift_baixo = safe_round(bad_rate_baixo / bad_rate_overall, 4)
            if bad_rate_medio is not None: lift_medio = safe_round(bad_rate_medio / bad_rate_overall, 4)
            if bad_rate_alto  is not None: lift_alto  = safe_round(bad_rate_alto  / bad_rate_overall, 4)
        if y.sum() > 0 and y.sum() < len(y):
            good = s[y == 0]
            bad  = s[y == 1]
            ks_stat, _ = ks_2samp(good, bad)
            ks_val = safe_round(float(ks_stat))
            auc_val = roc_auc_score(y, s)
            roc_val = safe_round(float(auc_val))
            gini_val = safe_round(float(2 * auc_val - 1))

    psi_train = calc_psi(TRAIN_BAND_DIST, band_dist) if not is_train else 0.0
    psi_rolling = None
    if prev_sub is not None:
        prev_band_dist = norm_vc(prev_sub['band'].astype(str))
        psi_rolling = calc_psi(prev_band_dist, band_dist)

    pct_improved = pct_maintained = pct_worsened = None
    if prev_sub is not None and len(prev_sub) > 0:
        merged = prev_sub[['id_customer', 'band']].rename(columns={'band': 'prev'}).merge(
            sub[['id_customer', 'band']].rename(columns={'band': 'curr'}),
            on='id_customer', how='inner'
        )
        if len(merged) > 0:
            direction = merged['curr'] - merged['prev']
            pct_improved   = safe_round((direction < 0).mean() * 100, 4)
            pct_maintained = safe_round((direction == 0).mean() * 100, 4)
            pct_worsened   = safe_round((direction > 0).mean() * 100, 4)

    metrics_rows.append({
        'reference_month': ref_m, 'period': period,
        'total_clients': total,
        'pop_baixo': pop_baixo, 'pop_medio': pop_medio, 'pop_alto': pop_alto,
        'pct_baixo': pct_baixo, 'pct_medio': pct_medio, 'pct_alto': pct_alto,
        'avg_score': avg_score, 'median_score': median_score,
        'avg_credit_limit': avg_cl, 'median_credit_limit': med_cl, 'total_credit_limit': tot_cl,
        'avg_credit_limit_clp': avg_clp, 'median_credit_limit_clp': med_clp, 'total_credit_limit_clp': tot_clp,
        'total_billed_usd': tot_billed, 'overdue_usd': tot_overdue, 'overdue_pct': overdue_pct_period,
        'bad_rate_overall': bad_rate_overall,
        'bad_rate_baixo': bad_rate_baixo, 'bad_rate_medio': bad_rate_medio, 'bad_rate_alto': bad_rate_alto,
        'lift_baixo': lift_baixo, 'lift_medio': lift_medio, 'lift_alto': lift_alto,
        'ks': ks_val, 'roc_auc': roc_val, 'gini': gini_val,
        'psi_vs_train': safe_round(psi_train), 'psi_vs_train_classification': classify_psi(psi_train),
        'psi_rolling': safe_round(psi_rolling), 'psi_rolling_classification': classify_psi(psi_rolling),
        'pct_improved': pct_improved, 'pct_maintained': pct_maintained, 'pct_worsened': pct_worsened,
        'market_name': MARKET, 'metric_key': 'monitoring_metrics', 'updated_at': NOW,
    })

    # ============================================================
    # monitoring_band_me_br (1 linha por banda + 1 total)
    # ============================================================
    psi_total = 0.0
    for b_str in sorted(set(list(TRAIN_BAND_DIST) + list(band_dist))):
        b_int = int(float(b_str))
        pct_pop = band_dist.get(b_str, 0.0) * 100
        pct_pop_train = TRAIN_BAND_DIST.get(b_str, 0.0) * 100
        pct_pop_diff = (pct_pop - pct_pop_train) if not is_train else 0.0

        br = None
        if len(sub_t) > 0:
            mask = sub_t['band'] == b_int
            if mask.any():
                br = float(sub_t.loc[mask, TARGET_COL].mean()) * 100
        br_train = TRAIN_BAD_BY_BAND.get(b_int)
        br_train_pct = br_train * 100 if br_train is not None else None
        br_diff = (br - br_train_pct) if (br is not None and br_train_pct is not None and not is_train) else (0.0 if is_train else None)

        pbs = bs.get(b_int, 0.0) * 100 if bs else None
        pbs_train = TRAIN_BADSHARE.get(b_int, 0.0) * 100 if TRAIN_BADSHARE else None
        pbs_diff = (pbs - pbs_train) if (pbs is not None and pbs_train is not None and not is_train) else (0.0 if is_train else None)

        psi_c = psi_contribution(TRAIN_BAND_DIST.get(b_str, 0.0), band_dist.get(b_str, 0.0)) if not is_train else 0.0
        psi_total += psi_c

        band_rows.append({
            'reference_month': ref_m, 'period': period,
            'band': b_int, 'band_name': BAND_NAMES.get(b_int, str(b_int)),
            'pct_pop': safe_round(pct_pop),
            'pct_pop_diff_pp': safe_round(pct_pop_diff),
            'bad_rate': safe_round(br),
            'bad_rate_diff_pp': safe_round(br_diff),
            'pct_bad_share': safe_round(pbs),
            'pct_bad_share_diff_pp': safe_round(pbs_diff),
            'psi_contribution': safe_round(psi_c),
            'market_name': MARKET, 'metric_key': 'monitoring_band', 'updated_at': NOW,
        })

    # Linha total (band = 100)
    train_bad_pct = TRAIN_BAD_OVERALL * 100 if TRAIN_BAD_OVERALL is not None else None
    total_br_diff = (bad_rate_overall - train_bad_pct) if (bad_rate_overall is not None and train_bad_pct is not None and not is_train) else (0.0 if is_train else None)
    band_rows.append({
        'reference_month': ref_m, 'period': period,
        'band': 100, 'band_name': 'TOTAL',
        'pct_pop': 100.0, 'pct_pop_diff_pp': 0.0,
        'bad_rate': bad_rate_overall,  # already in % (0-100)
        'bad_rate_diff_pp': safe_round(total_br_diff),
        'pct_bad_share': 100.0, 'pct_bad_share_diff_pp': 0.0,
        'psi_contribution': safe_round(psi_total),
        'market_name': MARKET, 'metric_key': 'monitoring_band', 'updated_at': NOW,
    })

    # ============================================================
    # monitoring_features_me_br (1 linha por feature x bin)
    # ============================================================
    # Bad rate overall do periodo (denominador do lift)
    period_bad_overall = float(sub_t[TARGET_COL].mean()) if len(sub_t) > 0 else None

    # PSI por feature (versus Train baseline)
    psi_per_feature = {}
    for fname, _, _ in FEATURES:
        dist_now = norm_vc(apply_bins(sub[fname], BIN_EDGES[fname]))
        psi_per_feature[fname] = calc_psi(TRAIN_FEAT_DIST[fname], dist_now) if not is_train else 0.0

    # psi_within_group = fracao do PSI do grupo que esta feature explica
    psi_group_sums = {}
    for g in set(grupo for _, grupo, _ in FEATURES):
        feats_in_group = [f for f, gg, _ in FEATURES if gg == g]
        psi_group_sums[g] = float(sum(psi_per_feature[f] for f in feats_in_group))

    for fname, grupo, _ in FEATURES:
        coef = COEF_MAP.get(fname)
        bins_now = apply_bins(sub[fname], BIN_EDGES[fname])
        dist_now = norm_vc(bins_now)
        vol_now  = bins_now.value_counts().to_dict()
        train_dist = TRAIN_FEAT_DIST[fname]

        # Lift por bin
        bin_bad_rates = {}
        if period_bad_overall and period_bad_overall > 0 and len(sub_t) > 0:
            sub_t_bins = apply_bins(sub_t[fname], BIN_EDGES[fname])
            bin_bad_rates = sub_t.assign(_bin=sub_t_bins).groupby('_bin')[TARGET_COL].mean().to_dict()

        all_bins = sorted(set(list(dist_now) + list(train_dist)))
        for bin_lbl in all_bins:
            pct_pop = dist_now.get(bin_lbl, 0.0) * 100
            pct_pop_train = train_dist.get(bin_lbl, 0.0) * 100
            pct_pop_diff = (pct_pop - pct_pop_train) if not is_train else 0.0
            volume = float(vol_now.get(bin_lbl, 0))

            br = bin_bad_rates.get(bin_lbl)
            if br is not None and period_bad_overall and period_bad_overall > 0:
                lift_val = float(br) / float(period_bad_overall)
                lift_trigger = 'true' if (lift_val < 0.5 or lift_val > 2.0) else 'false'
            else:
                lift_val = None
                lift_trigger = None

            feature_rows.append({
                'reference_month': ref_m, 'period': period,
                'feature_name': fname, 'grupo': grupo, 'coeficiente': coef,
                'bin_label': str(bin_lbl),
                'pct_pop': safe_round(pct_pop),
                'pct_pop_diff_pp': safe_round(pct_pop_diff),
                'volume': volume,
                'lift': safe_round(lift_val),
                'lift_trigger': lift_trigger,
                'psi_feature': safe_round(psi_per_feature[fname]),
                'psi_within_group': safe_round(psi_per_feature[fname] / psi_group_sums[grupo]) if psi_group_sums[grupo] > 1e-9 else 0.0,
                'market_name': MARKET, 'metric_key': 'monitoring_features', 'updated_at': NOW,
            })

    # ============================================================
    # monitoring_band_migrations_me_br
    # ============================================================
    if prev_sub is not None and len(prev_sub) > 0 and not is_train:
        merged = prev_sub[['id_customer', 'band']].rename(columns={'band': 'prev'}).merge(
            sub[['id_customer', 'band']].rename(columns={'band': 'curr'}),
            on='id_customer', how='right'
        )
        merged['prev'] = merged['prev'].fillna(-1).astype(int)
        g = merged.groupby(['prev', 'curr']).size().reset_index(name='count')
        for _, row in g.iterrows():
            migration_rows.append({
                'reference_month': ref_m, 'period': period,
                'previous_band': int(row['prev']),
                'current_band':  int(row['curr']),
                'count': int(row['count']),
                'pct_of_pop': safe_round(float(row['count']) / total * 100) if total > 0 else None,
                'market_name': MARKET, 'metric_key': 'monitoring_band_migrations', 'updated_at': NOW,
            })

    prev_sub = sub

print(f"\nmetricas_metrics  : {len(metrics_rows)} linhas")
print(f"metricas_band     : {len(band_rows)} linhas")
print(f"metricas_features : {len(feature_rows)} linhas")
print(f"metricas_migration: {len(migration_rows)} linhas")

# ============================================================
# Escrita (OVERWRITE em todas)
# ============================================================
def write_overwrite(rows, schema, table_name):
    if not rows:
        print(f"  {table_name}: 0 linhas, skip")
        return
    pdf = pd.DataFrame(rows)
    sdf = spark.createDataFrame(pdf, schema=schema)
    sdf.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(table_name)
    print(f"  {table_name}: {len(rows)} linhas (OVERWRITE)")

schema_metrics = StructType([
    StructField('reference_month', DateType()),
    StructField('period', StringType()),
    StructField('total_clients', IntegerType()),
    StructField('pop_baixo', IntegerType()), StructField('pop_medio', IntegerType()), StructField('pop_alto', IntegerType()),
    StructField('pct_baixo', DoubleType()), StructField('pct_medio', DoubleType()), StructField('pct_alto', DoubleType()),
    StructField('avg_score', DoubleType()), StructField('median_score', DoubleType()),
    StructField('avg_credit_limit', DoubleType()), StructField('median_credit_limit', DoubleType()), StructField('total_credit_limit', DoubleType()),
    StructField('avg_credit_limit_clp', DoubleType()), StructField('median_credit_limit_clp', DoubleType()), StructField('total_credit_limit_clp', DoubleType()),
    StructField('total_billed_usd', DoubleType()), StructField('overdue_usd', DoubleType()), StructField('overdue_pct', DoubleType()),
    StructField('bad_rate_overall', DoubleType()),
    StructField('bad_rate_baixo', DoubleType()), StructField('bad_rate_medio', DoubleType()), StructField('bad_rate_alto', DoubleType()),
    StructField('lift_baixo', DoubleType()), StructField('lift_medio', DoubleType()), StructField('lift_alto', DoubleType()),
    StructField('ks', DoubleType()), StructField('roc_auc', DoubleType()), StructField('gini', DoubleType()),
    StructField('psi_vs_train', DoubleType()), StructField('psi_vs_train_classification', StringType()),
    StructField('psi_rolling', DoubleType()), StructField('psi_rolling_classification', StringType()),
    StructField('pct_improved', DoubleType()), StructField('pct_maintained', DoubleType()), StructField('pct_worsened', DoubleType()),
    StructField('market_name', StringType()), StructField('metric_key', StringType()), StructField('updated_at', TimestampType()),
])

schema_band = StructType([
    StructField('reference_month', DateType()), StructField('period', StringType()),
    StructField('band', IntegerType()), StructField('band_name', StringType()),
    StructField('pct_pop', DoubleType()), StructField('pct_pop_diff_pp', DoubleType()),
    StructField('bad_rate', DoubleType()), StructField('bad_rate_diff_pp', DoubleType()),
    StructField('pct_bad_share', DoubleType()), StructField('pct_bad_share_diff_pp', DoubleType()),
    StructField('psi_contribution', DoubleType()),
    StructField('market_name', StringType()), StructField('metric_key', StringType()), StructField('updated_at', TimestampType()),
])

schema_features = StructType([
    StructField('reference_month', DateType()), StructField('period', StringType()),
    StructField('feature_name', StringType()), StructField('grupo', StringType()), StructField('coeficiente', DoubleType()),
    StructField('bin_label', StringType()),
    StructField('pct_pop', DoubleType()), StructField('pct_pop_diff_pp', DoubleType()),
    StructField('volume', DoubleType()),
    StructField('lift', DoubleType()), StructField('lift_trigger', StringType()),
    StructField('psi_feature', DoubleType()), StructField('psi_within_group', DoubleType()),
    StructField('market_name', StringType()), StructField('metric_key', StringType()), StructField('updated_at', TimestampType()),
])

schema_migration = StructType([
    StructField('reference_month', DateType()), StructField('period', StringType()),
    StructField('previous_band', IntegerType()), StructField('current_band', IntegerType()),
    StructField('count', IntegerType()), StructField('pct_of_pop', DoubleType()),
    StructField('market_name', StringType()), StructField('metric_key', StringType()), StructField('updated_at', TimestampType()),
])

print("\nEscrevendo tabelas:")
write_overwrite(metrics_rows,   schema_metrics,   'ds_catalog_dev.credit_engine.monitoring_metrics_me_br')
write_overwrite(band_rows,      schema_band,      'ds_catalog_dev.credit_engine.monitoring_band_me_br')
write_overwrite(feature_rows,   schema_features,  'ds_catalog_dev.credit_engine.monitoring_features_me_br')
write_overwrite(migration_rows, schema_migration, 'ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br')

print("\nOK: 4 tabelas de metricas atualizadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'monitoring_me_br' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT reference_month) AS safras FROM ds_catalog_dev.credit_engine.monitoring_me_br
# MAGIC UNION ALL SELECT 'monitoring_metrics_me_br',         COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
# MAGIC UNION ALL SELECT 'monitoring_band_me_br',            COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
# MAGIC UNION ALL SELECT 'monitoring_features_me_br',        COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
# MAGIC UNION ALL SELECT 'monitoring_band_migrations_me_br', COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Performance (KS/ROC/Gini) e PSI por safra OOT
# MAGIC SELECT period, reference_month,
# MAGIC        ROUND(ks,4)               AS ks,
# MAGIC        ROUND(roc_auc,4)          AS roc,
# MAGIC        ROUND(gini,4)             AS gini,
# MAGIC        ROUND(psi_vs_train,4)     AS psi_vs_train,
# MAGIC        psi_vs_train_classification,
# MAGIC        ROUND(bad_rate_overall,2) AS bad_rate_pct
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
# MAGIC WHERE period <> 'Train'
# MAGIC ORDER BY reference_month DESC
# MAGIC LIMIT 24

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Distribuicao por banda na safra mais recente
# MAGIC SELECT period, band_name,
# MAGIC        ROUND(pct_pop,2)              AS pct_pop,
# MAGIC        ROUND(pct_pop_diff_pp,2)      AS pct_pop_diff_pp,
# MAGIC        ROUND(bad_rate,2)             AS bad_rate,
# MAGIC        ROUND(bad_rate_diff_pp,2)     AS bad_rate_diff_pp,
# MAGIC        ROUND(pct_bad_share,2)        AS pct_bad_share,
# MAGIC        ROUND(psi_contribution,4)     AS psi_contribution
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
# MAGIC WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_band_me_br WHERE period <> 'Train')
# MAGIC ORDER BY band

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Features com maior PSI na safra mais recente
# MAGIC SELECT period, feature_name, grupo,
# MAGIC        ROUND(MAX(psi_feature), 4)  AS psi,
# MAGIC        CASE WHEN MAX(psi_feature) < 0.10 THEN 'Estavel'
# MAGIC             WHEN MAX(psi_feature) < 0.25 THEN 'Moderado'
# MAGIC             ELSE 'Significativo' END AS classificacao
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
# MAGIC WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_features_me_br WHERE period <> 'Train')
# MAGIC GROUP BY period, feature_name, grupo
# MAGIC ORDER BY psi DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Triggers de risco relativo na safra mais recente
# MAGIC SELECT feature_name, grupo, bin_label, ROUND(lift,3) AS lift, lift_trigger, ROUND(volume,0) AS volume
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
# MAGIC WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_features_me_br WHERE period <> 'Train')
# MAGIC   AND lift_trigger = 'true'
# MAGIC ORDER BY ABS(lift - 1) DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Matriz de migracao da safra mais recente (heatmap)
# MAGIC SELECT previous_band, current_band, count, ROUND(pct_of_pop, 2) AS pct
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br
# MAGIC WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br)
# MAGIC ORDER BY previous_band, current_band

# COMMAND ----------

# MAGIC %sql
# MAGIC -- V1: Diagnostico de campos NULL em monitoring_metrics_me_br
# MAGIC -- Safras sem target = target ainda nao maduro (normal para meses recentes)
# MAGIC -- Safras sem billing = join com abt_inference sem match (verificar pipeline)
# MAGIC SELECT
# MAGIC   period,
# MAGIC   reference_month,
# MAGIC   total_clients,
# MAGIC   CASE WHEN bad_rate_overall IS NULL THEN 'SEM TARGET' ELSE 'com target' END AS status_target,
# MAGIC   CASE WHEN ks IS NULL            THEN 'NULL' ELSE CAST(ROUND(ks,4) AS STRING)      END AS ks,
# MAGIC   CASE WHEN roc_auc IS NULL       THEN 'NULL' ELSE CAST(ROUND(roc_auc,4) AS STRING) END AS roc_auc,
# MAGIC   CASE WHEN total_billed_usd IS NULL THEN 'SEM BILLING' ELSE CAST(ROUND(total_billed_usd,0) AS STRING) END AS total_billed,
# MAGIC   CASE WHEN avg_credit_limit IS NULL THEN 'SEM LIMITE'  ELSE CAST(ROUND(avg_credit_limit,0) AS STRING) END AS avg_limit,
# MAGIC   CASE WHEN psi_rolling IS NULL   THEN 'NULL (1o periodo)' ELSE CAST(ROUND(psi_rolling,4) AS STRING) END AS psi_rolling
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
# MAGIC ORDER BY reference_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- V2: Evolucao do PSI vs Train por safra OOT (todas as safras)
# MAGIC SELECT
# MAGIC   period,
# MAGIC   reference_month,
# MAGIC   ROUND(psi_vs_train, 4)              AS psi_vs_train,
# MAGIC   psi_vs_train_classification,
# MAGIC   ROUND(psi_rolling, 4)               AS psi_rolling,
# MAGIC   psi_rolling_classification,
# MAGIC   ROUND(bad_rate_overall, 2)          AS bad_rate_pct,
# MAGIC   ROUND(pct_baixo, 1) AS pct_baixo,
# MAGIC   ROUND(pct_medio, 1) AS pct_medio,
# MAGIC   ROUND(pct_alto,  1) AS pct_alto
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
# MAGIC ORDER BY reference_month

# COMMAND ----------

# MAGIC %sql
# MAGIC -- V3: PSI por feature ao longo do tempo (ultimas 6 safras OOT)
# MAGIC SELECT
# MAGIC   period,
# MAGIC   feature_name,
# MAGIC   grupo,
# MAGIC   ROUND(MAX(psi_feature), 4)       AS psi_feature,
# MAGIC   ROUND(MAX(psi_within_group), 4)  AS psi_share_do_grupo,
# MAGIC   CASE WHEN MAX(psi_feature) < 0.10 THEN 'Estavel'
# MAGIC        WHEN MAX(psi_feature) < 0.25 THEN 'Moderado'
# MAGIC        ELSE 'Significativo' END     AS classificacao
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
# MAGIC WHERE period IN (
# MAGIC   SELECT DISTINCT period FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
# MAGIC   WHERE period <> 'Train'
# MAGIC   ORDER BY period DESC LIMIT 6
# MAGIC )
# MAGIC GROUP BY period, feature_name, grupo
# MAGIC ORDER BY feature_name, period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- V4: Resumo de triggers por safra (contagem de bins com lift fora [0.5, 2.0])
# MAGIC SELECT
# MAGIC   period,
# MAGIC   COUNT(*) FILTER (WHERE lift_trigger = 'true')  AS bins_em_alerta,
# MAGIC   COUNT(*) FILTER (WHERE lift_trigger = 'false') AS bins_ok,
# MAGIC   COUNT(*) FILTER (WHERE lift IS NULL)            AS bins_sem_target,
# MAGIC   ROUND(COUNT(*) FILTER (WHERE lift_trigger = 'true') * 100.0 / NULLIF(COUNT(*) FILTER (WHERE lift IS NOT NULL), 0), 1) AS pct_bins_alerta
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
# MAGIC GROUP BY period
# MAGIC ORDER BY period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- V5: Estabilidade das bandas ao longo do tempo
# MAGIC -- Verifica se a distribuicao BAIXO/MEDIO/ALTO esta estavel (diff_pp vs Train)
# MAGIC SELECT
# MAGIC   b.period,
# MAGIC   b.reference_month,
# MAGIC   MAX(CASE WHEN b.band = 1   THEN ROUND(b.pct_pop, 1)          END) AS pct_baixo,
# MAGIC   MAX(CASE WHEN b.band = 1   THEN ROUND(b.pct_pop_diff_pp, 2)  END) AS baixo_diff_pp,
# MAGIC   MAX(CASE WHEN b.band = 2   THEN ROUND(b.pct_pop, 1)          END) AS pct_medio,
# MAGIC   MAX(CASE WHEN b.band = 2   THEN ROUND(b.pct_pop_diff_pp, 2)  END) AS medio_diff_pp,
# MAGIC   MAX(CASE WHEN b.band = 3   THEN ROUND(b.pct_pop, 1)          END) AS pct_alto,
# MAGIC   MAX(CASE WHEN b.band = 3   THEN ROUND(b.pct_pop_diff_pp, 2)  END) AS alto_diff_pp,
# MAGIC   MAX(CASE WHEN b.band = 100 THEN ROUND(b.psi_contribution, 4) END) AS psi_total,
# MAGIC   MAX(CASE WHEN b.band = 100 THEN ROUND(b.bad_rate, 2)         END) AS bad_rate_total_pct,
# MAGIC   MAX(CASE WHEN b.band = 100 THEN ROUND(b.bad_rate_diff_pp, 2) END) AS bad_rate_diff_pp
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_band_me_br b
# MAGIC GROUP BY b.period, b.reference_month
# MAGIC ORDER BY b.reference_month
