"""Gerador do notebook v2 de monitoramento ME BR.
Consolida 16 tabelas do Excel em 3 tabelas tidy + mantem as 2 wide ja existentes.
"""
import json

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text if isinstance(text, list) else [text]}

def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text if isinstance(text, list) else [text]}

CELLS = []

# ============================================================
# Cell 0: Header
# ============================================================
CELLS.append(md("""## Pipeline: Monitoramento ME BR — v2 (consolidado)

Este notebook gera **5 tabelas** que substituem as 16 tabelas Excel do padrão antigo + as 2 tabelas wide do v1.

---

### Tabelas produzidas

| Tabela | Granularidade | Comportamento | Substitui |
|---|---|---|---|
| `monitoring_me_br` | cliente × safra | MISTO (INSERT + UPDATE seletivo) | — (detalhe) |
| `monitoring_metrics_me_br` | 1 linha por safra (wide) | OVERWRITE | — (resumo) |
| `monitoring_band_me_br` | safra × banda | OVERWRITE | dist_por_decil, dist_por_decil_diff_pp, perc_bad_decil, perc_bad_decil_diff_pp, df_dist_bad_decil, df_dist_bad_decil_diff_pp, iep_por_decil |
| `monitoring_features_me_br` | safra × feature × bin | OVERWRITE | dist_vars, dist_vars_diff_pp, vol_vars, iep, iep_vars, risco_relativo, risco_relativo_trigger |
| `monitoring_band_migrations_me_br` | safra × prev_band × curr_band | OVERWRITE | decile_migrations |

**Resultado: 16 tabelas Excel → 3 tabelas tidy (+ 2 já existentes).**

---

### Mapeamento dos KPIs (todos preservados)

**`monitoring_band_me_br`** (1 linha por banda):
- `pct_pop` — % população na banda (= dist_por_decil)
- `pct_pop_diff_pp` — diferença pp vs Train (= dist_por_decil_diff_pp)
- `bad_rate` — % de bads na banda (= perc_bad_decil)
- `bad_rate_diff_pp` — diferença pp vs Train (= perc_bad_decil_diff_pp)
- `pct_bad_share` — % dos bads concentrado na banda (= df_dist_bad_decil)
- `pct_bad_share_diff_pp` — diferença pp vs Train (= df_dist_bad_decil_diff_pp)
- `psi_contribution` — contribuição da banda ao PSI total (= iep_por_decil)

**`monitoring_features_me_br`** (1 linha por feature × bin):
- `pct_pop` — % da população na faixa da feature (= dist_vars)
- `pct_pop_diff_pp` — diferença pp vs Train (= dist_vars_diff_pp)
- `volume` — contagem absoluta na faixa (= vol_vars)
- `lift` — bad_rate(faixa) / bad_rate(geral) (= risco_relativo)
- `lift_trigger` — 'true'/'false' se lift fora [0.5, 2.0] (= risco_relativo_trigger)
- `psi_feature` — PSI da feature inteira (= iep)
- `psi_within_group` — PSI da feature dentro do grupo (= iep_vars)

**`monitoring_band_migrations_me_br`** (1 linha por par de bandas):
- `count`, `pct_of_pop` — contagem e % de clientes na transição (= decile_migrations)

---

### Configuração

- **Score**: `integrated_score`
- **Banda**: `integrated_score_band` (1-BAIXO, 2-MEDIO, 3-ALTO)
- **Target**: `target_percent7mob1`
- **Train cutoff**: `2024-01-01` (configurável via widget)
- **Período**: 'Train' (todas as safras pré-cutoff agregadas) ou 'YYYY/MM' (cada safra OOT individualmente)
- **PSI baseline**: distribuição do período 'Train'
- **diff_pp baseline**: período 'Train'

### Pré-requisitos
NB1, NB1b, NB2, NB2b, NB3 executados (alimentam abt_inference, apply_model, targets, portfolio_abt_group)."""))

# ============================================================
# Cell 1: Widgets
# ============================================================
CELLS.append(code("""# Widgets de configuracao
from datetime import date

dbutils.widgets.text("data_referencia", "", "Data de Referencia (yyyy-mm-dd)")
dbutils.widgets.text("train_cutoff",   "2024-01-01", "Train cutoff (yyyy-mm-dd)")

data_referencia = dbutils.widgets.get("data_referencia") or str(date.today())
train_cutoff    = dbutils.widgets.get("train_cutoff") or "2024-01-01"

print(f"Data de referencia: {data_referencia}")
print(f"Train cutoff      : {train_cutoff}  (safras < cutoff = 'Train', >= cutoff = 'YYYY/MM')")"""))

# ============================================================
# Cell 2: DDL monitoring_me_br
# ============================================================
CELLS.append(code("""%sql
-- Tabela 1: detalhe cliente x safra. Sem DROP: historico preservado.
CREATE TABLE IF NOT EXISTS ds_catalog_dev.credit_engine.monitoring_me_br (
  id_customer                    INT,
  customer_name                  STRING,
  country                        STRING,
  reference_month                DATE,
  feature_reference_month        DATE,
  total_amount                   DOUBLE,
  overdue_amount                 DOUBLE,
  overdue_pct                    DOUBLE,
  months_with_billing            INT,
  months_defaulted               INT,
  pct_months_overdue_10_20       DOUBLE,
  pct_months_overdue_20_30       DOUBLE,
  pct_months_overdue_30_50       DOUBLE,
  pct_months_overdue_50_plus     DOUBLE,
  flag_transacted                INT,
  score                          DOUBLE,
  historical_weight              DOUBLE,
  reference_value                DOUBLE,
  reference_value_clp            DOUBLE,
  payment_term                   DOUBLE,
  median_cluster_10              DOUBLE,
  median_cluster_20              DOUBLE,
  median_cluster_30              DOUBLE,
  median_cluster_50              DOUBLE,
  portfolio_score                DOUBLE,
  adjusted_score                 DOUBLE,
  score_band                     STRING,
  integrated_score               DOUBLE,
  integrated_score_band          STRING,
  credit_limit                   DOUBLE,
  credit_limit_clp               DOUBLE,
  credit_limit_end               DOUBLE,
  credit_limit_end_clp           DOUBLE,
  target_percent7mob1            INT,
  target_percent7mob3            INT,
  target_percent7mob6            INT,
  target_percent7mob9            INT,
  target_percent7mob12           INT,
  target_billed_1m               DOUBLE,
  target_overdue_1m              DOUBLE,
  target_overdue_pct_1m          DOUBLE,
  target_billed_3m               DOUBLE,
  target_overdue_3m              DOUBLE,
  target_overdue_pct_3m          DOUBLE,
  target_billed_6m               DOUBLE,
  target_overdue_6m              DOUBLE,
  target_overdue_pct_6m          DOUBLE,
  target_billed_12m              DOUBLE,
  target_overdue_12m             DOUBLE,
  target_overdue_pct_12m         DOUBLE,
  updated_at                     TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"""))

# ============================================================
# Cell 3: source view
# ============================================================
CELLS.append(code("""%sql
-- View source: apply_model (m) + abt (m-1) + portfolio (m-1) + targets (m)
CREATE OR REPLACE TEMP VIEW monitoring_source AS
SELECT
  am.id_customer, am.customer_name, am.country, am.reference_month,
  add_months(am.reference_month, -1) AS feature_reference_month,
  inf.total_amount, inf.overdue_amount, inf.overdue_pct,
  inf.months_with_billing, inf.months_defaulted,
  inf.pct_months_overdue_10_20, inf.pct_months_overdue_20_30,
  inf.pct_months_overdue_30_50, inf.pct_months_overdue_50_plus,
  inf.flag_transacted, inf.score, inf.historical_weight,
  COALESCE(inf.reference_value, 0)     AS reference_value,
  COALESCE(inf.reference_value_clp, 0) AS reference_value_clp,
  COALESCE(inf.payment_term, 1.0)      AS payment_term,
  pag.median_cluster_10, pag.median_cluster_20,
  pag.median_cluster_30, pag.median_cluster_50, pag.portfolio_score,
  am.adjusted_score, am.score_band,
  am.integrated_score, am.integrated_score_band,
  am.credit_limit, am.credit_limit_clp,
  am.credit_limit_end, am.credit_limit_end_clp,
  t.percent7mob1  AS target_percent7mob1,
  t.percent7mob3  AS target_percent7mob3,
  t.percent7mob6  AS target_percent7mob6,
  t.percent7mob9  AS target_percent7mob9,
  t.percent7mob12 AS target_percent7mob12,
  t.billed_1m  AS target_billed_1m,  t.overdue_1m  AS target_overdue_1m,  t.overdue_pct_1m  AS target_overdue_pct_1m,
  t.billed_3m  AS target_billed_3m,  t.overdue_3m  AS target_overdue_3m,  t.overdue_pct_3m  AS target_overdue_pct_3m,
  t.billed_6m  AS target_billed_6m,  t.overdue_6m  AS target_overdue_6m,  t.overdue_pct_6m  AS target_overdue_pct_6m,
  t.billed_12m AS target_billed_12m, t.overdue_12m AS target_overdue_12m, t.overdue_pct_12m AS target_overdue_pct_12m,
  current_timestamp() AS updated_at
FROM ds_catalog_dev.credit_engine.apply_model_me_br AS am
LEFT JOIN ds_catalog_dev.credit_engine.abt_inference_me_br AS inf
  ON inf.id_customer = am.id_customer
 AND inf.reference_month = add_months(am.reference_month, -1)
LEFT JOIN ds_catalog_dev.credit_engine.portfolio_abt_group_me_br AS pag
  ON pag.reference_month = add_months(am.reference_month, -1)
LEFT JOIN ds_catalog_dev.credit_engine.targets_me_br AS t
  ON t.id_customer = am.id_customer
 AND t.reference_month = am.reference_month"""))

# ============================================================
# Cell 4: MERGE
# ============================================================
CELLS.append(code("""%sql
MERGE INTO ds_catalog_dev.credit_engine.monitoring_me_br AS target
USING monitoring_source AS source
ON target.reference_month = source.reference_month
   AND target.id_customer = source.id_customer
WHEN MATCHED THEN UPDATE SET
  target.integrated_score        = source.integrated_score,
  target.integrated_score_band   = source.integrated_score_band,
  target.credit_limit            = source.credit_limit,
  target.credit_limit_clp        = source.credit_limit_clp,
  target.credit_limit_end        = source.credit_limit_end,
  target.credit_limit_end_clp    = source.credit_limit_end_clp,
  target.target_percent7mob1     = source.target_percent7mob1,
  target.target_percent7mob3     = source.target_percent7mob3,
  target.target_percent7mob6     = source.target_percent7mob6,
  target.target_percent7mob9     = source.target_percent7mob9,
  target.target_percent7mob12    = source.target_percent7mob12,
  target.target_billed_1m        = source.target_billed_1m,
  target.target_overdue_1m       = source.target_overdue_1m,
  target.target_overdue_pct_1m   = source.target_overdue_pct_1m,
  target.target_billed_3m        = source.target_billed_3m,
  target.target_overdue_3m       = source.target_overdue_3m,
  target.target_overdue_pct_3m   = source.target_overdue_pct_3m,
  target.target_billed_6m        = source.target_billed_6m,
  target.target_overdue_6m       = source.target_overdue_6m,
  target.target_overdue_pct_6m   = source.target_overdue_pct_6m,
  target.target_billed_12m       = source.target_billed_12m,
  target.target_overdue_12m      = source.target_overdue_12m,
  target.target_overdue_pct_12m  = source.target_overdue_pct_12m,
  target.updated_at              = source.updated_at
WHEN NOT MATCHED THEN INSERT *"""))

# ============================================================
# Cell 5: DDL 4 tabelas de metricas
# ============================================================
CELLS.append(md("## DDL das 4 tabelas de métricas"))

CELLS.append(code("""%sql
-- Tabela 2: metricas agregadas por safra (1 linha por reference_month)
DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_metrics_me_br;
CREATE TABLE ds_catalog_dev.credit_engine.monitoring_metrics_me_br (
  reference_month                DATE,
  period                         STRING,
  total_clients                  INT,
  pop_baixo                      INT,
  pop_medio                      INT,
  pop_alto                       INT,
  pct_baixo                      DOUBLE,
  pct_medio                      DOUBLE,
  pct_alto                       DOUBLE,
  avg_score                      DOUBLE,
  median_score                   DOUBLE,
  avg_credit_limit               DOUBLE,
  median_credit_limit            DOUBLE,
  total_credit_limit             DOUBLE,
  avg_credit_limit_clp           DOUBLE,
  median_credit_limit_clp        DOUBLE,
  total_credit_limit_clp         DOUBLE,
  total_billed_usd               DOUBLE,
  overdue_usd                    DOUBLE,
  overdue_pct                    DOUBLE,
  bad_rate_overall               DOUBLE,
  bad_rate_baixo                 DOUBLE,
  bad_rate_medio                 DOUBLE,
  bad_rate_alto                  DOUBLE,
  lift_baixo                     DOUBLE,
  lift_medio                     DOUBLE,
  lift_alto                      DOUBLE,
  ks                             DOUBLE,
  roc_auc                        DOUBLE,
  gini                           DOUBLE,
  psi_vs_train                   DOUBLE,
  psi_vs_train_classification    STRING,
  psi_rolling                    DOUBLE,
  psi_rolling_classification     STRING,
  pct_improved                   DOUBLE,
  pct_maintained                 DOUBLE,
  pct_worsened                   DOUBLE,
  market_name                    STRING,
  metric_key                     STRING,
  updated_at                     TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"""))

CELLS.append(code("""%sql
-- Tabela 3: metricas por banda (long format) — substitui 7 tabelas Excel
DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_band_me_br;
CREATE TABLE ds_catalog_dev.credit_engine.monitoring_band_me_br (
  reference_month                DATE,
  period                         STRING,           -- 'Train' ou 'YYYY/MM'
  band                           INT,              -- 1, 2, 3, -1 (missing), 100 (total)
  band_name                      STRING,           -- '1-BAIXO', '2-MEDIO', '3-ALTO', 'Missing', 'TOTAL'
  pct_pop                        DOUBLE,           -- = dist_por_decil
  pct_pop_diff_pp                DOUBLE,           -- = dist_por_decil_diff_pp
  bad_rate                       DOUBLE,           -- = perc_bad_decil (%)
  bad_rate_diff_pp               DOUBLE,           -- = perc_bad_decil_diff_pp
  pct_bad_share                  DOUBLE,           -- = df_dist_bad_decil
  pct_bad_share_diff_pp          DOUBLE,           -- = df_dist_bad_decil_diff_pp
  psi_contribution               DOUBLE,           -- = iep_por_decil (contribuicao da banda; band=100 e o total)
  market_name                    STRING,
  metric_key                     STRING,
  updated_at                     TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"""))

CELLS.append(code("""%sql
-- Tabela 4: metricas por feature x bin (long format) — substitui 7 tabelas Excel
DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_features_me_br;
CREATE TABLE ds_catalog_dev.credit_engine.monitoring_features_me_br (
  reference_month                DATE,
  period                         STRING,
  feature_name                   STRING,           -- ex: 'pct_months_overdue_10_20', 'portfolio_score', 'score'
  grupo                          STRING,           -- 'severidade_atraso', 'peso_historico', 'exposicao', 'benchmark', 'score_individual'
  coeficiente                    DOUBLE,           -- peso efetivo (media mediana_cluster); NULL se nao aplicavel
  bin_label                      STRING,           -- ex: '(-inf, 0.5]', '(0.5, inf]', 'Missing'
  pct_pop                        DOUBLE,           -- = dist_vars
  pct_pop_diff_pp                DOUBLE,           -- = dist_vars_diff_pp
  volume                         DOUBLE,           -- = vol_vars (contagem absoluta)
  lift                           DOUBLE,           -- = risco_relativo (bad_rate_bin / bad_rate_geral)
  lift_trigger                   STRING,           -- = risco_relativo_trigger ('true' se fora [0.5, 2.0])
  psi_feature                    DOUBLE,           -- = iep (PSI da feature inteira; repetido em cada bin)
  psi_within_group               DOUBLE,           -- = iep_vars (PSI da feature dentro do grupo)
  market_name                    STRING,
  metric_key                     STRING,
  updated_at                     TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"""))

CELLS.append(code("""%sql
-- Tabela 5: matriz de migracao mes-1 -> mes (long format) — substitui decile_migrations
DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br;
CREATE TABLE ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br (
  reference_month                DATE,
  period                         STRING,
  previous_band                  INT,              -- -1 = novo cliente (nao havia banda no mes anterior)
  current_band                   INT,
  count                          INT,
  pct_of_pop                     DOUBLE,
  market_name                    STRING,
  metric_key                     STRING,
  updated_at                     TIMESTAMP
)
USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')"""))

# ============================================================
# Cell 10: Python computation
# ============================================================
CELLS.append(md("## Cálculo unificado das 4 tabelas de métricas"))

CELLS.append(code("""import numpy as np
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

# ============================================================
# Carga
# ============================================================
df = spark.sql(\"\"\"
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
\"\"\").toPandas()

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
    tot_cl    = safe_round(sub['credit_limit_end'].sum(), 2)
    avg_clp   = safe_round(sub['credit_limit_end_clp'].mean(), 2)
    med_clp   = safe_round(sub['credit_limit_end_clp'].median(), 2)
    tot_clp   = safe_round(sub['credit_limit_end_clp'].sum(), 2)

    tot_billed = safe_round(sub['total_amount'].sum(), 2)
    tot_overdue = safe_round(sub['overdue_amount'].sum(), 2)
    overdue_pct_period = safe_round(tot_overdue / tot_billed * 100, 4) if tot_billed and tot_billed > 0 else None

    bad_rate_overall = bad_rate_baixo = bad_rate_medio = bad_rate_alto = None
    lift_baixo = lift_medio = lift_alto = None
    ks_val = roc_val = gini_val = None
    if len(sub_t) > 0:
        sub_t[TARGET_COL] = sub_t[TARGET_COL].astype(int)
        y = sub_t[TARGET_COL].values
        s = sub_t[SCORE_COL].values
        bad_rate_overall = safe_round(float(y.mean()))
        for b, attr in [(1, 'bad_rate_baixo'), (2, 'bad_rate_medio'), (3, 'bad_rate_alto')]:
            mask = sub_t['band'] == b
            if mask.any():
                v = float(sub_t.loc[mask, TARGET_COL].mean())
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
    band_rows.append({
        'reference_month': ref_m, 'period': period,
        'band': 100, 'band_name': 'TOTAL',
        'pct_pop': 100.0, 'pct_pop_diff_pp': 0.0,
        'bad_rate': safe_round(bad_rate_overall * 100) if bad_rate_overall is not None else None,
        'bad_rate_diff_pp': None,
        'pct_bad_share': 100.0, 'pct_bad_share_diff_pp': 0.0,
        'psi_contribution': safe_round(psi_total),
        'market_name': MARKET, 'metric_key': 'monitoring_band', 'updated_at': NOW,
    })

    # ============================================================
    # monitoring_features_me_br (1 linha por feature x bin)
    # ============================================================
    # Bad rate overall do periodo (denominador do lift)
    period_bad_overall = float(sub_t[TARGET_COL].mean()) if len(sub_t) > 0 else None

    # PSI agregado por grupo (peso = 1/n_features_no_grupo, simples)
    group_counts = {}
    for _, g, _ in FEATURES:
        group_counts[g] = group_counts.get(g, 0) + 1

    # PSI por feature primeiro (precisamos pra psi_within_group)
    psi_per_feature = {}
    for fname, _, _ in FEATURES:
        dist_now = norm_vc(apply_bins(sub[fname], BIN_EDGES[fname]))
        psi_per_feature[fname] = calc_psi(TRAIN_FEAT_DIST[fname], dist_now) if not is_train else 0.0

    # PSI medio do grupo (media simples das features do grupo)
    psi_per_group = {}
    for g in set(grupo for _, grupo, _ in FEATURES):
        feats_in_group = [f for f, gg, _ in FEATURES if gg == g]
        psi_per_group[g] = float(np.mean([psi_per_feature[f] for f in feats_in_group]))

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
                'psi_within_group': safe_round(psi_per_group[grupo]),
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

print(f"\\nmetricas_metrics  : {len(metrics_rows)} linhas")
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

print("\\nEscrevendo tabelas:")
write_overwrite(metrics_rows,   schema_metrics,   'ds_catalog_dev.credit_engine.monitoring_metrics_me_br')
write_overwrite(band_rows,      schema_band,      'ds_catalog_dev.credit_engine.monitoring_band_me_br')
write_overwrite(feature_rows,   schema_features,  'ds_catalog_dev.credit_engine.monitoring_features_me_br')
write_overwrite(migration_rows, schema_migration, 'ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br')

print("\\nOK: 4 tabelas de metricas atualizadas.")"""))

# ============================================================
# Sanity checks
# ============================================================
CELLS.append(md("## Sanity checks"))

CELLS.append(code("""%sql
SELECT 'monitoring_me_br' AS tabela, COUNT(*) AS linhas, COUNT(DISTINCT reference_month) AS safras FROM ds_catalog_dev.credit_engine.monitoring_me_br
UNION ALL SELECT 'monitoring_metrics_me_br',         COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
UNION ALL SELECT 'monitoring_band_me_br',            COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
UNION ALL SELECT 'monitoring_features_me_br',        COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
UNION ALL SELECT 'monitoring_band_migrations_me_br', COUNT(*), COUNT(DISTINCT period) FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br"""))

CELLS.append(code("""%sql
-- Performance (KS/ROC/Gini) e PSI por safra OOT
SELECT period, reference_month,
       ROUND(ks,4)               AS ks,
       ROUND(roc_auc,4)          AS roc,
       ROUND(gini,4)             AS gini,
       ROUND(psi_vs_train,4)     AS psi_vs_train,
       psi_vs_train_classification,
       ROUND(bad_rate_overall*100,2) AS bad_rate_pct
FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
WHERE period <> 'Train'
ORDER BY reference_month DESC
LIMIT 24"""))

CELLS.append(code("""%sql
-- Distribuicao por banda na safra mais recente
SELECT period, band_name,
       ROUND(pct_pop,2)              AS pct_pop,
       ROUND(pct_pop_diff_pp,2)      AS pct_pop_diff_pp,
       ROUND(bad_rate,2)             AS bad_rate,
       ROUND(bad_rate_diff_pp,2)     AS bad_rate_diff_pp,
       ROUND(pct_bad_share,2)        AS pct_bad_share,
       ROUND(psi_contribution,4)     AS psi_contribution
FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_band_me_br WHERE period <> 'Train')
ORDER BY band"""))

CELLS.append(code("""%sql
-- Features com maior PSI na safra mais recente
SELECT period, feature_name, grupo,
       ROUND(MAX(psi_feature), 4)  AS psi,
       CASE WHEN MAX(psi_feature) < 0.10 THEN 'Estavel'
            WHEN MAX(psi_feature) < 0.25 THEN 'Moderado'
            ELSE 'Significativo' END AS classificacao
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_features_me_br WHERE period <> 'Train')
GROUP BY period, feature_name, grupo
ORDER BY psi DESC"""))

CELLS.append(code("""%sql
-- Triggers de risco relativo na safra mais recente
SELECT feature_name, grupo, bin_label, ROUND(lift,3) AS lift, lift_trigger, ROUND(volume,0) AS volume
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br
WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_features_me_br WHERE period <> 'Train')
  AND lift_trigger = 'true'
ORDER BY ABS(lift - 1) DESC
LIMIT 20"""))

CELLS.append(code("""%sql
-- Matriz de migracao da safra mais recente (heatmap)
SELECT previous_band, current_band, count, ROUND(pct_of_pop, 2) AS pct
FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br
WHERE period = (SELECT MAX(period) FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br)
ORDER BY previous_band, current_band"""))

# ============================================================
# Salvar
# ============================================================
notebook = {
    "cells": CELLS,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3.10"},
        "application/vnd.databricks.v1+notebook": {
            "notebookName": "04_monitoring_me_br_v2",
            "dashboards": [], "language": "python",
            "widgets": {}, "notebookOrigID": 0
        }
    },
    "nbformat": 4,
    "nbformat_minor": 5
}

# Garantir que source seja sempre lista de strings com \n
for cell in notebook["cells"]:
    src = cell["source"]
    if isinstance(src, str):
        cell["source"] = src.splitlines(keepends=True)
    # Adiciona \n no final de cada linha exceto a ultima, se nao tiver
    if isinstance(cell["source"], list) and len(cell["source"]) == 1 and "\n" in cell["source"][0]:
        # split multi-line single string
        cell["source"] = cell["source"][0].splitlines(keepends=True)

with open("/home/user/CreditoME/04_monitoring_me_br_v2.ipynb", "w") as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)

print(f"Notebook v2 criado: /home/user/CreditoME/04_monitoring_me_br_v2.ipynb")
print(f"Total de cells: {len(CELLS)}")
