# Databricks notebook source
# DBTITLE 0,Pipeline: Monitoring ME BR v7
# MAGIC %md
# MAGIC ## Pipeline: Tabela de Monitoramento do Modelo - ME BR v7
# MAGIC
# MAGIC Este notebook consolida todas as informacoes em **duas tabelas** para monitoramento do modelo de credito.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tabelas Produzidas
# MAGIC
# MAGIC | Tabela | Descricao | Comportamento de Ingestao |
# MAGIC |--------|-----------|---------------------------|
# MAGIC | `teste.monitoring_me_br` | Consolidacao por cliente x safra | **MISTO**: INSERT novos + UPDATE targets |
# MAGIC | `teste.monitoring_metrics_me_br` | Metricas agregadas por safra | **OVERWRITE**: recalculado a cada execucao |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Month Shift e Alinhamento Temporal
# MAGIC
# MAGIC As tabelas fonte operam em dois espacos temporais:
# MAGIC
# MAGIC | Tabela | reference_month | Espaco |
# MAGIC |--------|----------------|--------|
# MAGIC | `abt_inference_me_br` | m-1 | Dados historicos |
# MAGIC | `portfolio_abt_group_me_br` | m-1 | Medianas do portfolio |
# MAGIC | `apply_model_me_br` | m | Mes de decisao |
# MAGIC | `targets_me_br` | m | Resultado observado |
# MAGIC
# MAGIC A monitoring usa **reference_month = m** (mes de decisao) como eixo principal,
# MAGIC e inclui `feature_reference_month = m-1` para analises de estabilidade de features.
# MAGIC
# MAGIC **Joins**:
# MAGIC ```sql
# MAGIC apply_model <-> targets:    reference_month direto
# MAGIC apply_model <-> abt:        abt.reference_month = add_months(am.reference_month, -1)
# MAGIC apply_model <-> portfolio:  pag.reference_month = add_months(am.reference_month, -1)
# MAGIC ```
# MAGIC
# MAGIC **Uso das duas colunas temporais**:
# MAGIC - `reference_month`: PSI de scores, KS, Gini, performance do modelo
# MAGIC - `feature_reference_month`: PSI de features, drift detection, estabilidade
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tabela 1: monitoring_me_br (dados por cliente x safra)
# MAGIC
# MAGIC #### Comportamento: MISTO (INSERT + UPDATE Seletivo)
# MAGIC
# MAGIC | Origem | Colunas | Comportamento na Monitoring |
# MAGIC |--------|---------|-----------------------------|
# MAGIC | `abt_inference` | Features, score, historical_weight, reference_value, reference_value_clp, payment_term | **Imutavel** |
# MAGIC | `portfolio_abt_group` | Medianas, portfolio_score | **Imutavel** |
# MAGIC | `apply_model` | adjusted_score | **Imutavel** |
# MAGIC | `targets` | percent7mob, billed, overdue | **Atualizavel** — retroativo 24m |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tabela 2: monitoring_metrics_me_br (metricas por safra)
# MAGIC
# MAGIC Agrupa os dados de `monitoring_me_br` em **metricas de saude do modelo por reference_month**.
# MAGIC Recalculada a cada execucao (OVERWRITE) pois os targets atualizam retroativamente.
# MAGIC
# MAGIC #### Organizacao das Metricas
# MAGIC
# MAGIC | Grupo | Metricas | Disponibilidade |
# MAGIC |-------|----------|----------------|
# MAGIC | **Populacao** | total_clients, pop/pct por faixa (LOW/MEDIUM/HIGH) | Sempre |
# MAGIC | **Score** | avg_score, median_score | Sempre |
# MAGIC | **Limite USD** | avg_credit_limit, median_credit_limit, total_credit_limit | Sempre |
# MAGIC | **Limite CLP** | avg_credit_limit_clp, median_credit_limit_clp, total_credit_limit_clp | Sempre |
# MAGIC | **Financeiro** | total_billed_usd, overdue_usd, overdue_pct | Sempre |
# MAGIC | **Bad Rate** | bad_rate overall + por faixa | Quando target disponivel |
# MAGIC | **Lift** | lift por faixa | Quando target disponivel |
# MAGIC | **Discriminacao** | KS, ROC-AUC, Gini | Quando target tem ambas classes |
# MAGIC | **PSI** | psi_vs_base, psi_rolling + classificacao | Sempre |
# MAGIC | **Transicao** | pct melhorou/manteve/piorou | A partir da 2a safra |
# MAGIC
# MAGIC #### Configuracao (alinhada com monitoring_report)
# MAGIC - **Score**: `integrated_score`
# MAGIC - **Target**: `target_percent7mob1`
# MAGIC - **Faixas**: 1-BAIXO (<=0.02), 2-MEDIO (<=0.30), 3-ALTO (>0.30)
# MAGIC - **PSI base**: primeira safra disponivel
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pre-requisitos
# MAGIC
# MAGIC Os 3 notebooks anteriores devem ter sido executados com sucesso:
# MAGIC - **NB1**: popula `abt_inference` e `portfolio_abt_group`
# MAGIC - **NB2**: popula `apply_model`
# MAGIC - **NB3**: popula `targets`

# COMMAND ----------

# DBTITLE 0,Inicializacao: criar tabela destino se nao existir
# MAGIC %sql
# MAGIC -- Tabela consolidada para monitoramento de saude do modelo
# MAGIC -- Reune features (NB1), scores (NB1+NB2), portfolio (NB1) e targets (NB3)
# MAGIC -- Colunas de targets sao atualizadas retroativamente; features/scores sao imutaveis
# MAGIC DROP TABLE IF EXISTS teste.monitoring_me_br;
# MAGIC CREATE TABLE teste.monitoring_me_br (
# MAGIC   -- === Colunas comuns ===
# MAGIC   id_customer                    INT,
# MAGIC   customer_name                  STRING,
# MAGIC   country                        STRING,
# MAGIC   reference_month                DATE,        -- Mes de decisao (m)
# MAGIC   feature_reference_month        DATE,        -- Mes dos dados (m-1)
# MAGIC
# MAGIC   -- === Features (abt_inference_me_br) — imutaveis ===
# MAGIC   total_amount             DOUBLE,
# MAGIC   overdue_amount              DOUBLE,
# MAGIC   overdue_pct                DOUBLE,
# MAGIC   months_with_billing        INT,
# MAGIC   months_defaulted           INT,
# MAGIC   pct_months_overdue_10_20        DOUBLE,
# MAGIC   pct_months_overdue_20_30        DOUBLE,
# MAGIC   pct_months_overdue_30_50        DOUBLE,
# MAGIC   pct_months_overdue_50_plus        DOUBLE,
# MAGIC   flag_transacted            INT,
# MAGIC
# MAGIC   -- === Scores individuais (abt_inference_me_br) — imutaveis ===
# MAGIC   score                          DOUBLE,
# MAGIC   historical_weight              DOUBLE,
# MAGIC
# MAGIC   -- === Exposicao e prazo (abt_inference_me_br) — imutaveis ===
# MAGIC   reference_value                DOUBLE,      -- pico de exposicao 12m (USD)
# MAGIC   reference_value_clp            DOUBLE,      -- pico de exposicao 12m (CLP)
# MAGIC   payment_term                   DOUBLE,      -- prazo comercial medio (meses, clip [1,3])
# MAGIC
# MAGIC   -- === Scores portfolio (portfolio_abt_group_me_br) — imutaveis ===
# MAGIC   median_cluster_10           DOUBLE,
# MAGIC   median_cluster_20           DOUBLE,
# MAGIC   median_cluster_30           DOUBLE,
# MAGIC   median_cluster_50           DOUBLE,
# MAGIC   portfolio_score                DOUBLE,
# MAGIC
# MAGIC   -- === Score final (apply_model_me_br) — imutavel ===
# MAGIC   adjusted_score                 DOUBLE,
# MAGIC   score_band                     STRING,
# MAGIC   integrated_score               DOUBLE,
# MAGIC   integrated_score_band          STRING,
# MAGIC   credit_limit                   DOUBLE,
# MAGIC   credit_limit_clp               DOUBLE,
# MAGIC   credit_limit_end               DOUBLE,
# MAGIC   credit_limit_end_clp           DOUBLE,
# MAGIC
# MAGIC   -- === Targets (targets_me_br) — atualizacao retroativa ===
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
# MAGIC   target_overdue_pct_12m     DOUBLE,
# MAGIC
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# DBTITLE 0,Pre-check: validar que as 3 tabelas de entrada tem dados
# MAGIC %sql
# MAGIC -- Valida que as 4 tabelas de entrada tem dados antes de prosseguir
# MAGIC SELECT 
# MAGIC   'abt_inference_me_br' AS tabela,
# MAGIC   COUNT(*) AS total_linhas,
# MAGIC   CAST(MIN(reference_month) AS STRING) AS safra_min,
# MAGIC   CAST(MAX(reference_month) AS STRING) AS safra_max
# MAGIC FROM teste.abt_inference_me_br
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'portfolio_abt_group_me_br',
# MAGIC   COUNT(*),
# MAGIC   CAST(MIN(reference_month) AS STRING),
# MAGIC   CAST(MAX(reference_month) AS STRING)
# MAGIC FROM teste.portfolio_abt_group_me_br
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'apply_model_me_br',
# MAGIC   COUNT(*),
# MAGIC   CAST(MIN(reference_month) AS STRING),
# MAGIC   CAST(MAX(reference_month) AS STRING)
# MAGIC FROM teste.apply_model_me_br
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'targets_me_br',
# MAGIC   COUNT(*),
# MAGIC   CAST(MIN(reference_month) AS STRING),
# MAGIC   CAST(MAX(reference_month) AS STRING)
# MAGIC FROM teste.targets_me_br

# COMMAND ----------

# DBTITLE 0,Source view: join das 3 tabelas na janela de processamento
# MAGIC %sql
# MAGIC -- Base: apply_model (reference_month = m, mes de decisao)
# MAGIC -- Joins:
# MAGIC --   targets: direto em reference_month (ambos em espaco m)
# MAGIC --   abt_inference: shift -1 mes (abt esta em m-1)
# MAGIC --   portfolio_abt_group: shift -1 mes (portfolio esta em m-1)
# MAGIC -- feature_reference_month: preserva o mes original dos dados (m-1)
# MAGIC CREATE OR REPLACE TEMP VIEW monitoring_source AS
# MAGIC SELECT 
# MAGIC   -- Colunas comuns (de apply_model como tabela base)
# MAGIC   am.id_customer,
# MAGIC   am.customer_name,
# MAGIC   am.country,
# MAGIC   am.reference_month,
# MAGIC   add_months(am.reference_month, -1) AS feature_reference_month,
# MAGIC
# MAGIC   -- Features (abt_inference, espaco m-1)
# MAGIC   inf.total_amount,
# MAGIC   inf.overdue_amount,
# MAGIC   inf.overdue_pct,
# MAGIC   inf.months_with_billing,
# MAGIC   inf.months_defaulted,
# MAGIC   inf.pct_months_overdue_10_20,
# MAGIC   inf.pct_months_overdue_20_30,
# MAGIC   inf.pct_months_overdue_30_50,
# MAGIC   inf.pct_months_overdue_50_plus,
# MAGIC   inf.flag_transacted,
# MAGIC
# MAGIC   -- Scores individuais (abt_inference, espaco m-1)
# MAGIC   inf.score,
# MAGIC   inf.historical_weight,
# MAGIC
# MAGIC   -- Exposicao e prazo (abt_inference, espaco m-1) — imutaveis
# MAGIC   COALESCE(inf.reference_value, 0)     AS reference_value,
# MAGIC   COALESCE(inf.reference_value_clp, 0) AS reference_value_clp,
# MAGIC   COALESCE(inf.payment_term, 1.0)      AS payment_term,
# MAGIC
# MAGIC   -- Scores portfolio (portfolio_abt_group, espaco m-1)
# MAGIC   pag.median_cluster_10,
# MAGIC   pag.median_cluster_20,
# MAGIC   pag.median_cluster_30,
# MAGIC   pag.median_cluster_50,
# MAGIC   pag.portfolio_score,
# MAGIC
# MAGIC   -- Score final (apply_model, espaco m)
# MAGIC   am.adjusted_score,
# MAGIC   am.score_band,
# MAGIC   am.integrated_score,
# MAGIC   am.integrated_score_band,
# MAGIC   am.credit_limit,
# MAGIC   am.credit_limit_clp,
# MAGIC   am.credit_limit_end,
# MAGIC   am.credit_limit_end_clp,
# MAGIC
# MAGIC   -- Targets (backward-updating, espaco m) — prefixo target_
# MAGIC   t.percent7mob1       AS target_percent7mob1,
# MAGIC   t.percent7mob3       AS target_percent7mob3,
# MAGIC   t.percent7mob6       AS target_percent7mob6,
# MAGIC   t.percent7mob9       AS target_percent7mob9,
# MAGIC   t.percent7mob12      AS target_percent7mob12,
# MAGIC   t.billed_1m          AS target_billed_1m,
# MAGIC   t.overdue_1m         AS target_overdue_1m,
# MAGIC   t.overdue_pct_1m     AS target_overdue_pct_1m,
# MAGIC   t.billed_3m          AS target_billed_3m,
# MAGIC   t.overdue_3m         AS target_overdue_3m,
# MAGIC   t.overdue_pct_3m     AS target_overdue_pct_3m,
# MAGIC   t.billed_6m          AS target_billed_6m,
# MAGIC   t.overdue_6m         AS target_overdue_6m,
# MAGIC   t.overdue_pct_6m     AS target_overdue_pct_6m,
# MAGIC   t.billed_12m         AS target_billed_12m,
# MAGIC   t.overdue_12m        AS target_overdue_12m,
# MAGIC   t.overdue_pct_12m    AS target_overdue_pct_12m,
# MAGIC
# MAGIC   current_timestamp() AS updated_at
# MAGIC
# MAGIC FROM teste.apply_model_me_br AS am
# MAGIC LEFT JOIN teste.abt_inference_me_br AS inf
# MAGIC   ON inf.id_customer = am.id_customer
# MAGIC  AND inf.reference_month = add_months(am.reference_month, -1)
# MAGIC LEFT JOIN teste.portfolio_abt_group_me_br AS pag
# MAGIC   ON pag.reference_month = add_months(am.reference_month, -1)
# MAGIC LEFT JOIN teste.targets_me_br AS t
# MAGIC   ON t.id_customer = am.id_customer
# MAGIC  AND t.reference_month = am.reference_month
# MAGIC WHERE am.reference_month >= LEAST(
# MAGIC   add_months(date_trunc('month', current_date()), -24),
# MAGIC   COALESCE(
# MAGIC     (SELECT MAX(reference_month) FROM teste.monitoring_me_br),
# MAGIC     DATE '1900-01-01'
# MAGIC   )
# MAGIC )

# COMMAND ----------

# DBTITLE 0,MERGE: insere novos + atualiza apenas colunas de targets
# MAGIC %sql
# MAGIC -- Upsert com atualizacao seletiva:
# MAGIC --   NOT MATCHED: insere todas as colunas (features + scores + targets)
# MAGIC --   MATCHED: atualiza APENAS colunas de targets (backward-updating)
# MAGIC --            features, scores e feature_reference_month sao imutaveis
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

# MAGIC %sql
# MAGIC select * from monitoring_source

# COMMAND ----------

# DBTITLE 0,Sanity checks: validacao pos-processamento
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- SANITY CHECKS: monitoring_me_br
# MAGIC -- ============================================
# MAGIC SELECT 
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br) AS total_linhas,
# MAGIC   (SELECT MIN(reference_month) FROM teste.monitoring_me_br) AS safra_min,
# MAGIC   (SELECT MAX(reference_month) FROM teste.monitoring_me_br) AS safra_max,
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM teste.monitoring_me_br) AS total_safras,
# MAGIC   (SELECT COUNT(DISTINCT id_customer) FROM teste.monitoring_me_br) AS total_clientes,
# MAGIC   -- Duplicatas
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC     SELECT reference_month, id_customer, COUNT(*) AS cnt
# MAGIC     FROM teste.monitoring_me_br
# MAGIC     GROUP BY reference_month, id_customer HAVING cnt > 1
# MAGIC   )) AS duplicatas,
# MAGIC   -- Verificacao do month shift: monitoring.max deve = apply_model.max
# MAGIC   (SELECT MAX(reference_month) FROM teste.apply_model_me_br) AS apply_model_max,
# MAGIC   -- Verificacao feature_reference_month = reference_month - 1
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br
# MAGIC    WHERE feature_reference_month != add_months(reference_month, -1)
# MAGIC   ) AS feature_month_inconsistencias,
# MAGIC   -- Completude: linhas sem score (abt_inference faltou)
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br WHERE score IS NULL) AS linhas_sem_score,
# MAGIC   -- Completude: linhas sem adjusted_score (apply_model faltou)
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br WHERE adjusted_score IS NULL) AS linhas_sem_adjusted_score,
# MAGIC   -- Completude: linhas sem targets (esperado para safras recentes)
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_me_br WHERE target_percent7mob1 IS NULL AND target_percent7mob12 IS NULL) AS linhas_sem_targets

# COMMAND ----------

# DBTITLE 1,Secao 2: Metricas de Saude do Modelo
# MAGIC %md
# MAGIC ## Tabela 2: Metricas Agregadas de Saude do Modelo
# MAGIC
# MAGIC Calcula metricas por `reference_month` a partir de `monitoring_me_br`.
# MAGIC
# MAGIC **Comportamento**: OVERWRITE completo a cada execucao (targets atualizam retroativamente).
# MAGIC
# MAGIC **Grupos de metricas**:
# MAGIC 1. Populacao e distribuicao por faixa de risco
# MAGIC 2. Distribuicao de scores
# MAGIC 3. Limite de credito (avg, median, total)
# MAGIC 4. Volume financeiro (faturado e atraso em USD)
# MAGIC 5. Bad rate (geral e por faixa)
# MAGIC 6. Lift por faixa de risco
# MAGIC 7. Discriminacao: KS, ROC-AUC, Gini
# MAGIC 8. PSI vs base + rolling
# MAGIC 9. Transicao entre faixas (melhorou/manteve/piorou)
# MAGIC
# MAGIC **Configuracao** (alinhada com `monitoring_report_me_br`):
# MAGIC - Score: `integrated_score` | Target: `target_percent7mob1`
# MAGIC - Faixas: 1-BAIXO (<=0.02) | 2-MEDIO (<=0.30) | 3-ALTO (>0.30)
# MAGIC - PSI base: primeira safra disponivel

# COMMAND ----------

# DBTITLE 1,Inicializacao: criar tabela de metricas se nao existir
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS teste.monitoring_metrics_me_br;
# MAGIC CREATE TABLE teste.monitoring_metrics_me_br (
# MAGIC   reference_month                DATE,
# MAGIC
# MAGIC   -- === Populacao e distribuicao ===
# MAGIC   total_clients                  INT,
# MAGIC   pop_baixo                      INT,
# MAGIC   pop_medio                      INT,
# MAGIC   pop_alto                       INT,
# MAGIC   pct_baixo                      DOUBLE,
# MAGIC   pct_medio                      DOUBLE,
# MAGIC   pct_alto                       DOUBLE,
# MAGIC
# MAGIC   -- === Score ===
# MAGIC   avg_score                      DOUBLE,
# MAGIC   median_score                   DOUBLE,
# MAGIC
# MAGIC   -- === Limite de credito USD ===
# MAGIC   avg_credit_limit               DOUBLE,
# MAGIC   median_credit_limit            DOUBLE,
# MAGIC   total_credit_limit             DOUBLE,
# MAGIC
# MAGIC   -- === Limite de credito CLP ===
# MAGIC   avg_credit_limit_clp           DOUBLE,
# MAGIC   median_credit_limit_clp        DOUBLE,
# MAGIC   total_credit_limit_clp         DOUBLE,
# MAGIC
# MAGIC   -- === Financeiro ===
# MAGIC   total_billed_usd               DOUBLE,
# MAGIC   overdue_usd                    DOUBLE,
# MAGIC   overdue_pct                    DOUBLE,
# MAGIC
# MAGIC   -- === Bad rate ===
# MAGIC   bad_rate_overall               DOUBLE,
# MAGIC   bad_rate_baixo                 DOUBLE,
# MAGIC   bad_rate_medio                 DOUBLE,
# MAGIC   bad_rate_alto                  DOUBLE,
# MAGIC
# MAGIC   -- === Lift ===
# MAGIC   lift_baixo                     DOUBLE,
# MAGIC   lift_medio                     DOUBLE,
# MAGIC   lift_alto                      DOUBLE,
# MAGIC
# MAGIC   -- === Discriminacao ===
# MAGIC   ks                             DOUBLE,
# MAGIC   roc_auc                        DOUBLE,
# MAGIC   gini                           DOUBLE,
# MAGIC
# MAGIC   -- === PSI ===
# MAGIC   psi_vs_base                    DOUBLE,
# MAGIC   psi_vs_base_classification     STRING,
# MAGIC   psi_rolling                    DOUBLE,
# MAGIC   psi_rolling_classification     STRING,
# MAGIC
# MAGIC   -- === Transicao ===
# MAGIC   pct_improved                   DOUBLE,
# MAGIC   pct_maintained                 DOUBLE,
# MAGIC   pct_worsened                   DOUBLE,
# MAGIC
# MAGIC   updated_at                     TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Calcular metricas de saude do modelo por safra
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from scipy.stats import ks_2samp
from pyspark.sql.types import (
    StructType, StructField, DateType, IntegerType, DoubleType, StringType, TimestampType
)
from datetime import datetime

# ====== CONFIGURACAO (alinhada com monitoring_report_me_br) ======
TARGET_COL = 'target_percent7mob1'
SCORE_COL = 'integrated_score'
BAND_COL = 'integrated_score_band'
BAND_NAMES = ['1-BAIXO', '2-MEDIO', '3-ALTO']
BAND_MAP = {'1-BAIXO': 1, '2-MEDIO': 2, '3-ALTO': 3}

# ====== FUNCOES AUXILIARES ======
def calc_psi(base_dist, current_dist, epsilon=1e-4):
    base = np.array(base_dist, dtype=float) + epsilon
    curr = np.array(current_dist, dtype=float) + epsilon
    base = base / base.sum()
    curr = curr / curr.sum()
    return round(float(np.sum((curr - base) * np.log(curr / base))), 6)

def classify_psi(val):
    if val < 0.10: return 'Estavel'
    elif val < 0.25: return 'Moderado'
    return 'Significativo'

def safe_round(val, decimals=6):
    return round(val, decimals) if val is not None and not (isinstance(val, float) and np.isnan(val)) else None

# ====== CARGA DE DADOS ======
df = spark.sql("""
    SELECT id_customer, reference_month, integrated_score, integrated_score_band,
           total_amount, overdue_amount,
           credit_limit_end, credit_limit_end_clp,
           target_percent7mob1
    FROM teste.monitoring_me_br
    WHERE integrated_score IS NOT NULL
""").toPandas()

df = df.sort_values('reference_month')
months = sorted(df['reference_month'].unique())

print(f"Dados carregados: {len(df):,} registros, {len(months)} safras")
print(f"Range: {months[0]} a {months[-1]}")

# ====== DISTRIBUICOES PARA PSI ======
dist_map = {}
for m in months:
    vc = df[df['reference_month'] == m][BAND_COL].value_counts(normalize=True)
    dist_map[m] = [vc.get(b, 0) for b in BAND_NAMES]

base_dist = dist_map[months[0]]

# ====== CALCULO DE METRICAS POR SAFRA ======
rows = []
for i, m in enumerate(months):
    dm = df[df['reference_month'] == m]
    total = len(dm)

    # --- Populacao e distribuicao ---
    pop_baixo  = int((dm[BAND_COL] == '1-BAIXO').sum())
    pop_medio  = int((dm[BAND_COL] == '2-MEDIO').sum())
    pop_alto   = int((dm[BAND_COL] == '3-ALTO').sum())
    pct_baixo  = round(pop_baixo  / total * 100, 4) if total > 0 else None
    pct_medio  = round(pop_medio  / total * 100, 4) if total > 0 else None
    pct_alto   = round(pop_alto   / total * 100, 4) if total > 0 else None

    # --- Score ---
    avg_score = safe_round(dm[SCORE_COL].mean())
    median_score = safe_round(dm[SCORE_COL].median())

    # --- Limite de credito USD ---
    avg_credit_limit = safe_round(dm['credit_limit_end'].mean(), 2)
    median_credit_limit = safe_round(dm['credit_limit_end'].median(), 2)
    total_credit_limit = round(dm['credit_limit_end'].sum(), 2)

    # --- Limite de credito CLP ---
    avg_credit_limit_clp = safe_round(dm['credit_limit_end_clp'].mean(), 2)
    median_credit_limit_clp = safe_round(dm['credit_limit_end_clp'].median(), 2)
    total_credit_limit_clp = round(dm['credit_limit_end_clp'].sum(), 2)

    # --- Financeiro ---
    total_billed_usd = round(dm['total_amount'].sum(), 2)
    overdue_usd = round(dm['overdue_amount'].sum(), 2)
    overdue_pct = safe_round(overdue_usd / total_billed_usd * 100, 4) if total_billed_usd > 0 else None

    # --- Bad rate, Lift e Discriminacao (requer target) ---
    bad_rate_overall = bad_rate_baixo = bad_rate_medio = bad_rate_alto = None
    lift_baixo = lift_medio = lift_alto = None
    ks = roc_auc = gini_val = None

    dm_t = dm[dm[TARGET_COL].notna()].copy()
    if len(dm_t) > 0:
        dm_t[TARGET_COL] = dm_t[TARGET_COL].astype(int)
        y_true = dm_t[TARGET_COL].values
        y_score = dm_t[SCORE_COL].values

        bad_rate_overall = safe_round(y_true.mean())

        for band, attr in [('1-BAIXO', 'bad_rate_baixo'), ('2-MEDIO', 'bad_rate_medio'), ('3-ALTO', 'bad_rate_alto')]:
            mask = dm_t[BAND_COL] == band
            if mask.any():
                locals()[attr] = safe_round(dm_t.loc[mask, TARGET_COL].mean())

        # Lift
        if bad_rate_overall and bad_rate_overall > 0:
            lift_baixo = safe_round(bad_rate_baixo / bad_rate_overall, 4) if bad_rate_baixo is not None else None
            lift_medio = safe_round(bad_rate_medio / bad_rate_overall, 4) if bad_rate_medio is not None else None
            lift_alto  = safe_round(bad_rate_alto  / bad_rate_overall, 4) if bad_rate_alto  is not None else None

        # KS, AUC, Gini (precisa de ambas as classes)
        if y_true.sum() > 0 and y_true.sum() < len(y_true):
            good_scores = y_score[y_true == 0]
            bad_scores = y_score[y_true == 1]
            ks_stat, _ = ks_2samp(good_scores, bad_scores)
            ks = safe_round(ks_stat)
            auc_val = roc_auc_score(y_true, y_score)
            roc_auc = safe_round(auc_val)
            gini_val = safe_round(2 * auc_val - 1)

    # --- PSI ---
    psi_vs_base = calc_psi(base_dist, dist_map[m])
    psi_vs_base_class = classify_psi(psi_vs_base)
    psi_rolling = calc_psi(dist_map[months[i-1]], dist_map[m]) if i > 0 else None
    psi_rolling_class = classify_psi(psi_rolling) if psi_rolling is not None else None

    # --- Transicao (vs safra anterior) ---
    pct_improved = pct_maintained = pct_worsened = None
    if i > 0:
        prev_m = months[i - 1]
        df_prev = df[df['reference_month'] == prev_m][['id_customer', BAND_COL]].rename(
            columns={BAND_COL: 'prev_band'}
        )
        df_curr = dm[['id_customer', BAND_COL]].rename(
            columns={BAND_COL: 'curr_band'}
        )
        paired = df_prev.merge(df_curr, on='id_customer', how='inner')
        if len(paired) > 0:
            direction = paired['curr_band'].map(BAND_MAP) - paired['prev_band'].map(BAND_MAP)
            pct_improved = safe_round((direction < 0).mean() * 100, 4)
            pct_maintained = safe_round((direction == 0).mean() * 100, 4)
            pct_worsened = safe_round((direction > 0).mean() * 100, 4)

    rows.append({
        'reference_month': m,
        'total_clients': total,
        'pop_baixo': pop_baixo, 'pop_medio': pop_medio, 'pop_alto': pop_alto,
        'pct_baixo': pct_baixo, 'pct_medio': pct_medio, 'pct_alto': pct_alto,
        'avg_score': avg_score, 'median_score': median_score,
        'avg_credit_limit': avg_credit_limit, 'median_credit_limit': median_credit_limit, 'total_credit_limit': total_credit_limit,
        'avg_credit_limit_clp': avg_credit_limit_clp, 'median_credit_limit_clp': median_credit_limit_clp, 'total_credit_limit_clp': total_credit_limit_clp,
        'total_billed_usd': total_billed_usd, 'overdue_usd': overdue_usd, 'overdue_pct': overdue_pct,
        'bad_rate_overall': bad_rate_overall,
        'bad_rate_baixo': bad_rate_baixo, 'bad_rate_medio': bad_rate_medio, 'bad_rate_alto': bad_rate_alto,
        'lift_baixo': lift_baixo, 'lift_medio': lift_medio, 'lift_alto': lift_alto,
        'ks': ks, 'roc_auc': roc_auc, 'gini': gini_val,
        'psi_vs_base': psi_vs_base, 'psi_vs_base_classification': psi_vs_base_class,
        'psi_rolling': psi_rolling, 'psi_rolling_classification': psi_rolling_class,
        'pct_improved': pct_improved, 'pct_maintained': pct_maintained, 'pct_worsened': pct_worsened,
    })

df_metrics = pd.DataFrame(rows)
df_metrics['updated_at'] = datetime.now()

print(f"\nMetricas calculadas: {len(df_metrics)} safras")
print(f"Safras com bad_rate: {df_metrics['bad_rate_overall'].notna().sum()}")
print(f"Safras com KS/AUC/Gini: {df_metrics['ks'].notna().sum()}")
print(f"Safras com PSI rolling: {df_metrics['psi_rolling'].notna().sum()}")
print(f"Safras com transicao: {df_metrics['pct_improved'].notna().sum()}")

# ====== ESCRITA (OVERWRITE) ======
schema = StructType([
    StructField('reference_month', DateType()),
    StructField('total_clients', IntegerType()),
    StructField('pop_baixo', IntegerType()),
    StructField('pop_medio', IntegerType()),
    StructField('pop_alto', IntegerType()),
    StructField('pct_baixo', DoubleType()),
    StructField('pct_medio', DoubleType()),
    StructField('pct_alto', DoubleType()),
    StructField('avg_score', DoubleType()),
    StructField('median_score', DoubleType()),
    StructField('avg_credit_limit', DoubleType()),
    StructField('median_credit_limit', DoubleType()),
    StructField('total_credit_limit', DoubleType()),
    StructField('avg_credit_limit_clp', DoubleType()),
    StructField('median_credit_limit_clp', DoubleType()),
    StructField('total_credit_limit_clp', DoubleType()),
    StructField('total_billed_usd', DoubleType()),
    StructField('overdue_usd', DoubleType()),
    StructField('overdue_pct', DoubleType()),
    StructField('bad_rate_overall', DoubleType()),
    StructField('bad_rate_baixo', DoubleType()),
    StructField('bad_rate_medio', DoubleType()),
    StructField('bad_rate_alto', DoubleType()),
    StructField('lift_baixo', DoubleType()),
    StructField('lift_medio', DoubleType()),
    StructField('lift_alto', DoubleType()),
    StructField('ks', DoubleType()),
    StructField('roc_auc', DoubleType()),
    StructField('gini', DoubleType()),
    StructField('psi_vs_base', DoubleType()),
    StructField('psi_vs_base_classification', StringType()),
    StructField('psi_rolling', DoubleType()),
    StructField('psi_rolling_classification', StringType()),
    StructField('pct_improved', DoubleType()),
    StructField('pct_maintained', DoubleType()),
    StructField('pct_worsened', DoubleType()),
    StructField('updated_at', TimestampType()),
])

sdf = spark.createDataFrame(df_metrics, schema=schema)
sdf.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('teste.monitoring_metrics_me_br')

print(f"\nTabela teste.monitoring_metrics_me_br gravada com {len(df_metrics)} linhas (OVERWRITE)")

# COMMAND ----------

# DBTITLE 1,Sanity checks: monitoring_metrics_me_br_v7
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- SANITY CHECKS: monitoring_metrics_me_br
# MAGIC -- ============================================
# MAGIC SELECT 
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_metrics_me_br) AS total_safras,
# MAGIC   (SELECT MIN(reference_month) FROM teste.monitoring_metrics_me_br) AS safra_min,
# MAGIC   (SELECT MAX(reference_month) FROM teste.monitoring_metrics_me_br) AS safra_max,
# MAGIC   -- Verificar que todas as safras de monitoring estao presentes
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM teste.monitoring_me_br
# MAGIC    WHERE integrated_score IS NOT NULL
# MAGIC   ) AS expected_safras,
# MAGIC   -- Metricas de discriminacao disponiveis
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_metrics_me_br WHERE ks IS NOT NULL) AS safras_com_ks,
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_metrics_me_br WHERE bad_rate_overall IS NOT NULL) AS safras_com_bad_rate,
# MAGIC   -- PSI ranges
# MAGIC   (SELECT ROUND(MAX(psi_vs_base), 4) FROM teste.monitoring_metrics_me_br) AS max_psi_vs_base,
# MAGIC   (SELECT SUM(CASE WHEN psi_vs_base_classification = 'Significativo' THEN 1 ELSE 0 END) FROM teste.monitoring_metrics_me_br) AS safras_psi_significativo,
# MAGIC   -- Transicao
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_metrics_me_br WHERE pct_improved IS NOT NULL) AS safras_com_transicao,
# MAGIC   -- Consistencia: pct_baixo + pct_medio + pct_alto ~ 100
# MAGIC   (SELECT COUNT(*) FROM teste.monitoring_metrics_me_br
# MAGIC    WHERE ABS(pct_baixo + pct_medio + pct_alto - 100.0) > 0.01
# MAGIC   ) AS inconsistencias_pct_faixas