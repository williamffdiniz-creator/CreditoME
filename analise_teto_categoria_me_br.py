# Databricks notebook source
# DBTITLE 0,Análise: Impacto do Teto por Categoria — ME BR
# MAGIC %md
# MAGIC ## Análise: Impacto do Teto por Categoria — ME BR
# MAGIC
# MAGIC Objetivo: responder 3 perguntas sobre a safra **2026-04-01**:
# MAGIC
# MAGIC 1. Quantos e quais clientes tiveram o limite **capado** pelo teto de categoria (`credit_limit_end < credit_limit`)?
# MAGIC 2. Por categoria: quantos clientes caem no teto e qual o percentual sobre `customer_count`?
# MAGIC 3. Qual o **impacto financeiro total** da carteira — quanto de crédito está sendo dado a menos?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Lógica de join
# MAGIC
# MAGIC ```
# MAGIC apply_model_me_br          (reference_month = 2026-04-01)
# MAGIC   ↕ id_customer = customer_code
# MAGIC customer_top_category_me_br (reference_quarter = 2026-04-01)  → top_category do cliente
# MAGIC   ↕ top_category = category AND reference_quarter
# MAGIC category_limit_me_br        (reference_quarter = 2026-04-01)  → category_limit_usd (teto)
# MAGIC ```
# MAGIC
# MAGIC **Nerfado** = `credit_limit_end < credit_limit` (o teto de categoria foi menor que o limite calculado pelo modelo)
# MAGIC
# MAGIC **Sem cap** = `credit_limit_end = credit_limit` (limite do modelo já estava abaixo do teto — categoria não restringiu)

# COMMAND ----------

# DBTITLE 0,Parâmetro da safra
# Ajuste aqui se quiser analisar outra safra
SAFRA = '2026-05-01'
print(f"Safra de análise: {SAFRA}")

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW config_analise AS
  SELECT CAST('{SAFRA}' AS DATE) AS safra
""")

# COMMAND ----------

# DBTITLE 0,1. Visão geral da carteira — capados vs sem cap
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- BLOCO 1: Visão consolidada da carteira na safra
# MAGIC -- Responde: quantos usam o limite calculado vs limite de categoria
# MAGIC -- e qual o impacto financeiro agregado (USD)
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   reference_month,
# MAGIC
# MAGIC   -- Universo total
# MAGIC   COUNT(*)                                                         AS total_clientes,
# MAGIC   COUNT(DISTINCT id_customer_group_economic)                       AS total_grupos_economicos,
# MAGIC
# MAGIC   -- Clientes capados pelo teto de categoria
# MAGIC   COUNT(CASE WHEN credit_limit_end < credit_limit THEN 1 END)     AS clientes_capados_pela_categoria,
# MAGIC   ROUND(
# MAGIC     COUNT(CASE WHEN credit_limit_end < credit_limit THEN 1 END)
# MAGIC     / COUNT(*) * 100, 1
# MAGIC   )                                                                AS pct_clientes_capados,
# MAGIC
# MAGIC   -- Clientes cujo limite calculado já era menor que o teto (sem impacto)
# MAGIC   COUNT(CASE WHEN credit_limit_end = credit_limit THEN 1 END)     AS clientes_sem_cap,
# MAGIC   ROUND(
# MAGIC     COUNT(CASE WHEN credit_limit_end = credit_limit THEN 1 END)
# MAGIC     / COUNT(*) * 100, 1
# MAGIC   )                                                                AS pct_sem_cap,
# MAGIC
# MAGIC   -- Clientes sem categoria registrada (credit_limit_end = credit_limit por COALESCE)
# MAGIC   -- Identificados por ausência de join com customer_top_category — aqui inferido via limite
# MAGIC   COUNT(CASE WHEN credit_limit_end IS NULL THEN 1 END)            AS clientes_sem_credit_limit_end,
# MAGIC
# MAGIC   -- Impacto financeiro: soma das reduções (USD)
# MAGIC   ROUND(SUM(GREATEST(credit_limit - credit_limit_end, 0)), 2)     AS reducao_total_usd,
# MAGIC
# MAGIC   -- Carteira total: crédito aprovado vs crédito disponibilizado
# MAGIC   ROUND(SUM(credit_limit), 2)                                     AS carteira_limite_calculado_usd,
# MAGIC   ROUND(SUM(credit_limit_end), 2)                                 AS carteira_limite_final_usd,
# MAGIC   ROUND(
# MAGIC     (SUM(credit_limit) - SUM(credit_limit_end))
# MAGIC     / SUM(credit_limit) * 100, 2
# MAGIC   )                                                                AS pct_reducao_carteira,
# MAGIC
# MAGIC   -- Médias por cliente
# MAGIC   ROUND(AVG(credit_limit),     2)                                 AS avg_limite_calculado_usd,
# MAGIC   ROUND(AVG(credit_limit_end), 2)                                 AS avg_limite_final_usd
# MAGIC
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC WHERE reference_month = (SELECT safra FROM config_analise)
# MAGIC GROUP BY reference_month

# COMMAND ----------

# DBTITLE 0,2. category_limit_me_br enriquecida — clientes no teto e % por categoria
# MAGIC %sql
# MAGIC WITH
# MAGIC
# MAGIC clientes_com_top_cat AS (
# MAGIC   SELECT
# MAGIC     am.id_customer,
# MAGIC     am.credit_limit,
# MAGIC     am.credit_limit_end,
# MAGIC     ROUND(am.credit_limit - am.credit_limit_end, 2) AS reducao_usd,
# MAGIC     CASE WHEN am.credit_limit_end < am.credit_limit THEN 'CAPADO' ELSE 'SEM_CAP' END AS status_cap,
# MAGIC     ctc.top_category
# MAGIC   FROM ds_catalog_dev.credit_engine.apply_model_me_br am
# MAGIC   LEFT JOIN ds_catalog_dev.credit_engine.customer_top_category_me_br ctc
# MAGIC     ON  am.id_customer     = ctc.customer_code
# MAGIC     AND am.reference_month = ctc.reference_quarter
# MAGIC   WHERE am.reference_month = (SELECT safra FROM config_analise)
# MAGIC     AND am.credit_limit_end IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC -- Denominador correto: clientes com top_category = categoria na mesma safra
# MAGIC -- Denominador correto: clientes scored nesta safra com top_category = categoria
# MAGIC universo_por_cat AS (
# MAGIC   SELECT
# MAGIC     top_category AS category,
# MAGIC     COUNT(DISTINCT id_customer) AS clientes_top_cat
# MAGIC   FROM clientes_com_top_cat  -- <- vem da apply_model, não da customer_top_category diretamente
# MAGIC   GROUP BY top_category
# MAGIC ),
# MAGIC
# MAGIC nerfados_por_cat AS (
# MAGIC   SELECT
# MAGIC     top_category AS category,
# MAGIC     COUNT(CASE WHEN status_cap = 'CAPADO' THEN 1 END) AS clientes_no_teto,
# MAGIC     ROUND(SUM(CASE WHEN status_cap = 'CAPADO' THEN reducao_usd ELSE 0 END), 2) AS reducao_total_categoria_usd
# MAGIC   FROM clientes_com_top_cat
# MAGIC   GROUP BY top_category
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   cl.reference_quarter,
# MAGIC   cl.category,
# MAGIC   cl.order_count,
# MAGIC   cl.customer_count                               AS customer_count_3m,  -- clientes que compraram na cat. nos últimos 3 meses
# MAGIC   u.clientes_top_cat,                                                     -- clientes cuja top_category é esta (24m) — denominador correto
# MAGIC   ROUND(cl.total_amount_usd, 2)                   AS total_amount_usd,
# MAGIC   ROUND(cl.pct_total * 100, 2)                    AS pct_total_pct,
# MAGIC   cl.pct_limit,
# MAGIC   ROUND(cl.category_limit_usd, 2)                 AS category_limit_usd,
# MAGIC
# MAGIC   -- COLUNAS NOVAS
# MAGIC   COALESCE(nc.clientes_no_teto, 0)                AS clientes_no_teto,
# MAGIC   ROUND(
# MAGIC     COALESCE(nc.clientes_no_teto, 0) / NULLIF(u.clientes_top_cat, 0) * 100,
# MAGIC   1)                                              AS pct_clientes_no_teto,  -- % dos que têm top_cat=X que foram capados
# MAGIC   COALESCE(nc.reducao_total_categoria_usd, 0)     AS reducao_total_categoria_usd
# MAGIC
# MAGIC FROM ds_catalog_dev.credit_engine.category_limit_me_br cl
# MAGIC LEFT JOIN nerfados_por_cat nc   ON cl.category = nc.category
# MAGIC LEFT JOIN universo_por_cat u    ON cl.category = u.category
# MAGIC WHERE cl.reference_quarter = (SELECT safra FROM config_analise)
# MAGIC ORDER BY clientes_no_teto DESC, cl.category_limit_usd DESC

# COMMAND ----------

# DBTITLE 0,3. Clientes capados — detalhe individual com top_category
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- BLOCO 3: Clientes nerfados com top_category e teto aplicado
# MAGIC -- Extensão da query de amostra do NB02b — mostra a categoria responsável
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   am.id_customer,
# MAGIC   am.customer_name,
# MAGIC   am.reference_month,
# MAGIC   am.score_band,
# MAGIC   ctc.top_category,
# MAGIC   ROUND(cl.category_limit_usd, 2)              AS teto_categoria_usd,
# MAGIC   ROUND(am.credit_limit, 2)                    AS credit_limit_calculado,
# MAGIC   ROUND(am.credit_limit_end, 2)                AS credit_limit_final,
# MAGIC   ROUND(am.credit_limit - am.credit_limit_end, 2) AS reducao_usd,
# MAGIC   ROUND((am.credit_limit - am.credit_limit_end)
# MAGIC         / am.credit_limit * 100, 1)            AS pct_reducao,
# MAGIC   CASE
# MAGIC     WHEN am.limit_mi_usd IS NOT NULL THEN 'MI+ME'
# MAGIC     ELSE 'Apenas-ME'
# MAGIC   END                                          AS tipo_cliente
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br am
# MAGIC LEFT JOIN ds_catalog_dev.credit_engine.customer_top_category_me_br ctc
# MAGIC   ON  am.id_customer     = ctc.customer_code
# MAGIC   AND am.reference_month = ctc.reference_quarter
# MAGIC LEFT JOIN ds_catalog_dev.credit_engine.category_limit_me_br cl
# MAGIC   ON  ctc.top_category      = cl.category
# MAGIC   AND ctc.reference_quarter = cl.reference_quarter
# MAGIC WHERE am.credit_limit_end < am.credit_limit
# MAGIC   AND am.reference_month = (SELECT safra FROM config_analise)
# MAGIC ORDER BY reducao_usd DESC

# COMMAND ----------

# DBTITLE 0,4. Impacto por score_band — quanto de crédito cada faixa perde
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- BLOCO 4: Redução de crédito por score_band
# MAGIC -- Responde: o cap de categoria afeta mais clientes de alto ou baixo risco?
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   score_band,
# MAGIC   COUNT(*)                                                          AS total_clientes_faixa,
# MAGIC   COUNT(CASE WHEN credit_limit_end < credit_limit THEN 1 END)      AS capados,
# MAGIC   ROUND(
# MAGIC     COUNT(CASE WHEN credit_limit_end < credit_limit THEN 1 END)
# MAGIC     / COUNT(*) * 100, 1
# MAGIC   )                                                                 AS pct_capados_na_faixa,
# MAGIC   ROUND(SUM(credit_limit), 2)                                       AS carteira_calculada_usd,
# MAGIC   ROUND(SUM(credit_limit_end), 2)                                   AS carteira_final_usd,
# MAGIC   ROUND(SUM(credit_limit) - SUM(credit_limit_end), 2)              AS reducao_usd,
# MAGIC   ROUND(AVG(CASE WHEN credit_limit_end < credit_limit
# MAGIC              THEN credit_limit - credit_limit_end END), 2)         AS avg_reducao_por_capado_usd
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC WHERE reference_month = (SELECT safra FROM config_analise)
# MAGIC   AND credit_limit_end IS NOT NULL
# MAGIC GROUP BY score_band
# MAGIC ORDER BY score_band

# COMMAND ----------

# DBTITLE 0,5. Clientes sem categoria registrada — não sujeitos ao teto
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- BLOCO 5: Clientes em apply_model_me_br sem match em
# MAGIC -- customer_top_category_me_br na mesma safra.
# MAGIC -- Para esses, o COALESCE no NB02b faz credit_limit_end = credit_limit
# MAGIC -- (nenhuma restrição aplicada, mesmo que existisse teto disponível)
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC   COUNT(*)                                                          AS total_sem_top_category,
# MAGIC   ROUND(SUM(am.credit_limit), 2)                                   AS carteira_sem_categoria_usd,
# MAGIC   ROUND(AVG(am.credit_limit), 2)                                   AS avg_limite_sem_categoria_usd
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br am
# MAGIC LEFT JOIN ds_catalog_dev.credit_engine.customer_top_category_me_br ctc
# MAGIC   ON  am.id_customer     = ctc.customer_code
# MAGIC   AND am.reference_month = ctc.reference_quarter
# MAGIC WHERE am.reference_month = (SELECT safra FROM config_analise)
# MAGIC   AND ctc.customer_code IS NULL