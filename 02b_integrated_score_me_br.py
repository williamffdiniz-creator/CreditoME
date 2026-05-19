# Databricks notebook source
# DBTITLE 1,Score Integrado ME BR + Teto por Categoria
# MAGIC %md
# MAGIC ## 02b: Score Integrado ME BR (MI x ME) + Limite Final por Categoria
# MAGIC
# MAGIC Este notebook executa **dois passos sequenciais** sobre `apply_model_me_br`:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Passo 1 — Score Integrado (MI + ME)
# MAGIC
# MAGIC Consome `ds_catalog_dev.credit_engine.integrated_score_chile` (produzida pelo NB02b do pipeline Chile)
# MAGIC e atualiza 9 colunas nos clientes presentes em ambos os mercados.
# MAGIC Clientes apenas-ME nao sao tocados neste passo.
# MAGIC
# MAGIC | Coluna atualizada | Valor | Fonte |
# MAGIC |-------------------|-------|-------|
# MAGIC | `integrated_score` | Score integrado (mi_share > 70% → MI, senao ME) | `integrated_score_chile` |
# MAGIC | `integrated_score_band` | Banda do score integrado | `integrated_score_chile` |
# MAGIC | `credit_limit` | `limit_mi_usd + limit_me_usd` (soma dos mercados) | `integrated_score_chile` |
# MAGIC | `credit_limit_clp` | `limit_mi_clp + limit_me_clp` (soma dos mercados) | `integrated_score_chile` |
# MAGIC | `adjusted_score_mi` | Score MI do cliente espelhado | `integrated_score_chile` |
# MAGIC | `limit_mi_usd` | Limite MI em USD | `integrated_score_chile` |
# MAGIC | `limit_mi_clp` | Limite MI em CLP | `integrated_score_chile` |
# MAGIC | `id_customer_mi` | cod_pessoa MI do cliente espelhado | `integrated_score_chile` |
# MAGIC | `market_scope` | `'MI_CHILE'` (indica cliente integrado MI+ME) | Fixo |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Passo 2 — Teto por Categoria (credit_limit_end)
# MAGIC
# MAGIC Aplica o teto de categoria sobre o `credit_limit` de **todos os clientes**
# MAGIC (independente de serem MI+ME ou apenas-ME). Executa depois do Passo 1 para
# MAGIC garantir que o `credit_limit` dos clientes compartilhados ja reflete a soma
# MAGIC dos dois mercados antes da comparacao com o teto.
# MAGIC
# MAGIC **Alinhamento temporal:** join direto `apply_model.reference_month = customer_top_category.reference_quarter`.
# MAGIC Apesar do nome `reference_quarter`, a coluna e mensal com janela movel m+1,
# MAGIC identica ao espaco de `apply_model.reference_month`. Sem necessidade de DATE_TRUNC.
# MAGIC
# MAGIC **Logica do teto por Grupo Economico:**
# MAGIC
# MAGIC ```
# MAGIC Para cada GE, avalia-se a categoria de todos os membros:
# MAGIC   teto_categoria_ge_usd = MAX(category_limit_usd) entre todos os membros do GE
# MAGIC   (o membro com a categoria de maior teto define o limite para todo o grupo)
# MAGIC
# MAGIC credit_limit_end     = LEAST(credit_limit,     teto_categoria_ge_usd)
# MAGIC credit_limit_end_clp = LEAST(credit_limit_clp, teto_categoria_ge_usd * taxa_clp_usd)
# MAGIC
# MAGIC Se o cliente nao tem categoria registrada: credit_limit_end = credit_limit (sem cap)
# MAGIC ```
# MAGIC
# MAGIC **Colunas atualizadas no Passo 2:**
# MAGIC
# MAGIC | Coluna | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `credit_limit_end` | Limite final USD: `LEAST(credit_limit, teto_categoria_ge_usd)` |
# MAGIC | `credit_limit_end_clp` | Limite final CLP: `LEAST(credit_limit_clp, teto_categoria_ge_usd * taxa)` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Ordem de execucao obrigatoria
# MAGIC
# MAGIC ```
# MAGIC Pipeline Chile:  NB01 → NB02 (apply_model_mi_chile) → NB02b (integrated_score_chile)
# MAGIC Pipeline ME BR:  NB01 → NB01b (category_limit) → NB02 (apply_model_me_br) → NB02b (este)
# MAGIC ```
# MAGIC
# MAGIC ### Dependencias
# MAGIC
# MAGIC - `ds_catalog_dev.credit_engine.integrated_score_chile` (NB02b Chile) — scores e limites MI+ME
# MAGIC - `ds_catalog_dev.credit_engine.apply_model_me_br` (NB02 ME BR) — tabela destino de ambos os MERGEs
# MAGIC - `ds_catalog_dev.credit_engine.customer_top_category_me_br` (NB01b) — categoria principal por cliente/trimestre
# MAGIC - `ds_catalog_dev.credit_engine.category_limit_me_br` (NB01b) — teto de limite por categoria/trimestre
# MAGIC - `de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial` — taxa CLP/USD para conversao do teto

# COMMAND ----------

# DBTITLE 1,Parametros do pipeline
from datetime import date

dbutils.widgets.text('data_referencia', '', 'Data de Referencia')
data_referencia = dbutils.widgets.get('data_referencia')

effective_date = data_referencia if data_referencia else str(date.today())
print(f'Data de referencia: {effective_date}')

spark.sql(f"CREATE OR REPLACE TEMP VIEW config_pipeline AS SELECT CAST('{effective_date}' AS DATE) AS data_referencia")

# COMMAND ----------

# DBTITLE 1,Pre-check: validar dependencias
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.integrated_score_chile)                     AS isc_total_linhas,
# MAGIC   (SELECT CAST(MAX(reference_month) AS STRING)
# MAGIC    FROM ds_catalog_dev.credit_engine.integrated_score_chile)                                     AS isc_safra_max,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br)                          AS me_total_linhas,
# MAGIC   (SELECT CAST(MAX(reference_month) AS STRING)
# MAGIC    FROM ds_catalog_dev.credit_engine.apply_model_me_br)                                          AS me_safra_max,
# MAGIC   -- Clientes em comum esperados no Passo 1
# MAGIC   (SELECT COUNT(DISTINCT isc.id_customer_me)
# MAGIC    FROM ds_catalog_dev.credit_engine.integrated_score_chile isc
# MAGIC    INNER JOIN ds_catalog_dev.credit_engine.apply_model_me_br me
# MAGIC      ON isc.id_customer_me = me.id_customer
# MAGIC     AND isc.reference_month = me.reference_month)                         AS clientes_passo1_atualizar,
# MAGIC   -- Disponibilidade dos dados de categoria
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.customer_top_category_me_br)                AS ctc_total_linhas,
# MAGIC   (SELECT CAST(MAX(reference_quarter) AS STRING)
# MAGIC    FROM ds_catalog_dev.credit_engine.customer_top_category_me_br)                                AS ctc_trimestre_max,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.category_limit_me_br)                       AS cl_total_linhas,
# MAGIC   (SELECT CAST(MAX(reference_quarter) AS STRING)
# MAGIC    FROM ds_catalog_dev.credit_engine.category_limit_me_br)                                       AS cl_trimestre_max

# COMMAND ----------

# DBTITLE 1,PASSO 1 — MERGE: score integrado MI+ME
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- MERGE PASSO 1: atualizar apply_model_me_br com scores integrados
# MAGIC -- ====================================================================
# MAGIC -- Fonte: integrated_score_chile (calculada pelo NB02b do pipeline Chile)
# MAGIC -- Apenas clientes com presenca em ambos os mercados sao atualizados.
# MAGIC -- Clientes apenas-ME nao sao tocados: mantem integrated_score = adjusted_score.
# MAGIC -- Idempotente: re-execucoes sobrescrevem com os mesmos valores.
# MAGIC -- ====================================================================
# MAGIC MERGE INTO ds_catalog_dev.credit_engine.apply_model_me_br AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     id_customer_mi,
# MAGIC     id_customer_me,
# MAGIC     reference_month,
# MAGIC     integrated_score,
# MAGIC     integrated_score_band,
# MAGIC     ROUND(credit_limit, 4) AS credit_limit_combined,
# MAGIC     ROUND(credit_limit_clp, 4) AS credit_limit_clp_combined,
# MAGIC     adjusted_score_mi,
# MAGIC     limit_mi_usd,
# MAGIC     limit_mi_clp
# MAGIC   FROM ds_catalog_dev.credit_engine.integrated_score_chile
# MAGIC ) AS source
# MAGIC ON target.id_customer     = source.id_customer_me
# MAGIC    AND target.reference_month = source.reference_month
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.integrated_score      = source.integrated_score,
# MAGIC   target.integrated_score_band = source.integrated_score_band,
# MAGIC   target.credit_limit          = source.credit_limit_combined,
# MAGIC   target.credit_limit_clp      = source.credit_limit_clp_combined,
# MAGIC   target.adjusted_score_mi     = source.adjusted_score_mi,
# MAGIC   target.limit_mi_usd          = source.limit_mi_usd,
# MAGIC   target.limit_mi_clp          = source.limit_mi_clp,
# MAGIC   target.id_customer_mi        = source.id_customer_mi,
# MAGIC   target.market_scope          = 'MI_CHILE',
# MAGIC   target.updated_at            = current_timestamp()

# COMMAND ----------

# DBTITLE 1,Taxa CLP/USD (forward-fill) — para converter teto categoria em CLP
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- TAXA CLP/USD DO DIA
# MAGIC -- ====================================================================
# MAGIC -- Necessaria para converter teto_categoria_ge_usd em CLP ao calcular
# MAGIC -- credit_limit_end_clp = LEAST(credit_limit_clp, teto_usd * taxa_clp_usd)
# MAGIC -- Mesma logica de forward-fill do NB02 (de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial)
# MAGIC -- ====================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW view_taxa_clp_hoje AS
# MAGIC WITH raw AS (
# MAGIC   SELECT
# MAGIC     CAST(valido_desde AS DATE) AS valido_desde,
# MAGIC     CAST(valido_ate   AS DATE) AS valido_ate,
# MAGIC     TRY_CAST(REPLACE(REPLACE(taxa_cambio, '/', ''), ',', '.') AS FLOAT) AS taxa_cambio
# MAGIC   FROM de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial
# MAGIC   WHERE moeda_procedencia = 'CLP'
# MAGIC     AND moeda_destino     = 'USD'
# MAGIC     AND taxa_cambio IS NOT NULL
# MAGIC     AND CAST(valido_desde AS DATE) <= CAST(valido_ate AS DATE)
# MAGIC ),
# MAGIC exploded AS (
# MAGIC   SELECT CAST(exploded_date AS DATE) AS data_taxa, taxa_cambio
# MAGIC   FROM raw
# MAGIC   LATERAL VIEW EXPLODE(SEQUENCE(valido_desde, valido_ate, INTERVAL 1 DAY)) t AS exploded_date
# MAGIC ),
# MAGIC date_spine AS (
# MAGIC   SELECT CAST(d AS DATE) AS data_taxa
# MAGIC   FROM (
# MAGIC     SELECT EXPLODE(SEQUENCE(
# MAGIC       (SELECT MIN(valido_desde) FROM raw),
# MAGIC       current_date(),
# MAGIC       INTERVAL 1 DAY
# MAGIC     )) AS d
# MAGIC   )
# MAGIC ),
# MAGIC joined AS (
# MAGIC   SELECT ds.data_taxa, e.taxa_cambio,
# MAGIC     COUNT(e.taxa_cambio) OVER (
# MAGIC       ORDER BY ds.data_taxa ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS grp
# MAGIC   FROM date_spine ds
# MAGIC   LEFT JOIN exploded e ON e.data_taxa = ds.data_taxa
# MAGIC ),
# MAGIC filled AS (
# MAGIC   SELECT data_taxa,
# MAGIC     FIRST_VALUE(taxa_cambio) OVER (PARTITION BY grp ORDER BY data_taxa) AS taxa_cambio
# MAGIC   FROM joined
# MAGIC )
# MAGIC SELECT taxa_cambio AS taxa_clp_usd
# MAGIC FROM filled
# MAGIC WHERE data_taxa = (SELECT MAX(data_taxa) FROM filled WHERE taxa_cambio IS NOT NULL)
# MAGIC LIMIT 1

# COMMAND ----------

# DBTITLE 1,Teto por categoria: preparar view com teto do GE
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- TETO POR CATEGORIA — LOGICA DO GRUPO ECONOMICO
# MAGIC -- ====================================================================
# MAGIC --
# MAGIC -- Alinhamento temporal:
# MAGIC --   apply_model.reference_month e customer_top_category.reference_quarter
# MAGIC --   estao no mesmo espaco mensal (janela movel m+1). O nome "reference_quarter"
# MAGIC --   e apenas o nome da coluna na tabela de origem — o join e direto por igualdade,
# MAGIC --   sem DATE_TRUNC nem conversao de granularidade.
# MAGIC --
# MAGIC -- Logica do GE:
# MAGIC --   Cada membro do GE tem sua top_category e, consequentemente, seu category_limit_usd.
# MAGIC --   O teto do GE = MAX(category_limit_usd) entre todos os membros para o mesmo mes.
# MAGIC --   Ou seja: o membro com a categoria de maior limite "puxa" o teto para todo o grupo.
# MAGIC --
# MAGIC -- Se o cliente nao tem categoria registrada: teto = NULL → credit_limit_end = credit_limit
# MAGIC -- ====================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW category_teto_ge AS
# MAGIC WITH
# MAGIC
# MAGIC -- Categoria + limite por cliente/trimestre
# MAGIC -- customer_code em customer_top_category_me_br = id_customer (cod_pessoa)
# MAGIC customer_category AS (
# MAGIC   SELECT
# MAGIC     ctc.customer_code                     AS id_customer,
# MAGIC     ctc.id_customer_group_economic,
# MAGIC     ctc.reference_quarter,
# MAGIC     ctc.top_category,
# MAGIC     cl.category_limit_usd
# MAGIC   FROM ds_catalog_dev.credit_engine.customer_top_category_me_br ctc
# MAGIC   LEFT JOIN ds_catalog_dev.credit_engine.category_limit_me_br cl
# MAGIC     ON  ctc.top_category      = cl.category
# MAGIC     AND ctc.reference_quarter = cl.reference_quarter
# MAGIC ),
# MAGIC
# MAGIC -- Teto do GE: MAX(category_limit_usd) entre todos os membros do mesmo GE/trimestre
# MAGIC -- "avalia-se qual membro tem maior valor de categoria — esse define o limite do grupo"
# MAGIC ge_teto AS (
# MAGIC   SELECT
# MAGIC     id_customer_group_economic,
# MAGIC     reference_quarter,
# MAGIC     MAX(category_limit_usd) AS teto_categoria_ge_usd
# MAGIC   FROM customer_category
# MAGIC   GROUP BY id_customer_group_economic, reference_quarter
# MAGIC )
# MAGIC
# MAGIC -- Junta o teto do GE de volta para cada cliente individual
# MAGIC -- (cada membro recebe o teto calculado para o seu grupo)
# MAGIC SELECT
# MAGIC   cc.id_customer,
# MAGIC   cc.reference_quarter,
# MAGIC   gt.teto_categoria_ge_usd
# MAGIC FROM customer_category cc
# MAGIC INNER JOIN ge_teto gt
# MAGIC   ON  cc.id_customer_group_economic = gt.id_customer_group_economic
# MAGIC   AND cc.reference_quarter          = gt.reference_quarter

# COMMAND ----------

# DBTITLE 1,PASSO 2 — MERGE: credit_limit_end (teto categoria)
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- MERGE PASSO 2: credit_limit_end para TODOS os clientes
# MAGIC -- ====================================================================
# MAGIC -- Executa sobre toda a apply_model_me_br (nao apenas clientes MI+ME).
# MAGIC -- O credit_limit ja reflete a soma MI+ME para clientes compartilhados
# MAGIC -- (Passo 1 ja rodou), entao o LEAST e aplicado sobre o valor correto.
# MAGIC --
# MAGIC -- Regra:
# MAGIC --   credit_limit_end     = LEAST(credit_limit,     teto_categoria_ge_usd)
# MAGIC --   credit_limit_end_clp = LEAST(credit_limit_clp, teto_categoria_ge_usd * taxa_clp_usd)
# MAGIC --
# MAGIC -- Se cliente nao tem categoria (LEFT JOIN retorna NULL):
# MAGIC --   COALESCE(teto, credit_limit) = credit_limit → sem cap → credit_limit_end = credit_limit
# MAGIC --
# MAGIC -- Idempotente: re-execucoes recalculam com os mesmos valores.
# MAGIC -- ====================================================================
# MAGIC MERGE INTO ds_catalog_dev.credit_engine.apply_model_me_br AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     am.id_customer,
# MAGIC     am.reference_month,
# MAGIC     -- USD: menor entre o limite calculado e o teto da categoria do GE
# MAGIC     ROUND(
# MAGIC       LEAST(
# MAGIC         am.credit_limit,
# MAGIC         COALESCE(ct.teto_categoria_ge_usd, am.credit_limit)
# MAGIC       ),
# MAGIC     4) AS credit_limit_end,
# MAGIC     -- CLP: converte o teto USD para CLP e aplica o mesmo LEAST
# MAGIC     ROUND(
# MAGIC       LEAST(
# MAGIC         am.credit_limit_clp,
# MAGIC         COALESCE(ct.teto_categoria_ge_usd * taxa.taxa_clp_usd, am.credit_limit_clp)
# MAGIC       ),
# MAGIC     4) AS credit_limit_end_clp
# MAGIC   FROM ds_catalog_dev.credit_engine.apply_model_me_br am
# MAGIC   -- Join direto: reference_month = reference_quarter (mesmo espaco mensal m+1)
# MAGIC   LEFT JOIN category_teto_ge ct
# MAGIC     ON  am.id_customer        = ct.id_customer
# MAGIC     AND am.reference_month    = ct.reference_quarter
# MAGIC   CROSS JOIN view_taxa_clp_hoje taxa
# MAGIC ) AS source
# MAGIC ON target.id_customer     = source.id_customer
# MAGIC    AND target.reference_month = source.reference_month
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.credit_limit_end       = source.credit_limit_end,
# MAGIC   target.credit_limit_end_clp   = source.credit_limit_end_clp,
# MAGIC   target.updated_at             = current_timestamp()

# COMMAND ----------

# DBTITLE 1,Sanity checks: validacao pos-execucao
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- SANITY CHECKS: apply_model_me_br apos NB02b (Passos 1 e 2)
# MAGIC -- ====================================================================
# MAGIC SELECT
# MAGIC   -- Volume
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br)                          AS total_linhas,
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)   AS total_safras,
# MAGIC   (SELECT CAST(MAX(reference_month) AS STRING)
# MAGIC    FROM ds_catalog_dev.credit_engine.apply_model_me_br)                                          AS safra_max,
# MAGIC
# MAGIC   -- PASSO 1: clientes MI+ME vs apenas-ME
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE limit_mi_usd IS NOT NULL)                                        AS clientes_mi_me,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE limit_mi_usd IS NULL)                                            AS clientes_apenas_me,
# MAGIC
# MAGIC   -- PASSO 1: qualidade do integrated_score
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE integrated_score IS NULL)                                        AS nulls_integrated_score,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE integrated_score < 0 OR integrated_score > 1)                   AS integrated_score_out_of_range,
# MAGIC
# MAGIC   -- PASSO 2: cobertura de credit_limit_end
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE credit_limit_end IS NOT NULL)                                    AS clientes_com_credit_limit_end,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE credit_limit_end IS NULL)                                        AS clientes_sem_credit_limit_end,
# MAGIC
# MAGIC   -- PASSO 2: quantos tiveram o limite capado pela categoria
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE credit_limit_end < credit_limit
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br))
# MAGIC                                                                           AS clientes_capados_ultima_safra,
# MAGIC   -- PASSO 2: quantos nao foram afetados pelo cap (limite ja abaixo do teto)
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE credit_limit_end = credit_limit
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br))
# MAGIC                                                                           AS clientes_abaixo_teto_ultima_safra,
# MAGIC
# MAGIC   -- Medias de limite para validacao de magnitude
# MAGIC   (SELECT ROUND(AVG(credit_limit), 2) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br))
# MAGIC                                                                           AS avg_credit_limit,
# MAGIC   (SELECT ROUND(AVG(credit_limit_end), 2) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE credit_limit_end IS NOT NULL
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br))
# MAGIC                                                                           AS avg_credit_limit_end,
# MAGIC
# MAGIC   -- Alinhamento com integrated_score_chile (Passo 1)
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.integrated_score_chile
# MAGIC    WHERE reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.integrated_score_chile))
# MAGIC                                                                           AS isc_clientes_ultima_safra,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE limit_mi_usd IS NOT NULL
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br))
# MAGIC                                                                           AS me_clientes_mi_me_ultima_safra,
# MAGIC
# MAGIC   -- PASSO 1: market_scope e id_customer_mi
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE market_scope = 'MI_CHILE')                                       AS clientes_market_scope_mi_chile,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE id_customer_mi IS NOT NULL)                                      AS clientes_com_id_customer_mi,
# MAGIC   -- Consistencia: market_scope e id_customer_mi devem ser preenchidos juntos
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE (market_scope IS NOT NULL AND id_customer_mi IS NULL)
# MAGIC       OR (market_scope IS NULL AND id_customer_mi IS NOT NULL))           AS inconsistencias_market_scope

# COMMAND ----------

# DBTITLE 1,Inspecao: amostra de clientes capados por categoria
# MAGIC %sql
# MAGIC -- Amostra para validacao manual: clientes onde o teto de categoria
# MAGIC -- efetivamente reduziu o limite (credit_limit_end < credit_limit)
# MAGIC SELECT
# MAGIC   id_customer,
# MAGIC   customer_name,
# MAGIC   reference_month,
# MAGIC   score_band,
# MAGIC   ROUND(credit_limit, 2)                    AS credit_limit_antes,
# MAGIC   ROUND(credit_limit_end, 2)                AS credit_limit_end,
# MAGIC   ROUND(credit_limit - credit_limit_end, 2) AS reducao_usd,
# MAGIC   CASE WHEN limit_mi_usd IS NOT NULL THEN 'MI+ME' ELSE 'Apenas-ME' END AS tipo_cliente,
# MAGIC   market_scope,
# MAGIC   id_customer_mi
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC WHERE credit_limit_end < credit_limit
# MAGIC   AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)
# MAGIC ORDER BY reducao_usd DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ds_catalog_dev.credit_engine.apply_model_me_br