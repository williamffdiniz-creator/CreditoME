# Databricks notebook source
# DBTITLE 0,Pipeline: Apply Model ME BR v4
# MAGIC %md
# MAGIC ## Apply Model — ME BR v4 (Grupo Economico + Score Integrado + Teto Categoria)
# MAGIC
# MAGIC `adjusted_score = C × score_individual + (1 - C) × portfolio_score`
# MAGIC
# MAGIC Month shift: `apply_model.reference_month = abt.reference_month + 1 mes`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Colunas e responsabilidades por notebook
# MAGIC
# MAGIC | Coluna | Default (NB02) | Quem atualiza |
# MAGIC |--------|----------------|---------------|
# MAGIC | `rut` | Extraido de `dbpessoa_tab_pessoa.num_registro_comercial` | Este notebook |
# MAGIC | `adjusted_score` | `C*score + (1-C)*portfolio_score` | Este notebook |
# MAGIC | `score_band` | Faixa individual (1-BAIXO/2-MEDIO/3-ALTO) | Este notebook |
# MAGIC | `credit_limit` | Limite do grupo economico (USD) | Este notebook → NB02b soma MI+ME |
# MAGIC | `credit_limit_clp` | Limite do grupo economico (CLP) | Este notebook → NB02b soma MI+ME |
# MAGIC | `integrated_score` | `adjusted_score` | NB02b sobrescreve para clientes MI+ME |
# MAGIC | `integrated_score_band` | `score_band` | NB02b sobrescreve para clientes MI+ME |
# MAGIC | `adjusted_score_mi` | NULL | NB02b preenche para clientes MI+ME |
# MAGIC | `limit_mi_usd` | NULL | NB02b preenche para clientes MI+ME |
# MAGIC | `limit_mi_clp` | NULL | NB02b preenche para clientes MI+ME |
# MAGIC | `credit_limit_end` | NULL | NB02b: `LEAST(credit_limit, teto_categoria_ge_usd)` |
# MAGIC | `credit_limit_end_clp` | NULL | NB02b: `LEAST(credit_limit_clp, teto_categoria_ge_usd * taxa)` |
# MAGIC | `id_customer_mi` | NULL | NB02b preenche com cod_pessoa MI para clientes integrados MI+ME |
# MAGIC | `market_scope` | NULL | NB02b preenche com 'MI_CHILE' para clientes integrados MI+ME |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Grupo Economico
# MAGIC
# MAGIC - `reference_value_ge` = **SOMA** de `reference_value` de todos os membros do GE na mesma safra
# MAGIC - `adjusted_score_ge` = **MAX** do grupo (pior risco domina)
# MAGIC - `payment_term_ge` = **MAX** do grupo (mais conservador)
# MAGIC
# MAGIC ### Multiplicadores por faixa (sobre o pior score do GE)
# MAGIC
# MAGIC | Faixa (grupo) | Threshold | Formula |
# MAGIC |---------------|-----------|---------|
# MAGIC | 1-BAIXO | adjusted_score_ge <= 0.02 | reference_value_ge × payment_term_ge × 1.2 |
# MAGIC | 2-MEDIO | adjusted_score_ge <= 0.30 | reference_value_ge × payment_term_ge × 1.1 |
# MAGIC | 3-ALTO  | adjusted_score_ge > 0.30  | reference_value_ge × payment_term_ge × 0.9 |
# MAGIC
# MAGIC ### Fator de Suavizacao — Teto Global do Limite (aplicado neste notebook)
# MAGIC
# MAGIC `teto_global_ge = reference_value_ge × smoothing_factor_ge`
# MAGIC
# MAGIC - `smoothing_factor` por membro = **5.0** (default; diferenciavel por banda de risco quando necessario)
# MAGIC - `smoothing_factor_ge` do GE = **MIN** entre membros → captura a maior inadimplencia do grupo (menor multiplicador = maior penalizacao)
# MAGIC - Aplicado **antes** do teto por categoria (NB02b), pois e o teto global absoluto do cliente/grupo
# MAGIC - Regra: multiplicador proximo de 0 penaliza | proximo de 1 (ou superior) bonifica
# MAGIC
# MAGIC `credit_limit_ge = LEAST(rv_ge × pt_ge × band_mult, rv_ge × smoothing_factor_ge)`
# MAGIC
# MAGIC ### Teto por Categoria (calculado no NB02b)
# MAGIC
# MAGIC ```
# MAGIC teto_categoria_ge_usd = MAX(category_limit_usd) entre membros do GE na mesma referencia mensal
# MAGIC credit_limit_end      = LEAST(credit_limit,     teto_categoria_ge_usd)
# MAGIC credit_limit_end_clp  = LEAST(credit_limit_clp, teto_categoria_ge_usd x taxa_clp_usd)
# MAGIC ```
# MAGIC
# MAGIC ### Dependencias
# MAGIC
# MAGIC - `ds_catalog_dev.credit_engine.abt_inference_me_br` (NB1) — features e scores individuais
# MAGIC - `ds_catalog_dev.credit_engine.portfolio_abt_group_me_br` (NB1) — portfolio_score
# MAGIC - `de_data_lake_prd.dados_mestres.dbpessoa_tab_pessoa` — RUT (num_registro_comercial)
# MAGIC - `de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial` — taxa CLP/USD
# MAGIC - `ds_catalog_dev.credit_engine.customer_top_category_me_br` (NB1b) — categoria principal por cliente/mes
# MAGIC - `ds_catalog_dev.credit_engine.category_limit_me_br` (NB1b) — teto de limite por categoria/mes
# MAGIC - NB02b (`integrated_score_me_br`) atualiza colunas MI, integrated, credit_limit_end, id_customer_mi e market_scope

# COMMAND ----------

# DBTITLE 0,Parametros do pipeline
from datetime import date

dbutils.widgets.text("data_referencia", "", "Data de Referencia")
data_referencia = dbutils.widgets.get("data_referencia")

effective_date = data_referencia if data_referencia else str(date.today())
spark.sql(f"CREATE OR REPLACE TEMP VIEW config_pipeline AS SELECT CAST('{effective_date}' AS DATE) AS data_referencia")
print(f"Data de referencia do pipeline: {effective_date}")

# COMMAND ----------

# DBTITLE 1,UDF: normalizar_rut
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION normalizar_rut(rut_str STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN
# MAGIC   UPPER(
# MAGIC     TRIM(
# MAGIC       REGEXP_REPLACE(
# MAGIC         REGEXP_REPLACE(
# MAGIC           REGEXP_REPLACE(
# MAGIC             REGEXP_REPLACE(
# MAGIC               UPPER(TRIM(rut_str)),
# MAGIC               '[^0-9K]',
# MAGIC               ''
# MAGIC             ),
# MAGIC             '[A-Z]+(?=\\d)',
# MAGIC             ''
# MAGIC           ),
# MAGIC           '[A-Z]+(?=\\d)',
# MAGIC           ''
# MAGIC         ),
# MAGIC         '\\s+',
# MAGIC         ''
# MAGIC       )
# MAGIC     )
# MAGIC   )

# COMMAND ----------

# DBTITLE 0,Inicializacao: criar tabela destino se nao existir
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ds_catalog_dev.credit_engine.apply_model_me_br (
# MAGIC   id_customer                  INT,
# MAGIC   rut                          STRING     COMMENT 'RUT normalizado via dbpessoa_tab_pessoa.num_registro_comercial',
# MAGIC   customer_name                STRING,
# MAGIC   country                      STRING,
# MAGIC   reference_month              DATE       COMMENT 'Mes de vigencia (abt.ref + 1)',
# MAGIC   adjusted_score               DOUBLE     COMMENT 'C*score + (1-C)*portfolio_score',
# MAGIC   score_band                   STRING     COMMENT '1-BAIXO (<=0.02), 2-MEDIO (<=0.3), 3-ALTO (>0.3)',
# MAGIC   integrated_score             DOUBLE     COMMENT 'Default = adjusted_score. NB02b sobrescreve para clientes MI+ME',
# MAGIC   integrated_score_band        STRING     COMMENT 'Default = score_band. NB02b sobrescreve para clientes MI+ME',
# MAGIC   credit_limit                 DOUBLE     COMMENT 'Limite do GE em USD antes do teto categoria. NB02b soma MI+ME para compartilhados',
# MAGIC   credit_limit_clp             DOUBLE     COMMENT 'Limite do GE em CLP antes do teto categoria. NB02b soma MI+ME para compartilhados',
# MAGIC   adjusted_score_mi            DOUBLE     COMMENT 'Score MI do cliente espelhado (NULL se apenas ME). NB02b preenche',
# MAGIC   limit_mi_usd                 DOUBLE     COMMENT 'Limite MI em USD (NULL se apenas ME). NB02b preenche',
# MAGIC   limit_mi_clp                 DOUBLE     COMMENT 'Limite MI em CLP (NULL se apenas ME). NB02b preenche',
# MAGIC   credit_limit_end             DOUBLE     COMMENT 'Limite final USD apos teto de categoria do GE. NB02b preenche',
# MAGIC   credit_limit_end_clp         DOUBLE     COMMENT 'Limite final CLP apos teto de categoria do GE. NB02b preenche',
# MAGIC   id_customer_mi               INT        COMMENT 'cod_pessoa MI do cliente espelhado (NULL se apenas ME). NB02b preenche',
# MAGIC   market_scope                 STRING     COMMENT 'Escopo de mercado: NULL=apenas ME, MI_CHILE=integrado MI+ME. NB02b preenche',
# MAGIC   id_customer_group_economic   INT        COMMENT 'ID do grupo economico (cod_pessoa_empresa_pai ou id_customer)',
# MAGIC   updated_at                   TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 0,Pre-check: validar que abt_inference_me_br tem dados
# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN cnt = 0 THEN 'ERRO: abt_inference_me_br esta vazia. Execute o notebook 01 primeiro.'
# MAGIC     ELSE CONCAT('OK: abt_inference_me_br tem ', cnt, ' linhas, safras de ', min_safra, ' ate ', max_safra)
# MAGIC   END AS status_pre_check
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     COUNT(*)                             AS cnt,
# MAGIC     CAST(MIN(reference_month) AS STRING) AS min_safra,
# MAGIC     CAST(MAX(reference_month) AS STRING) AS max_safra
# MAGIC   FROM ds_catalog_dev.credit_engine.abt_inference_me_br
# MAGIC )

# COMMAND ----------

# DBTITLE 0,Controle incremental
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW controle_processamento AS
# MAGIC SELECT
# MAGIC   MAX(reference_month)  AS ultima_safra_processada,
# MAGIC   current_timestamp()   AS data_atualizacao
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br

# COMMAND ----------

# DBTITLE 1,Taxa CLP/USD (forward-fill)
# MAGIC %sql
# MAGIC -- Taxa CLP/USD utilizada na conversao do limite.
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
# MAGIC       ORDER BY ds.data_taxa
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
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

# DBTITLE 1,Scores e limites: score_base + grupo_economico + rut_lookup
# MAGIC %sql
# MAGIC -- ====================================================================
# MAGIC -- MODEL SCORES — ME BR v4
# MAGIC -- ====================================================================
# MAGIC -- Calcula adjusted_score, score_band e credit_limit (USD + CLP) para
# MAGIC -- cada cliente/safra, aplicando a logica de Grupo Economico.
# MAGIC --
# MAGIC -- Colunas de credit_limit_end chegam NULL aqui e
# MAGIC -- sao preenchidas pelo NB02b apos os merges de score integrado.
# MAGIC -- ====================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW model_scores AS
# MAGIC WITH
# MAGIC
# MAGIC -- RUT NORMALIZADO (fonte ME: dbpessoa_tab_pessoa.num_registro_comercial)
# MAGIC rut_ranked AS (
# MAGIC   SELECT
# MAGIC     tp.cod_pessoa AS id_customer,
# MAGIC     normalizar_rut(tp.num_registro_comercial) AS rut,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY tp.cod_pessoa
# MAGIC       ORDER BY COALESCE(am.max_ref, DATE '1900-01-01') DESC, tp.num_registro_comercial
# MAGIC     ) AS rn
# MAGIC   FROM de_data_lake_prd.dados_mestres.dbpessoa_tab_pessoa tp
# MAGIC   LEFT JOIN (
# MAGIC     SELECT id_customer, MAX(reference_month) AS max_ref
# MAGIC     FROM ds_catalog_dev.credit_engine.abt_inference_me_br
# MAGIC     GROUP BY id_customer
# MAGIC   ) am ON tp.cod_pessoa = am.id_customer
# MAGIC   WHERE tp.num_registro_comercial IS NOT NULL
# MAGIC     AND TRIM(tp.num_registro_comercial) != ''
# MAGIC ),
# MAGIC rut_lookup AS (
# MAGIC   SELECT id_customer, rut FROM rut_ranked WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- SCORE BASE: adjusted_score = C * score + (1-C) * portfolio_score
# MAGIC score_base AS (
# MAGIC   SELECT
# MAGIC     inf.id_customer,
# MAGIC     inf.customer_name,
# MAGIC     inf.country,
# MAGIC     add_months(inf.reference_month, 1)                            AS reference_month,
# MAGIC     ROUND(
# MAGIC       inf.historical_weight * inf.score
# MAGIC       + (1.0 - inf.historical_weight) * COALESCE(pag.portfolio_score, 0.30),
# MAGIC     6)                                                            AS adjusted_score,
# MAGIC     COALESCE(inf.reference_value,     0)                         AS reference_value,
# MAGIC     COALESCE(inf.reference_value_clp, 0)                         AS reference_value_clp,
# MAGIC     COALESCE(inf.payment_term,        1.0)                       AS payment_term,
# MAGIC     COALESCE(inf.id_customer_group_economic, inf.id_customer)    AS id_customer_group_economic,
# MAGIC     5.0                                                           AS smoothing_factor
# MAGIC   FROM ds_catalog_dev.credit_engine.abt_inference_me_br AS inf
# MAGIC   LEFT JOIN ds_catalog_dev.credit_engine.portfolio_abt_group_me_br AS pag
# MAGIC     ON inf.reference_month = pag.reference_month
# MAGIC   WHERE inf.reference_month > add_months(
# MAGIC     COALESCE(
# MAGIC       (SELECT ultima_safra_processada FROM controle_processamento),
# MAGIC       DATE '1900-01-01'
# MAGIC     ),
# MAGIC     -1
# MAGIC   )
# MAGIC ),
# MAGIC
# MAGIC -- GRUPO ECONOMICO
# MAGIC -- reference_value_ge    = SOMA de exposicao do grupo/safra
# MAGIC -- adjusted_score_ge     = MAX do grupo (pior risco domina)
# MAGIC -- payment_term_ge       = MAX do grupo (mais conservador)
# MAGIC -- smoothing_factor_ge   = MIN do grupo (maior inadimplencia = menor fator = maior penalizacao)
# MAGIC -- Multiplicadores: 1-BAIXO *1.2 | 2-MEDIO *1.1 | 3-ALTO *0.9 (sobre payment_term_ge)
# MAGIC -- Teto global: credit_limit_ge = LEAST(formula_banda, rv_ge * smoothing_factor_ge)
# MAGIC grupo_economico_agg AS (
# MAGIC   SELECT
# MAGIC     s.id_customer_group_economic,
# MAGIC     s.reference_month,
# MAGIC     SUM(s.reference_value)     AS reference_value_ge,
# MAGIC     SUM(s.reference_value_clp) AS reference_value_clp_ge,
# MAGIC     MAX(s.adjusted_score)      AS adjusted_score_ge,
# MAGIC     MAX(s.payment_term)        AS payment_term_ge,
# MAGIC     MIN(s.smoothing_factor)    AS smoothing_factor_ge,
# MAGIC     CASE
# MAGIC       WHEN MAX(s.adjusted_score) <= 0.02 THEN '1-BAIXO'
# MAGIC       WHEN MAX(s.adjusted_score) <= 0.30 THEN '2-MEDIO'
# MAGIC       ELSE '3-ALTO'
# MAGIC     END AS score_band_ge,
# MAGIC     -- Limite USD por grupo/safra: formula de banda limitada pelo teto global de suavizacao
# MAGIC     LEAST(
# MAGIC       ROUND(
# MAGIC         SUM(s.reference_value) * MAX(s.payment_term) *
# MAGIC         CASE
# MAGIC           WHEN MAX(s.adjusted_score) <= 0.02 THEN 1.2
# MAGIC           WHEN MAX(s.adjusted_score) <= 0.30 THEN 1.1
# MAGIC           ELSE 0.9
# MAGIC         END
# MAGIC       , 4),
# MAGIC       ROUND(SUM(s.reference_value) * MIN(s.smoothing_factor), 4)
# MAGIC     ) AS credit_limit_ge,
# MAGIC     -- Limite CLP por grupo/safra: formula de banda limitada pelo teto global de suavizacao
# MAGIC     LEAST(
# MAGIC       ROUND(
# MAGIC         SUM(s.reference_value_clp) * MAX(s.payment_term) *
# MAGIC         CASE
# MAGIC           WHEN MAX(s.adjusted_score) <= 0.02 THEN 1.2
# MAGIC           WHEN MAX(s.adjusted_score) <= 0.30 THEN 1.1
# MAGIC           ELSE 0.9
# MAGIC         END
# MAGIC       , 4),
# MAGIC       ROUND(SUM(s.reference_value_clp) * MIN(s.smoothing_factor), 4)
# MAGIC     ) AS credit_limit_clp_ge
# MAGIC   FROM score_base s
# MAGIC   GROUP BY s.id_customer_group_economic, s.reference_month
# MAGIC )
# MAGIC
# MAGIC -- SELECT FINAL
# MAGIC -- Colunas de teto categoria chegam NULL: NB02b preenche apos ambos os merges
# MAGIC SELECT
# MAGIC   s.id_customer,
# MAGIC   rl.rut,
# MAGIC   s.customer_name,
# MAGIC   s.country,
# MAGIC   s.reference_month,
# MAGIC   s.adjusted_score,
# MAGIC   CASE
# MAGIC     WHEN s.adjusted_score <= 0.02 THEN '1-BAIXO'
# MAGIC     WHEN s.adjusted_score <= 0.30 THEN '2-MEDIO'
# MAGIC     ELSE '3-ALTO'
# MAGIC   END                             AS score_band,
# MAGIC   s.adjusted_score               AS integrated_score,
# MAGIC   CASE
# MAGIC     WHEN s.adjusted_score <= 0.02 THEN '1-BAIXO'
# MAGIC     WHEN s.adjusted_score <= 0.30 THEN '2-MEDIO'
# MAGIC     ELSE '3-ALTO'
# MAGIC   END                             AS integrated_score_band,
# MAGIC   ge.credit_limit_ge             AS credit_limit,
# MAGIC   ge.credit_limit_clp_ge         AS credit_limit_clp,
# MAGIC   CAST(NULL AS DOUBLE)           AS adjusted_score_mi,
# MAGIC   CAST(NULL AS DOUBLE)           AS limit_mi_usd,
# MAGIC   CAST(NULL AS DOUBLE)           AS limit_mi_clp,
# MAGIC   CAST(NULL AS DOUBLE)           AS credit_limit_end,
# MAGIC   CAST(NULL AS DOUBLE)           AS credit_limit_end_clp,
# MAGIC   CAST(NULL AS INT)              AS id_customer_mi,
# MAGIC   CAST(NULL AS STRING)           AS market_scope,
# MAGIC   s.id_customer_group_economic
# MAGIC FROM score_base AS s
# MAGIC LEFT JOIN grupo_economico_agg AS ge
# MAGIC   ON  ge.id_customer_group_economic = s.id_customer_group_economic
# MAGIC   AND ge.reference_month            = s.reference_month
# MAGIC LEFT JOIN rut_lookup rl
# MAGIC   ON rl.id_customer = s.id_customer

# COMMAND ----------

# DBTITLE 1,MERGE incremental: apply_model_me_br
# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.credit_engine.apply_model_me_br AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     id_customer,
# MAGIC     rut,
# MAGIC     customer_name,
# MAGIC     country,
# MAGIC     reference_month,
# MAGIC     adjusted_score,
# MAGIC     score_band,
# MAGIC     integrated_score,
# MAGIC     integrated_score_band,
# MAGIC     credit_limit,
# MAGIC     credit_limit_clp,
# MAGIC     adjusted_score_mi,
# MAGIC     limit_mi_usd,
# MAGIC     limit_mi_clp,
# MAGIC     credit_limit_end,
# MAGIC     credit_limit_end_clp,
# MAGIC     id_customer_mi,
# MAGIC     market_scope,
# MAGIC     id_customer_group_economic,
# MAGIC     (SELECT data_atualizacao FROM controle_processamento) AS updated_at
# MAGIC   FROM model_scores
# MAGIC ) AS source
# MAGIC ON target.reference_month = source.reference_month
# MAGIC    AND target.id_customer  = source.id_customer
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 1,Sanity checks: validacao pos-processamento
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br)                          AS total_linhas,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE updated_at = (SELECT MAX(updated_at) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC                        WHERE updated_at IS NOT NULL))                      AS linhas_ultima_execucao,
# MAGIC   (SELECT MIN(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)              AS safra_min,
# MAGIC   (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)              AS safra_max,
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)   AS total_safras,
# MAGIC   (SELECT COUNT(DISTINCT id_customer) FROM ds_catalog_dev.credit_engine.apply_model_me_br)       AS total_clientes,
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC     SELECT reference_month, id_customer, COUNT(*) AS cnt
# MAGIC     FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC     GROUP BY reference_month, id_customer HAVING cnt > 1
# MAGIC   ))                                                                       AS duplicatas,
# MAGIC   (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.abt_inference_me_br)            AS abt_max_safra,
# MAGIC   (SELECT add_months(MAX(reference_month), 1)
# MAGIC    FROM ds_catalog_dev.credit_engine.abt_inference_me_br)                                         AS expected_apply_max,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE adjusted_score IS NULL
# MAGIC      AND updated_at = (SELECT MAX(updated_at) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC                        WHERE updated_at IS NOT NULL))                      AS nulls_adjusted_score,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE rut IS NULL
# MAGIC      AND updated_at = (SELECT MAX(updated_at) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC                        WHERE updated_at IS NOT NULL))                      AS nulls_rut,
# MAGIC   -- Distribuicao de bandas na ultima safra
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE score_band = '1-BAIXO'
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)) AS band_baixo,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE score_band = '2-MEDIO'
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)) AS band_medio,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE score_band = '3-ALTO'
# MAGIC      AND reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)) AS band_alto,
# MAGIC   -- Monitoramento do fator de suavizacao: clientes contidos pelo teto global (credit_limit < rv * payment_term * band_mult)
# MAGIC   -- Um cliente e contido quando payment_term * band_mult > smoothing_factor (5.0), independente da banda
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC     SELECT am.id_customer
# MAGIC     FROM ds_catalog_dev.credit_engine.apply_model_me_br am
# MAGIC     JOIN ds_catalog_dev.credit_engine.abt_inference_me_br inf
# MAGIC       ON  inf.id_customer     = am.id_customer
# MAGIC       AND inf.reference_month = add_months(am.reference_month, -1)
# MAGIC     WHERE am.reference_month = (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.apply_model_me_br)
# MAGIC       AND inf.payment_term * CASE
# MAGIC             WHEN (inf.historical_weight * inf.score + (1.0 - inf.historical_weight) * 0.30) <= 0.02 THEN 1.2
# MAGIC             WHEN (inf.historical_weight * inf.score + (1.0 - inf.historical_weight) * 0.30) <= 0.30 THEN 1.1
# MAGIC             ELSE 0.9 END > 5.0
# MAGIC   ))                                                                       AS clientes_contidos_pelo_teto_suavizacao,
# MAGIC   -- credit_limit_end: preenchido pelo NB02b (esperado NULL neste ponto)
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE credit_limit_end IS NOT NULL)                                     AS clientes_com_credit_limit_end,
# MAGIC   -- id_customer_mi e market_scope: preenchidos pelo NB02b (esperado NULL neste ponto)
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE id_customer_mi IS NOT NULL)                                       AS clientes_com_id_customer_mi,
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.apply_model_me_br
# MAGIC    WHERE market_scope IS NOT NULL)                                         AS clientes_com_market_scope

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ds_catalog_dev.credit_engine.apply_model_me_br