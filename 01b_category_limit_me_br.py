# Databricks notebook source
# DBTITLE 0,Pipeline: Limite por Categoria ME BR
# MAGIC %md
# MAGIC ## Pipeline: Limite por Categoria de Pedido — ME BR
# MAGIC
# MAGIC Calcula o limite de credito por categoria de produto com base no volume de pedidos,
# MAGIC e identifica a categoria principal por cliente no historico de 24 meses.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Tabelas de Saida
# MAGIC
# MAGIC | Tabela | Janela de Dados | Descricao |
# MAGIC |--------|-----------------|-----------|
# MAGIC | `ds_catalog_dev.default.category_limit_me_br` | **3 meses** | Limite por categoria (agregado trimestral) |
# MAGIC | `ds_catalog_dev.default.customer_top_category_me_br` | **24 meses** | Categoria principal por cliente no historico longo |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Conceito de Safra
# MAGIC
# MAGIC O campo `reference_quarter` armazena o primeiro dia do mes corrente como identificador
# MAGIC da safra. As duas tabelas usam janelas de dados distintas:
# MAGIC
# MAGIC - **`category_limit_me_br`**: 3 meses anteriores (`config_safra_trimestral`)
# MAGIC   Exemplo: `reference_quarter = 2026-04-01` → periodo de `2026-01-01` ate `2026-03-31`
# MAGIC
# MAGIC - **`customer_top_category_me_br`**: 24 meses anteriores (`config_safra_24m`)
# MAGIC   Exemplo: `reference_quarter = 2026-04-01` → periodo de `2024-04-01` ate `2026-03-31`
# MAGIC
# MAGIC O filtro de datas usa `>= data_inicio AND < reference_quarter` (strict less than)
# MAGIC em ambos os casos, garantindo que o ultimo dia incluido e sempre o ultimo dia do mes anterior.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Regras de Negocio
# MAGIC
# MAGIC 1. **Mapeamento de categoria**: `order_type` → `categoria` via DE-PARA fixo (`tab_categorias`).
# MAGIC    Tipos nao mapeados recebem prefixo `[NAO MAPEADO]` e pct_limite default de 0.03.
# MAGIC
# MAGIC 2. **Conversao cambial**: via `de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial`.
# MAGIC    - EUR, AUD: multiplicam pela taxa (cotacao direta)
# MAGIC    - CNY, AED, ARS, BRL: dividem pela taxa (cotacao indireta)
# MAGIC    - CNY sem taxa disponivel: fallback fixo /7
# MAGIC    - USD e demais moedas: valor original mantido
# MAGIC
# MAGIC 3. **Filtros**: somente pedidos faturados (`invoice_date IS NOT NULL`),
# MAGIC    valor > 0, exclui Minerva e Swift (nomes proprios).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Controle de Reprocessamento
# MAGIC
# MAGIC O campo `updated_at` registra quando a safra foi processada. O MERGE com chave
# MAGIC composta garante idempotencia — reexecutar o notebook atualiza a safra existente
# MAGIC sem duplicar registros.

# COMMAND ----------

# DBTITLE 0,Parametros
from datetime import date

dbutils.widgets.text("data_referencia", "", "Data de Referencia (YYYY-MM-DD)")

data_referencia = dbutils.widgets.get("data_referencia")
effective_date = data_referencia if data_referencia else str(date.today())

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW config_safra_trimestral AS
  SELECT
    CAST('{effective_date}' AS DATE) AS data_referencia,
    -- Safra: primeiro dia do mes da data de referencia
    date_trunc('month', CAST('{effective_date}' AS DATE)) AS reference_quarter,
    -- Periodo inicio: 3 meses antes da safra (usado por category_limit_me_br)
    add_months(date_trunc('month', CAST('{effective_date}' AS DATE)), -3) AS data_inicio,
    -- Periodo fim (para exibicao): ultimo dia do mes anterior a safra
    date_sub(date_trunc('month', CAST('{effective_date}' AS DATE)), 1) AS data_fim
""")

spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW config_safra_24m AS
  SELECT
    CAST('{effective_date}' AS DATE) AS data_referencia,
    -- Safra: primeiro dia do mes da data de referencia (mesmo identificador)
    date_trunc('month', CAST('{effective_date}' AS DATE)) AS reference_quarter,
    -- Periodo inicio: 24 meses antes da safra (usado por customer_top_category_me_br)
    add_months(date_trunc('month', CAST('{effective_date}' AS DATE)), -24) AS data_inicio,
    -- Periodo fim (para exibicao): ultimo dia do mes anterior a safra
    date_sub(date_trunc('month', CAST('{effective_date}' AS DATE)), 1) AS data_fim
""")

print(f"Data de referencia: {effective_date}")
print("--- config_safra_trimestral (3 meses) ---")
spark.sql("SELECT * FROM config_safra_trimestral").show(truncate=False)
print("--- config_safra_24m (24 meses) ---")
spark.sql("SELECT * FROM config_safra_24m").show(truncate=False)

# COMMAND ----------

# DBTITLE 0,Validacao das datas da safra
# MAGIC %sql
# MAGIC -- config_safra_trimestral: janela de 3 meses
# MAGIC SELECT
# MAGIC   '3 meses (category_limit)' AS janela,
# MAGIC   reference_quarter,
# MAGIC   data_inicio,
# MAGIC   data_fim,
# MAGIC   datediff(data_fim, data_inicio) + 1 AS dias_no_periodo,
# MAGIC   CASE
# MAGIC     WHEN data_fim = date_sub(reference_quarter, 1)
# MAGIC      AND data_inicio = add_months(reference_quarter, -3)
# MAGIC     THEN 'OK'
# MAGIC     ELSE 'ERRO — VERIFICAR DATAS'
# MAGIC   END AS status_validacao
# MAGIC FROM config_safra_trimestral
# MAGIC UNION ALL
# MAGIC -- config_safra_24m: janela de 24 meses
# MAGIC SELECT
# MAGIC   '24 meses (customer_top_category)' AS janela,
# MAGIC   reference_quarter,
# MAGIC   data_inicio,
# MAGIC   data_fim,
# MAGIC   datediff(data_fim, data_inicio) + 1 AS dias_no_periodo,
# MAGIC   CASE
# MAGIC     WHEN data_fim = date_sub(reference_quarter, 1)
# MAGIC      AND data_inicio = add_months(reference_quarter, -24)
# MAGIC     THEN 'OK'
# MAGIC     ELSE 'ERRO — VERIFICAR DATAS'
# MAGIC   END AS status_validacao
# MAGIC FROM config_safra_24m

# COMMAND ----------

# DBTITLE 0,DDL — ds_catalog_dev.default.category_limit_me_br
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ds_catalog_dev.default.category_limit_me_br (
# MAGIC   reference_quarter   DATE      COMMENT 'Safra trimestral — primeiro dia do mes de processamento / Quarterly vintage — first day of the processing month',
# MAGIC   category            STRING    COMMENT 'Categoria do produto mapeada a partir do order_type / Product category mapped from order_type',
# MAGIC   order_count         INT       COMMENT 'Quantidade de pedidos no trimestre / Number of orders in the quarter',
# MAGIC   customer_count      INT       COMMENT 'Quantidade de clientes distintos / Number of distinct customers',
# MAGIC   total_amount_usd    DOUBLE    COMMENT 'Valor total em USD (convertido via tabela de cambio) / Total amount in USD (converted via exchange rate table)',
# MAGIC   pct_total           DOUBLE    COMMENT 'Percentual do total geral (0-1, ex: 0.9275 = 92.75 porcento) / Percentage of grand total (0-1)',
# MAGIC   pct_limit           DOUBLE    COMMENT 'Percentual aplicado como limite (0-1, ex: 0.03 = 3 porcento) / Percentage applied as limit (0-1)',
# MAGIC   category_limit_usd  DOUBLE    COMMENT 'Limite da categoria em USD (total_amount_usd * pct_limit) / Category limit in USD',
# MAGIC   updated_at          TIMESTAMP COMMENT 'Data e hora do processamento / Processing timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Limite de credito por categoria de produto — safra trimestral ME BR / Credit limit by product category — quarterly vintage ME BR'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 0,DDL — ds_catalog_dev.default.customer_top_category_me_br
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ds_catalog_dev.default.customer_top_category_me_br (
# MAGIC   reference_quarter         DATE      COMMENT 'Safra — primeiro dia do mes de processamento / Vintage — first day of the processing month',
# MAGIC   customer_name             STRING    COMMENT 'Nome do cliente (importer_name) / Customer name (importer_name)',
# MAGIC   customer_code             INT       COMMENT 'Codigo do cliente (importer_id) / Customer code (importer_id)',
# MAGIC   top_category              STRING    COMMENT 'Categoria com maior valor de venda nos ultimos 24 meses / Category with highest sales value in the last 24 months',
# MAGIC   top_category_amount_usd   DOUBLE    COMMENT 'Valor total USD da top categoria nos ultimos 24 meses / Total USD amount of the top category in the last 24 months',
# MAGIC   id_customer_group_economic INT      COMMENT 'cod_pessoa_empresa_pai se existir, senao cod_pessoa_cliente (grupo economico) / cod_pessoa_empresa_pai if exists, otherwise cod_pessoa_cliente (economic group)',
# MAGIC   updated_at                TIMESTAMP COMMENT 'Data e hora do processamento / Processing timestamp'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Categoria principal por cliente — historico 24 meses ME BR / Top category per customer — 24-month history ME BR'
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 0,View: pedidos do trimestre com regras de categoria e cambio
# MAGIC %sql
# MAGIC -- ================================================================
# MAGIC -- Base de pedidos do trimestre parametrizada pela safra
# MAGIC -- Granularidade: 1 linha por pedido (num_pedido_comex)
# MAGIC -- Usada como fonte para analise de categoria e analise por cliente
# MAGIC --
# MAGIC -- Regras de negocio (origem: limite_por_categoria_pedido_v5):
# MAGIC --   1. Mapeamento order_type → categoria via tab_categorias (DE-PARA fixo)
# MAGIC --   2. Conversao cambial via de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial
# MAGIC --      - EUR, AUD: multiplicam pela taxa (cotacao direta)
# MAGIC --      - CNY, AED, ARS, BRL: dividem pela taxa (cotacao indireta)
# MAGIC --      - CNY sem taxa: fallback fixo /7
# MAGIC --      - USD e demais: valor original mantido
# MAGIC --   3. Filtros: somente pedidos faturados (invoice_date IS NOT NULL),
# MAGIC --      valor > 0, exclui Minerva e Swift (nomes proprios)
# MAGIC -- ================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_orders_quarter AS
# MAGIC WITH
# MAGIC
# MAGIC tab_categorias AS (
# MAGIC   SELECT * FROM (
# MAGIC     VALUES
# MAGIC       ('CARNE CONGELADA'           , 'Beef'                , 0.03),
# MAGIC       ('CARNE RESFRIADA'           , 'Beef'                , 0.03),
# MAGIC       ('CARNE CONGELADA - TRADER'  , 'Beef'                , 0.03),
# MAGIC       ('CARNE RESFRIADA - TRADER'  , 'Beef'                , 0.03),
# MAGIC       ('MIUDOS'                    , 'Beef'                , 0.03),
# MAGIC       ('CORDEIRO CONGELADO'        , 'Lamb'                , 0.03),
# MAGIC       ('BOI VIVO'                  , 'Live Cattle'         , 0.03),
# MAGIC       ('COURO'                     , 'Leather'             , 0.03),
# MAGIC       ('TRIPA'                     , 'Casing'              , 0.03),
# MAGIC       ('INDUSTRIALIZADOS'          , 'Industrialized'      , 0.03),
# MAGIC       ('CARNE EM CONSERVA'         , 'Processed / Canned'  , 0.03),
# MAGIC       ('FARINHAS/OUTROS'           , 'Byproducts'          , 0.03),
# MAGIC       ('OSSOS'                     , 'Byproducts'          , 0.03),
# MAGIC       ('SEBO'                      , 'Byproducts'          , 0.03),
# MAGIC       ('GALLSTONES'                , 'Byproducts'          , 0.03),
# MAGIC       ('AMOSTRA'                   , 'Sample'              , 0.03),
# MAGIC       ('TRADING'                   , 'Trading'             , 0.03)
# MAGIC   ) AS t(order_type, categoria, pct_limite)
# MAGIC ),
# MAGIC
# MAGIC taxas_raw AS (
# MAGIC   SELECT
# MAGIC     moeda_procedencia,
# MAGIC     CAST(valido_desde AS DATE) AS valido_desde,
# MAGIC     CAST(valido_ate   AS DATE) AS valido_ate,
# MAGIC     TRY_CAST(REPLACE(REPLACE(taxa_cambio, '/', ''), ',', '.') AS FLOAT) AS taxa_cambio
# MAGIC   FROM de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial
# MAGIC   WHERE moeda_procedencia IN ('AUD', 'EUR', 'CNY', 'AED', 'ARS', 'BRL')
# MAGIC     AND moeda_destino = 'USD'
# MAGIC     AND CONCAT(ctg_taxa_cambio, '-', moeda_procedencia) IN (
# MAGIC           'EURX-EUR', 'B-AUD', 'B-CNY', 'B-AED', 'B-ARS', 'B-BRL')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   cfg.reference_quarter,
# MAGIC   o.comex_order_number                                              AS num_pedido_comex,
# MAGIC   o.internal_order_number                                           AS num_pedido_interno,
# MAGIC   o.importer_id                                                     AS cod_pessoa_cliente,
# MAGIC   o.importer_name                                                   AS nom_pessoa_cliente,
# MAGIC   o.destination_country_name                                        AS pais_destino,
# MAGIC   o.order_type,
# MAGIC   COALESCE(tc.categoria, CONCAT('[NAO MAPEADO] ', o.order_type))    AS categoria,
# MAGIC   COALESCE(tc.pct_limite, 0.03)                                     AS pct_limite,
# MAGIC   CAST(o.order_opening_date AS DATE)                                AS dta_abertura_pedido,
# MAGIC   date_trunc('month', CAST(o.order_opening_date AS DATE))           AS mes_abertura,
# MAGIC   o.currency_id                                                     AS moeda,
# MAGIC   o.total_order_value                                               AS valor_original,
# MAGIC   CASE
# MAGIC     WHEN o.currency_id = 'CNY'  AND tx.taxa_cambio IS NULL     THEN ROUND(o.total_order_value / 7, 2)
# MAGIC     WHEN o.currency_id = 'CNY'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'AED'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'ARS'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'REAL' AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'EUR'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value * tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'AUD'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value * tx.taxa_cambio, 2)
# MAGIC     ELSE o.total_order_value
# MAGIC   END AS valor_usd
# MAGIC FROM de_data_lake_prd.business_analytics.flat_orders_external_market o
# MAGIC CROSS JOIN config_safra_trimestral cfg
# MAGIC LEFT JOIN tab_categorias tc ON tc.order_type = o.order_type
# MAGIC LEFT JOIN taxas_raw tx
# MAGIC   ON  tx.moeda_procedencia = CASE WHEN o.currency_id = 'REAL' THEN 'BRL' ELSE o.currency_id END
# MAGIC   AND COALESCE(CAST(o.invoice_date AS DATE), CAST(o.order_opening_date AS DATE))
# MAGIC       BETWEEN tx.valido_desde AND tx.valido_ate
# MAGIC WHERE CAST(o.order_opening_date AS DATE) >= cfg.data_inicio
# MAGIC   AND CAST(o.order_opening_date AS DATE) <  cfg.reference_quarter
# MAGIC   AND o.invoice_date IS NOT NULL
# MAGIC   AND o.total_order_value > 0
# MAGIC   AND o.comex_order_number IS NOT NULL
# MAGIC   AND o.importer_name NOT LIKE 'MINER%'
# MAGIC   AND o.importer_name NOT LIKE 'SWIF%'

# COMMAND ----------

# DBTITLE 0,Diagnostico: volume do trimestre
# MAGIC %sql
# MAGIC SELECT
# MAGIC   reference_quarter,
# MAGIC   (SELECT data_inicio FROM config_safra_trimestral) AS data_inicio,
# MAGIC   (SELECT data_fim FROM config_safra_trimestral)    AS data_fim,
# MAGIC   COUNT(DISTINCT num_pedido_comex)  AS total_orders,
# MAGIC   COUNT(DISTINCT cod_pessoa_cliente) AS total_customers,
# MAGIC   ROUND(SUM(valor_usd), 2) AS total_usd,
# MAGIC   COUNT(DISTINCT categoria) AS total_categorias
# MAGIC FROM vw_orders_quarter
# MAGIC GROUP BY reference_quarter

# COMMAND ----------

# DBTITLE 0,View: pedidos dos ultimos 24 meses (base para customer_top_category_me_br)
# MAGIC %sql
# MAGIC -- ================================================================
# MAGIC -- Replica de vw_orders_quarter com janela de 24 meses.
# MAGIC -- Usada exclusivamente pelo MERGE de customer_top_category_me_br.
# MAGIC -- Mesmas regras de negocio (categoria, cambio, filtros),
# MAGIC -- diferindo apenas na config de safra utilizada.
# MAGIC -- ================================================================
# MAGIC CREATE OR REPLACE TEMP VIEW vw_orders_24m AS
# MAGIC WITH
# MAGIC
# MAGIC tab_categorias AS (
# MAGIC   SELECT * FROM (
# MAGIC     VALUES
# MAGIC       ('CARNE CONGELADA'           , 'Beef'                , 0.03),
# MAGIC       ('CARNE RESFRIADA'           , 'Beef'                , 0.03),
# MAGIC       ('CARNE CONGELADA - TRADER'  , 'Beef'       , 0.03),
# MAGIC       ('CARNE RESFRIADA - TRADER'  , 'Beef'       , 0.03),
# MAGIC       ('MIUDOS'                    , 'Beef'              , 0.03),
# MAGIC       ('CORDEIRO CONGELADO'        , 'Lamb'                , 0.03),
# MAGIC       ('BOI VIVO'                  , 'Live Cattle'         , 0.03),
# MAGIC       ('COURO'                     , 'Leather'             , 0.03),
# MAGIC       ('TRIPA'                     , 'Casing'              , 0.03),
# MAGIC       ('INDUSTRIALIZADOS'          , 'Industrialized'      , 0.03),
# MAGIC       ('CARNE EM CONSERVA'         , 'Processed / Canned'  , 0.03),
# MAGIC       ('FARINHAS/OUTROS'           , 'Byproducts'          , 0.03),
# MAGIC       ('OSSOS'                     , 'Byproducts'          , 0.03),
# MAGIC       ('SEBO'                      , 'Byproducts'          , 0.03),
# MAGIC       ('GALLSTONES'                , 'Byproducts'          , 0.03),
# MAGIC       ('AMOSTRA'                   , 'Sample'              , 0.03),
# MAGIC       ('TRADING'                   , 'Trading'             , 0.03)
# MAGIC   ) AS t(order_type, categoria, pct_limite)
# MAGIC ),
# MAGIC
# MAGIC taxas_raw AS (
# MAGIC   SELECT
# MAGIC     moeda_procedencia,
# MAGIC     CAST(valido_desde AS DATE) AS valido_desde,
# MAGIC     CAST(valido_ate   AS DATE) AS valido_ate,
# MAGIC     TRY_CAST(REPLACE(REPLACE(taxa_cambio, '/', ''), ',', '.') AS FLOAT) AS taxa_cambio
# MAGIC   FROM de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial
# MAGIC   WHERE moeda_procedencia IN ('AUD', 'EUR', 'CNY', 'AED', 'ARS', 'BRL')
# MAGIC     AND moeda_destino = 'USD'
# MAGIC     AND CONCAT(ctg_taxa_cambio, '-', moeda_procedencia) IN (
# MAGIC           'EURX-EUR', 'B-AUD', 'B-CNY', 'B-AED', 'B-ARS', 'B-BRL')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   cfg.reference_quarter,
# MAGIC   o.comex_order_number                                              AS num_pedido_comex,
# MAGIC   o.internal_order_number                                           AS num_pedido_interno,
# MAGIC   o.importer_id                                                     AS cod_pessoa_cliente,
# MAGIC   o.importer_name                                                   AS nom_pessoa_cliente,
# MAGIC   o.destination_country_name                                        AS pais_destino,
# MAGIC   o.order_type,
# MAGIC   COALESCE(tc.categoria, CONCAT('[NAO MAPEADO] ', o.order_type))    AS categoria,
# MAGIC   COALESCE(tc.pct_limite, 0.03)                                     AS pct_limite,
# MAGIC   CAST(o.order_opening_date AS DATE)                                AS dta_abertura_pedido,
# MAGIC   date_trunc('month', CAST(o.order_opening_date AS DATE))           AS mes_abertura,
# MAGIC   o.currency_id                                                     AS moeda,
# MAGIC   o.total_order_value                                               AS valor_original,
# MAGIC   CASE
# MAGIC     WHEN o.currency_id = 'CNY'  AND tx.taxa_cambio IS NULL     THEN ROUND(o.total_order_value / 7, 2)
# MAGIC     WHEN o.currency_id = 'CNY'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'AED'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'ARS'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'REAL' AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value / tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'EUR'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value * tx.taxa_cambio, 2)
# MAGIC     WHEN o.currency_id = 'AUD'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(o.total_order_value * tx.taxa_cambio, 2)
# MAGIC     ELSE o.total_order_value
# MAGIC   END AS valor_usd
# MAGIC FROM de_data_lake_prd.business_analytics.flat_orders_external_market o
# MAGIC CROSS JOIN config_safra_24m cfg
# MAGIC LEFT JOIN tab_categorias tc ON tc.order_type = o.order_type
# MAGIC LEFT JOIN taxas_raw tx
# MAGIC   ON  tx.moeda_procedencia = CASE WHEN o.currency_id = 'REAL' THEN 'BRL' ELSE o.currency_id END
# MAGIC   AND COALESCE(CAST(o.invoice_date AS DATE), CAST(o.order_opening_date AS DATE))
# MAGIC       BETWEEN tx.valido_desde AND tx.valido_ate
# MAGIC WHERE CAST(o.order_opening_date AS DATE) >= cfg.data_inicio
# MAGIC   AND CAST(o.order_opening_date AS DATE) <  cfg.reference_quarter
# MAGIC   AND o.invoice_date IS NOT NULL
# MAGIC   AND o.total_order_value > 0
# MAGIC   AND o.comex_order_number IS NOT NULL
# MAGIC   AND o.importer_name NOT LIKE 'MINER%'
# MAGIC   AND o.importer_name NOT LIKE 'SWIF%'

# COMMAND ----------

# DBTITLE 0,Diagnostico: order_types nao mapeados
# MAGIC %sql
# MAGIC -- Se aparecer algum [NAO MAPEADO], precisa atualizar tab_categorias
# MAGIC SELECT
# MAGIC   categoria,
# MAGIC   COUNT(*) AS qtd
# MAGIC FROM vw_orders_quarter
# MAGIC WHERE categoria LIKE '[NAO MAPEADO]%'
# MAGIC GROUP BY categoria
# MAGIC ORDER BY qtd DESC

# COMMAND ----------

# DBTITLE 0,MERGE — ds_catalog_dev.default.category_limit_me_br
# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.default.category_limit_me_br AS target
# MAGIC USING (
# MAGIC   WITH agg_category AS (
# MAGIC     SELECT
# MAGIC       reference_quarter,
# MAGIC       categoria                              AS category,
# MAGIC       pct_limite                             AS pct_limit,
# MAGIC       COUNT(DISTINCT num_pedido_comex)        AS order_count,
# MAGIC       COUNT(DISTINCT cod_pessoa_cliente)      AS customer_count,
# MAGIC       ROUND(SUM(valor_usd), 2)               AS total_amount_usd
# MAGIC     FROM vw_orders_quarter
# MAGIC     GROUP BY reference_quarter, categoria, pct_limite
# MAGIC   )
# MAGIC   SELECT
# MAGIC     a.reference_quarter,
# MAGIC     a.category,
# MAGIC     a.order_count,
# MAGIC     a.customer_count,
# MAGIC     a.total_amount_usd,
# MAGIC     ROUND(a.total_amount_usd / SUM(a.total_amount_usd) OVER (), 4) AS pct_total,
# MAGIC     a.pct_limit,
# MAGIC     ROUND(a.total_amount_usd * a.pct_limit, 2) AS category_limit_usd,
# MAGIC     current_timestamp() AS updated_at
# MAGIC   FROM agg_category a
# MAGIC ) AS source
# MAGIC ON target.reference_quarter = source.reference_quarter
# MAGIC   AND target.category = source.category
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 0,View: grupo economico (id_customer_group_economic — logica me_compiled)
# MAGIC %sql
# MAGIC -- Replica cod_pessoa_cliente_grupo_economico do me_compiled (view_base_ultimo_mes_6)
# MAGIC -- Fonte: de_data_lake_prd.dados_mestres.dbpessoa_tab_empresas_relacionadas, cod_tipo_relacionamento = 4
# MAGIC -- Regra: se cliente tem empresa pai, usa cod_pessoa_empresa_pai; senao usa cod_pessoa_cliente
# MAGIC CREATE OR REPLACE TEMP VIEW view_grupo_economico_cat AS
# MAGIC SELECT
# MAGIC   x.cod_pessoa_empresa_filha AS cod_pessoa_cliente,
# MAGIC   MAX(x.cod_pessoa_empresa_pai) AS cod_pessoa_empresa_pai
# MAGIC FROM de_data_lake_prd.dados_mestres.dbpessoa_tab_empresas_relacionadas AS x
# MAGIC WHERE x.cod_tipo_relacionamento = 4
# MAGIC GROUP BY x.cod_pessoa_empresa_filha

# COMMAND ----------

# DBTITLE 0,MERGE — ds_catalog_dev.default.customer_top_category_me_br
# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.default.customer_top_category_me_br AS target
# MAGIC USING (
# MAGIC   WITH customer_category_sales AS (
# MAGIC     SELECT
# MAGIC       reference_quarter,
# MAGIC       nom_pessoa_cliente                     AS customer_name,
# MAGIC       CAST(cod_pessoa_cliente AS INT)         AS customer_code,
# MAGIC       categoria                              AS category,
# MAGIC       SUM(valor_usd)                         AS category_total_usd
# MAGIC     FROM vw_orders_24m
# MAGIC     GROUP BY reference_quarter, nom_pessoa_cliente, cod_pessoa_cliente, categoria
# MAGIC   ),
# MAGIC   ranked AS (
# MAGIC     SELECT
# MAGIC       reference_quarter,
# MAGIC       customer_name,
# MAGIC       customer_code,
# MAGIC       category AS top_category,
# MAGIC       category_total_usd,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY reference_quarter, customer_code
# MAGIC         ORDER BY category_total_usd DESC
# MAGIC       ) AS rn
# MAGIC     FROM customer_category_sales
# MAGIC   )
# MAGIC   SELECT
# MAGIC     reference_quarter,
# MAGIC     customer_name,
# MAGIC     customer_code,
# MAGIC     top_category,
# MAGIC     ROUND(ranked.category_total_usd, 2)                                        AS top_category_amount_usd,
# MAGIC     COALESCE(ge.cod_pessoa_empresa_pai, ranked.customer_code) AS id_customer_group_economic,
# MAGIC     current_timestamp() AS updated_at
# MAGIC   FROM ranked
# MAGIC   LEFT JOIN view_grupo_economico_cat ge ON ge.cod_pessoa_cliente = ranked.customer_code
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.reference_quarter = source.reference_quarter
# MAGIC   AND target.customer_code = source.customer_code
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ds_catalog_dev.default.category_limit_me_br