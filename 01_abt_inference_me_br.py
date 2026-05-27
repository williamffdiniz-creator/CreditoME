# Databricks notebook source
# DBTITLE 0,Pipeline: ABT Inference ME BR — reference_value via logica total_amount
# MAGIC %md
# MAGIC ## ABT Inference — ME Brasil — reference_value unificado com total_amount
# MAGIC
# MAGIC Variante do pipeline que elimina por design a possibilidade de reference_value = 0
# MAGIC quando total_amount > 0, unificando ambos os campos na mesma fonte de dados.
# MAGIC
# MAGIC ### Problema resolvido
# MAGIC
# MAGIC O pipeline original calcula `total_amount` e `reference_value` usando fontes diferentes:
# MAGIC - `total_amount`: `view_base_parcelas` + `dta_abertura_pedido` (janela 12 meses)
# MAGIC - `reference_value`: `flat_orders_external_market` + `invoice_date` (janela 24 meses)
# MAGIC
# MAGIC Clientes presentes em `flat_financial_position` mas ausentes de `flat_orders` ficam com
# MAGIC `reference_value = 0` mesmo tendo `total_amount > 0`. Diagnóstico confirma esse padrão
# MAGIC nas safras 2020-01 (~51% zeros) e 2020-02 (~26% zeros).
# MAGIC
# MAGIC ### Solucao aplicada
# MAGIC
# MAGIC `reference_value` passa a usar a mesma fonte, mesma data e mesma lógica de `total_amount`
# MAGIC (`view_base_parcelas` + `dta_abertura_pedido`), apenas com janela de 24 meses e
# MAGIC agregação em média mensal. Isso garante:
# MAGIC - Se `total_amount > 0` → `reference_value > 0` (mesma fonte, impossivel divergir)
# MAGIC - Consistência semântica: exposição histórica medida pelo mesmo critério da inadimplência
# MAGIC
# MAGIC ### Regras aplicadas
# MAGIC
# MAGIC | Aspecto | Origem | Detalhes |
# MAGIC |---------|--------|----------|
# MAGIC | Baseline | Original ME BR | PERFORMANCE→data_chegada_navio; CRÉDITO→data_vencimento |
# MAGIC | Escala | Original ME BR | pct em % (25.00), medianas em %, score = Σ(pct×med)/10000 |
# MAGIC | Granularidade | Original ME BR | Pedidos (dta_abertura_pedido), parcela→invoice→cliente |
# MAGIC | Benchmark | v7 | Portfolio score = Σ(fracao_inadimpl_k × mediana_k) / 10000, fallback 0.30 |
# MAGIC | Câmbio | v3 | 6 moedas (AUD,EUR,CNY,AED,ARS,BRL) com forward-fill |
# MAGIC | Processamento | v3 | Incremental multi-safra (CROSS JOIN) |
# MAGIC | **reference_value** | **Novo** | **view_base_parcelas + dta_abertura_pedido, janela 24m, AVG mensal** |
# MAGIC
# MAGIC ### Tabelas
# MAGIC | Tabela | Descricao |
# MAGIC |--------|-----------|
# MAGIC | `ds_catalog_dev.credit_engine.abt_inference_me_br` | Features + score por cliente/safra (4 faixas, escala %) |
# MAGIC | `ds_catalog_dev.credit_engine.portfolio_abt_group_me_br` | Medianas + portfolio_score por safra |

# COMMAND ----------

# DBTITLE 0,Parametros
from datetime import date
dbutils.widgets.text("data_referencia", "", "Data de Referencia")
data_referencia = dbutils.widgets.get("data_referencia")
effective_date = data_referencia if data_referencia else str(date.today())
spark.sql(f"CREATE OR REPLACE TEMP VIEW config_pipeline AS SELECT CAST('{effective_date}' AS DATE) AS data_referencia")
print(f"Data de referencia: {effective_date}")

# COMMAND ----------

# DBTITLE 0,DROP + CREATE abt_inference_me_br_v7
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ds_catalog_dev.credit_engine.abt_inference_me_br (
# MAGIC   id_customer                  INT,
# MAGIC   customer_name                STRING,
# MAGIC   country                      STRING,
# MAGIC   reference_month              DATE,
# MAGIC   total_amount                 DOUBLE    COMMENT 'Total faturado USD',
# MAGIC   overdue_amount               DOUBLE    COMMENT 'Valor em atraso >7d USD',
# MAGIC   overdue_pct                  DOUBLE    COMMENT '% atraso (escala 0-100)',
# MAGIC   target                       INT       COMMENT '1 se teve atraso >7d',
# MAGIC   months_with_billing          INT       COMMENT 'Meses com faturamento na janela',
# MAGIC   months_defaulted             INT       COMMENT 'Meses inadimplentes (>7d E >=10%)',
# MAGIC   pct_months_overdue_10_20     DOUBLE    COMMENT '% meses faixa 10-20% (escala 0-100)',
# MAGIC   pct_months_overdue_20_30     DOUBLE    COMMENT '% meses faixa 20-30% (escala 0-100)',
# MAGIC   pct_months_overdue_30_50     DOUBLE    COMMENT '% meses faixa 30-50% (escala 0-100)',
# MAGIC   pct_months_overdue_50_plus   DOUBLE    COMMENT '% meses faixa 50%+ (escala 0-100)',
# MAGIC   score                        DOUBLE    COMMENT 'Score individual = sum(pct*med)/10000',
# MAGIC   historical_weight            DOUBLE    COMMENT 'C = LEAST(1, months/3)',
# MAGIC   flag_transacted              INT       COMMENT '1=transacionou no mes ref',
# MAGIC   reference_value              DOUBLE    COMMENT 'Media mensal de exposicao 24m USD (logica motor)',
# MAGIC   reference_value_clp          DOUBLE    COMMENT 'Media mensal de exposicao 24m em CLP (reference_value USD * taxa CLP/USD do dia)',
# MAGIC   payment_term                 DOUBLE    COMMENT 'Prazo comercial medio meses, clip motor (<=0 ou >=5 -> 1)',
# MAGIC   id_customer_group_economic   INT       COMMENT 'cod_pessoa_empresa_pai se existir, senao cod_pessoa_cliente (grupo economico)',
# MAGIC   updated_at                   TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 0,Controle incremental
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW controle_processamento AS
# MAGIC SELECT MAX(reference_month) AS ultima_safra_processada,
# MAGIC        current_timestamp() AS data_atualizacao,
# MAGIC        (SELECT data_referencia FROM config_pipeline) AS data_referencia
# MAGIC FROM ds_catalog_dev.credit_engine.abt_inference_me_br

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views Auxiliares

# COMMAND ----------

# DBTITLE 1,view_taxas_cambio (6 moedas, forward-fill — v3)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_taxas_cambio AS
# MAGIC WITH cte_taxas_raw AS (
# MAGIC   SELECT moeda_procedencia,
# MAGIC          CAST(valido_desde AS DATE) AS valido_desde,
# MAGIC          TRY_CAST(REPLACE(REPLACE(taxa_cambio, '/', ''), ',', '.') AS FLOAT) AS taxa_cambio,
# MAGIC          CAST(valido_ate AS DATE) AS valido_ate
# MAGIC   FROM de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial
# MAGIC   WHERE moeda_procedencia IN ('AUD', 'EUR', 'CNY', 'AED', 'ARS', 'BRL')
# MAGIC     AND moeda_destino = 'USD'
# MAGIC     AND CONCAT(ctg_taxa_cambio, '-', moeda_procedencia) IN (
# MAGIC           'EURX-EUR', 'B-AUD', 'B-CNY', 'B-AED', 'B-ARS', 'B-BRL')
# MAGIC ),
# MAGIC cte_moedas AS (SELECT DISTINCT moeda_procedencia FROM cte_taxas_raw),
# MAGIC cte_calendario AS (
# MAGIC   SELECT EXPLODE(SEQUENCE((SELECT MIN(valido_desde) FROM cte_taxas_raw), current_date(), INTERVAL 1 DAY)) AS dia
# MAGIC ),
# MAGIC cte_grid AS (SELECT c.dia, m.moeda_procedencia FROM cte_calendario c CROSS JOIN cte_moedas m),
# MAGIC cte_join AS (
# MAGIC   SELECT g.dia, g.moeda_procedencia, t.taxa_cambio
# MAGIC   FROM cte_grid g LEFT JOIN cte_taxas_raw t
# MAGIC     ON t.moeda_procedencia = g.moeda_procedencia AND g.dia BETWEEN t.valido_desde AND t.valido_ate
# MAGIC )
# MAGIC SELECT moeda_procedencia, dia AS data_taxa,
# MAGIC   COALESCE(
# MAGIC     LAST_VALUE(taxa_cambio, TRUE) OVER (PARTITION BY moeda_procedencia ORDER BY dia ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
# MAGIC     FIRST_VALUE(taxa_cambio, TRUE) OVER (PARTITION BY moeda_procedencia ORDER BY dia ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
# MAGIC   ) AS taxa_cambio
# MAGIC FROM cte_join

# COMMAND ----------

# DBTITLE 1,view_taxa_clp_hoje (taxa CLP/USD do dia — forward-fill)
# MAGIC %sql
# MAGIC -- Retorna uma unica linha com a taxa CLP/USD mais recente disponivel.
# MAGIC -- Usa a mesma tabela de parametros de cambio do pipeline Chile
# MAGIC -- (de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial, moeda_procedencia = 'CLP', moeda_destino = 'USD').
# MAGIC --
# MAGIC -- Logica de forward-fill identica ao pipeline Chile:
# MAGIC --   1. raw: expande cada registro de vigencia em dias individuais (SEQUENCE + LATERAL VIEW EXPLODE)
# MAGIC --   2. date_spine: serie continua de datas desde o min ate hoje
# MAGIC --   3. joined + filled: FIRST_VALUE por grupo acumulado garante forward-fill sem gaps
# MAGIC --   4. SELECT final: pega apenas current_date() (ou a data preenchida mais proxima)
# MAGIC --
# MAGIC -- Conversao no MERGE: reference_value (USD) * taxa_clp = reference_value_clp (CLP)
# MAGIC --   ex: 100 USD * 900 (CLP/USD) = 90.000 CLP
# MAGIC CREATE OR REPLACE TEMP VIEW view_taxa_clp_hoje AS
# MAGIC WITH raw AS (
# MAGIC   SELECT
# MAGIC     CAST(valido_desde AS DATE) AS valido_desde,
# MAGIC     CAST(valido_ate   AS DATE) AS valido_ate,
# MAGIC     TRY_CAST(REPLACE(REPLACE(taxa_cambio, '/', ''), ',', '.') AS FLOAT) AS taxa_cambio
# MAGIC   FROM de_data_lake_prd.financeiro.dw_tab_parametro_cotacao_cambial
# MAGIC   WHERE moeda_procedencia = 'CLP'
# MAGIC     AND moeda_destino      = 'USD'
# MAGIC     AND taxa_cambio IS NOT NULL
# MAGIC     AND CAST(valido_desde AS DATE) <= CAST(valido_ate AS DATE)
# MAGIC ),
# MAGIC exploded AS (
# MAGIC   SELECT
# MAGIC     CAST(exploded_date AS DATE) AS data_taxa,
# MAGIC     taxa_cambio
# MAGIC   FROM raw
# MAGIC   LATERAL VIEW EXPLODE(
# MAGIC     SEQUENCE(valido_desde, valido_ate, INTERVAL 1 DAY)
# MAGIC   ) t AS exploded_date
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
# MAGIC   SELECT
# MAGIC     ds.data_taxa,
# MAGIC     e.taxa_cambio,
# MAGIC     COUNT(e.taxa_cambio) OVER (
# MAGIC       ORDER BY ds.data_taxa
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS grp
# MAGIC   FROM date_spine ds
# MAGIC   LEFT JOIN exploded e ON e.data_taxa = ds.data_taxa
# MAGIC ),
# MAGIC filled AS (
# MAGIC   SELECT
# MAGIC     data_taxa,
# MAGIC     FIRST_VALUE(taxa_cambio) OVER (
# MAGIC       PARTITION BY grp ORDER BY data_taxa
# MAGIC     ) AS taxa_cambio
# MAGIC   FROM joined
# MAGIC )
# MAGIC SELECT taxa_cambio AS taxa_clp_usd
# MAGIC FROM filled
# MAGIC WHERE data_taxa = (SELECT MAX(data_taxa) FROM filled WHERE taxa_cambio IS NOT NULL)
# MAGIC LIMIT 1

# COMMAND ----------

# DBTITLE 1,view_fato_risco_dist (original)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_fato_risco_dist AS
# MAGIC SELECT id_invoice AS numero_invoice, year(dt_billing) AS ano_faturamento,
# MAGIC        max(desc_customer) AS nome_cliente, max(desc_country_destination) AS pais_destino, max(desc_origin) AS origem
# MAGIC FROM de_data_lake_prd.business_analytics.photo_risk_me
# MAGIC GROUP BY numero_invoice, ano_faturamento

# COMMAND ----------

# DBTITLE 1,view_canal_cliente_dist (original)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_canal_cliente_dist AS
# MAGIC WITH cte_base AS (
# MAGIC   SELECT order_date, order_number,
# MAGIC          paying_client_code AS cod_cliente_pagador, paying_client AS cliente_pagador,
# MAGIC          paying_client_t_channel AS rma_cliente_pagador, comex_client_t_channel AS rma_cliente_comex,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY order_date DESC) AS ordem
# MAGIC   FROM de_data_lake_prd.pricing_models.tab_sales_history_dw
# MAGIC   WHERE order_number IS NOT NULL
# MAGIC   GROUP BY order_date, order_number, paying_client_code, paying_client, paying_client_t_channel, comex_client_t_channel
# MAGIC )
# MAGIC SELECT * FROM cte_base WHERE ordem = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pedidos + Tipo de Risco

# COMMAND ----------

# DBTITLE 1,view_principal_p_base_unificada (original)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_principal_p_base_unificada AS
# MAGIC WITH cte_cons AS (
# MAGIC     SELECT a.company_id AS cod_pessoa_empresa, a.importer_id AS cod_pessoa_cliente,
# MAGIC         a.comex_order_number AS num_pedido_comex, a.internal_order_number AS num_pedido,
# MAGIC         a.payment_method_id AS cod_forma_pagamento, a.importer_name AS nom_pessoa_cliente,
# MAGIC         a.destination_country_name AS pais_destino, a.origin_order AS origem,
# MAGIC         a.destination_port_name AS porto_destino, a.order_type AS tipo_pedido,
# MAGIC         a.order_opening_date AS dta_abertura_pedido, a.invoice_date AS dta_fatura,
# MAGIC         a.ship_departure_date AS dta_saida_navio, a.arrival_date AS dta_chegada_destino,
# MAGIC         a.total_order_value AS vlr_total_pedido, a.payment_method_description AS des_forma_pagamento,
# MAGIC         b.dta_vencimento, a.payer_name AS nom_pagador,
# MAGIC         a.kilograms_requested AS kgs_solicitado, a.kilograms_shipped AS kgs_embarcados,
# MAGIC         CASE WHEN a.importer_name = a.payer_name THEN 1 ELSE 0 END AS flag_mesmo_nome
# MAGIC     FROM de_data_lake_prd.business_analytics.flat_orders_external_market AS a
# MAGIC     LEFT JOIN de_data_lake_prd.exportacao.dbcomfri_tab_pedido_comex_vencimento AS b ON b.num_pedido_comex = a.comex_order_number
# MAGIC     WHERE a.sales_confirmation_date >= '2019-07-01'
# MAGIC ),
# MAGIC cte_inicial AS (
# MAGIC     SELECT ROW_NUMBER() OVER (PARTITION BY num_pedido_comex ORDER BY dta_fatura DESC) AS ordem, *
# MAGIC     FROM cte_cons
# MAGIC )
# MAGIC SELECT a.*, trp.des_risco_pagamento,
# MAGIC   CASE WHEN UPPER(trp.des_risco_pagamento) IN ('ALTO RISCO', 'RISCO MODERADO COM GARANTIA') THEN 'RISCO CREDITO'
# MAGIC        WHEN UPPER(trp.des_risco_pagamento) IN ('RISCO MODERADO SEM GARANTIA', 'SEM RISCO A VISTA') THEN 'RISCO PERFORMANCE'
# MAGIC   ELSE NULL END AS tipo_risco
# MAGIC FROM cte_inicial AS a
# MAGIC LEFT JOIN de_data_lake_prd.dados_mestres.dbcomfri_tab_forma_pagamento AS tfp ON tfp.cod_forma_pagamento = a.cod_forma_pagamento
# MAGIC LEFT JOIN de_data_lake_prd.dados_mestres.dbfinanceiro_tab_risco_pagamento AS trp ON trp.cod_risco_pagamento = tfp.cod_risco_pagamento
# MAGIC WHERE a.ordem = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Posicao Financeira

# COMMAND ----------

# DBTITLE 1,view_posicao_financeira (original — sem calculo de atraso)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_posicao_financeira AS
# MAGIC WITH cte_claim AS (
# MAGIC   SELECT customer_id AS codcliente, order_number AS numero_ordem,
# MAGIC     SUM(CAST(total_credit_note_value AS FLOAT)) AS valor_total_credit_note,
# MAGIC     SUM(CAST(REPLACE(REPLACE(received_dn_value, '.', ''), ',', '.') AS FLOAT)) AS valor_recebido_dn,
# MAGIC     ROUND(SUM(CAST(total_credit_note_value AS FLOAT)) - SUM(CAST(REPLACE(REPLACE(received_dn_value, '.', ''), ',', '.') AS FLOAT)), 2) AS prejuizo_Final
# MAGIC   FROM (
# MAGIC     SELECT customer_id, order_number, total_credit_note_value, received_dn_value, credit_note_number,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY credit_note_number ORDER BY order_number DESC) AS ordem
# MAGIC     FROM de_data_lake_prd.business_analytics.claim_customer_service_br
# MAGIC     WHERE complaint_classification IN ('Restricao', 'Recuperação de despesa')
# MAGIC       AND cn_process_status <> 'CN Cancelada' AND process_status <> 'Cancelada'
# MAGIC       AND main_complaint_reason NOT IN ('Alteração de Legislação / Sem controle ', 'Cambio de Legislación / Sin control')
# MAGIC     GROUP BY customer_id, order_number, total_credit_note_value, received_dn_value, credit_note_number
# MAGIC   ) sub WHERE ordem = 1
# MAGIC   GROUP BY codcliente, numero_ordem
# MAGIC )
# MAGIC SELECT a.billing_date AS data_faturamento, e.cod_pessoa_cliente, e.nom_pessoa_cliente,
# MAGIC        a.invoice_number AS numero_invoice, a.installment_id AS id_parcela,
# MAGIC        a.customer_country AS pais_cliente, a.destination_country AS pais_destino,
# MAGIC        a.due_date AS data_vencimento, a.payment_date AS data_pagamento,
# MAGIC        a.ship_arrival_date AS data_chegada_navio, a.total_invoice AS total_fatura,
# MAGIC        a.financial_position AS posicao_financeira, a.payment_method AS forma_pagamento,
# MAGIC        a.currency AS moeda, a.contract_type AS tipo_contrato,
# MAGIC        COUNT(a.invoice_number) OVER (PARTITION BY a.invoice_number) AS qtd_parcelas,
# MAGIC        c.valor_total_credit_note,
# MAGIC        c.valor_total_credit_note / COUNT(a.invoice_number) OVER (PARTITION BY a.invoice_number) AS vl_credit_note_rateio,
# MAGIC        c.prejuizo_final / COUNT(a.invoice_number) OVER (PARTITION BY a.invoice_number) AS prejuizo_final_rateio
# MAGIC FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys AS a
# MAGIC LEFT JOIN cte_claim AS c ON c.numero_ordem = a.invoice_number
# MAGIC LEFT JOIN view_principal_p_base_unificada AS e ON e.num_pedido_comex = a.invoice_number
# MAGIC WHERE a.financial_position IN ('A RECEBER', 'JUDICIAL')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cadastro Cliente

# COMMAND ----------

# DBTITLE 1,TRAT_CLIENT (JDBC — original)
user = "svc_ml_aws"
password = "nch}6,OLs16#Kzd%IJ>6k5H8L"
 
jdbc_url = (
    "jdbc:sqlserver://172.25.2.76:1433;"
    "instanceName=DWMINERVA;"
    "databaseName=dw_trusted;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)
 
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("query", "SELECT * FROM gestao_de_risco.trat_client WITH (NOLOCK)")
    .option("database", "dw")
    .option("user", user)
    .option("password", password)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)
df.createOrReplaceTempView('view_trat_client')


# COMMAND ----------

# DBTITLE 1,view_cadastro_cliente (original)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_cadastro_cliente AS
# MAGIC WITH cte_cons AS (
# MAGIC   SELECT YEAR(data_faturamento) AS ano, MAX(data_faturamento) AS max_data_faturamento,
# MAGIC          numero_invoice, cod_pessoa_cliente AS codigo_do_cliente,
# MAGIC          nom_pessoa_cliente AS cliente_comex, pais_cliente AS pais_do_cliente, pais_destino AS pais_do_destino
# MAGIC   FROM view_posicao_financeira
# MAGIC   GROUP BY data_faturamento, numero_invoice, cod_pessoa_cliente, nom_pessoa_cliente, pais_cliente, pais_destino
# MAGIC ),
# MAGIC cte_trat_client AS (SELECT cliente, MAX(cliente_tratado) AS cliente_tratado FROM view_trat_client GROUP BY cliente),
# MAGIC cte_cons1 AS (SELECT a.*, b.cliente_tratado FROM cte_cons a LEFT JOIN cte_trat_client b ON b.cliente = a.cliente_comex),
# MAGIC cte_cons2 AS (
# MAGIC   SELECT a.ano, a.max_data_faturamento, a.numero_invoice, a.codigo_do_cliente, a.cliente_comex,
# MAGIC          a.pais_do_cliente, a.pais_do_destino, a.cliente_tratado, b.origem
# MAGIC   FROM cte_cons1 a LEFT JOIN view_fato_risco_dist b ON b.numero_invoice = a.numero_invoice
# MAGIC   GROUP BY a.ano, a.max_data_faturamento, a.numero_invoice, a.codigo_do_cliente, a.cliente_comex,
# MAGIC            a.pais_do_cliente, a.pais_do_destino, a.cliente_tratado, b.origem
# MAGIC ),
# MAGIC cte_cons3 AS (
# MAGIC   SELECT a.*, b.cod_cliente_pagador, b.cliente_pagador,
# MAGIC     CASE WHEN b.rma_cliente_pagador = 'TBD' THEN 'MISSING INFORMATION' ELSE CAST(COALESCE(b.rma_cliente_pagador, 'MISSING INFORMATION') AS VARCHAR(25)) END AS rma_cliente_pagador,
# MAGIC     CASE WHEN b.rma_cliente_comex = 'TBD' THEN 'MISSING INFORMATION' ELSE CAST(COALESCE(b.rma_cliente_pagador, 'MISSING INFORMATION') AS VARCHAR(25)) END AS rma_cliente_comex
# MAGIC   FROM cte_cons2 a LEFT JOIN view_canal_cliente_dist b ON b.order_number = a.numero_invoice
# MAGIC   GROUP BY a.ano, a.max_data_faturamento, a.numero_invoice, a.codigo_do_cliente, a.cliente_comex,
# MAGIC            a.pais_do_cliente, a.pais_do_destino, a.cliente_tratado, a.origem,
# MAGIC            b.cod_cliente_pagador, b.cliente_pagador, b.rma_cliente_pagador, b.rma_cliente_comex
# MAGIC ),
# MAGIC cte_base_cad AS (
# MAGIC   SELECT a.max_data_faturamento, a.numero_invoice, a.codigo_do_cliente,
# MAGIC          COALESCE(a.cod_cliente_pagador, a.codigo_do_cliente) AS codigo_cliente_pagador,
# MAGIC          a.cliente_pagador, a.cliente_comex, a.cliente_tratado, a.rma_cliente_comex, a.rma_cliente_pagador,
# MAGIC          a.pais_do_cliente, a.pais_do_destino,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY a.codigo_do_cliente ORDER BY a.max_data_faturamento DESC, a.numero_invoice DESC) AS ordem
# MAGIC   FROM cte_cons3 a
# MAGIC   GROUP BY a.max_data_faturamento, a.numero_invoice, a.codigo_do_cliente, a.cod_cliente_pagador,
# MAGIC            a.cliente_pagador, a.cliente_comex, a.cliente_tratado, a.rma_cliente_comex, a.rma_cliente_pagador,
# MAGIC            a.pais_do_cliente, a.pais_do_destino
# MAGIC )
# MAGIC SELECT * FROM cte_base_cad WHERE ordem = 1

# COMMAND ----------

# DBTITLE 1,view_cadastro_cliente_final (FIX v3.2 — pais do pagador)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_cadastro_cliente_final AS
# MAGIC WITH cte_classificacao_item AS (
# MAGIC     SELECT tp.cod_pessoa AS client_code, tp.nom_pessoa AS client_name,
# MAGIC            tp.dta_nascimento AS company_foundation, tp.dta_cadastramento AS client_since,
# MAGIC            tp.ind_alerta, tp.ind_blacklist
# MAGIC     FROM de_data_lake_prd.dados_mestres.dbpessoa_tab_pessoa AS tp
# MAGIC     INNER JOIN de_data_lake_prd.faturamento.dbpessoa_tab_analise_credito AS tac ON tac.cod_pessoa = tp.cod_pessoa
# MAGIC     INNER JOIN de_data_lake_prd.dados_mestres.dbpessoa_tab_resultado_classificacao AS rc ON rc.num_proposta = tac.num_proposta
# MAGIC     WHERE tp.ind_pessoa_inativa = FALSE AND tp.ind_pessoa_estrangeira = 'S'
# MAGIC     GROUP BY tp.cod_pessoa, tp.nom_pessoa, tp.dta_nascimento, tp.dta_cadastramento, tp.ind_alerta, tp.ind_blacklist
# MAGIC )
# MAGIC SELECT a.codigo_do_cliente, a.codigo_cliente_pagador, a.cliente_comex, a.cliente_tratado,
# MAGIC        a.rma_cliente_comex, a.rma_cliente_pagador,
# MAGIC        COALESCE(pag.pais_do_cliente, a.pais_do_cliente) AS pais_do_cliente,
# MAGIC        a.pais_do_destino,
# MAGIC        CAST(b.company_foundation AS DATE) AS data_fundacao,
# MAGIC        CAST(b.client_since AS DATE) AS data_cadastro_minerva
# MAGIC FROM view_cadastro_cliente AS a
# MAGIC LEFT JOIN view_cadastro_cliente AS pag
# MAGIC   ON pag.codigo_do_cliente = a.codigo_cliente_pagador AND pag.codigo_do_cliente <> a.codigo_do_cliente
# MAGIC LEFT JOIN cte_classificacao_item AS b ON b.client_code = a.codigo_do_cliente
# MAGIC GROUP BY a.codigo_do_cliente, a.codigo_cliente_pagador, a.cliente_comex, a.cliente_tratado,
# MAGIC          a.rma_cliente_comex, a.rma_cliente_pagador,
# MAGIC          COALESCE(pag.pais_do_cliente, a.pais_do_cliente), a.pais_do_destino,
# MAGIC          CAST(b.company_foundation AS DATE), CAST(b.client_since AS DATE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Parcelas (baseline por tipo_risco + cambio 6 moedas)

# COMMAND ----------

# DBTITLE 1,view_base_parcelas (baseline original + cambio v3)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_base_parcelas AS
# MAGIC WITH cte_raw AS (
# MAGIC   SELECT fp.data_faturamento, fp.cod_pessoa_cliente, fp.nom_pessoa_cliente,
# MAGIC     fp.numero_invoice, fp.id_parcela, fp.pais_cliente, fp.pais_destino,
# MAGIC     fp.data_vencimento, fp.data_pagamento, fp.data_chegada_navio,
# MAGIC     fp.total_fatura, fp.moeda, fp.posicao_financeira, fp.forma_pagamento, fp.qtd_parcelas,
# MAGIC     ped.dta_abertura_pedido, ped.dta_chegada_destino, ped.tipo_risco,
# MAGIC     cad.data_fundacao, cad.data_cadastro_minerva,
# MAGIC     cad.pais_do_cliente AS pais_do_cliente_cadastro,
# MAGIC     cad.pais_do_destino AS pais_do_destino_cadastro,
# MAGIC     -- *** BASELINE ORIGINAL: diferenciado por tipo_risco ***
# MAGIC     CASE ped.tipo_risco
# MAGIC       WHEN 'RISCO PERFORMANCE' THEN COALESCE(fp.data_chegada_navio, fp.data_vencimento)
# MAGIC       ELSE COALESCE(fp.data_vencimento, fp.data_chegada_navio)
# MAGIC     END AS baseline_date,
# MAGIC     -- Conversao cambial 6 moedas (v3)
# MAGIC     CASE
# MAGIC       WHEN fp.moeda = 'CNY'  AND tx.taxa_cambio IS NULL     THEN ROUND(fp.total_fatura / 7, 2)
# MAGIC       WHEN fp.moeda = 'CNY'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(fp.total_fatura / tx.taxa_cambio, 2)
# MAGIC       WHEN fp.moeda = 'AED'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(fp.total_fatura / tx.taxa_cambio, 2)
# MAGIC       WHEN fp.moeda = 'ARS'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(fp.total_fatura / tx.taxa_cambio, 2)
# MAGIC       WHEN fp.moeda = 'REAL' AND tx.taxa_cambio IS NOT NULL THEN ROUND(fp.total_fatura / tx.taxa_cambio, 2)
# MAGIC       WHEN fp.moeda = 'EUR'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(fp.total_fatura * tx.taxa_cambio, 2)
# MAGIC       WHEN fp.moeda = 'AUD'  AND tx.taxa_cambio IS NOT NULL THEN ROUND(fp.total_fatura * tx.taxa_cambio, 2)
# MAGIC       ELSE fp.total_fatura
# MAGIC     END AS valor_final,
# MAGIC     -- Raw delay para filtro > -180
# MAGIC     CASE ped.tipo_risco
# MAGIC       WHEN 'RISCO PERFORMANCE' THEN DATEDIFF(fp.data_pagamento, COALESCE(fp.data_chegada_navio, fp.data_vencimento))
# MAGIC       ELSE DATEDIFF(fp.data_pagamento, COALESCE(fp.data_vencimento, fp.data_chegada_navio))
# MAGIC     END AS raw_delay_for_filter,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY fp.numero_invoice ORDER BY COALESCE(fp.data_pagamento, '9999-12-31') ASC, fp.id_parcela ASC
# MAGIC     ) AS numero_classificacao
# MAGIC   FROM view_posicao_financeira AS fp
# MAGIC   INNER JOIN view_principal_p_base_unificada AS ped
# MAGIC     ON ped.num_pedido_comex = fp.numero_invoice AND ped.cod_pessoa_cliente = fp.cod_pessoa_cliente
# MAGIC   LEFT JOIN view_cadastro_cliente_final AS cad ON cad.codigo_do_cliente = fp.cod_pessoa_cliente
# MAGIC   LEFT JOIN view_taxas_cambio AS tx
# MAGIC     ON tx.data_taxa = fp.data_faturamento
# MAGIC     AND tx.moeda_procedencia = CASE WHEN fp.moeda = 'REAL' THEN 'BRL' ELSE fp.moeda END
# MAGIC   WHERE fp.nom_pessoa_cliente NOT LIKE 'MINER%' AND fp.nom_pessoa_cliente NOT LIKE 'SWIF%'
# MAGIC     AND (ped.dta_vencimento IS NOT NULL OR ped.dta_chegada_destino IS NOT NULL)
# MAGIC )
# MAGIC SELECT * FROM cte_raw
# MAGIC WHERE (raw_delay_for_filter > -180 OR raw_delay_for_filter IS NULL) AND valor_final IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento Multi-Safra Incremental (v3)

# COMMAND ----------

# DBTITLE 1,serie_safras (incremential multi-safra)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW serie_safras AS
# MAGIC WITH controle AS (
# MAGIC   SELECT ultima_safra_processada, data_referencia FROM controle_processamento
# MAGIC ),
# MAGIC periodo AS (
# MAGIC   SELECT
# MAGIC     CASE WHEN c.ultima_safra_processada IS NULL
# MAGIC       THEN date_trunc('month', (SELECT MIN(dta_abertura_pedido) FROM view_base_parcelas WHERE dta_abertura_pedido IS NOT NULL))
# MAGIC       ELSE add_months(c.ultima_safra_processada, 1)
# MAGIC     END AS mes_inicial,
# MAGIC     add_months(date_trunc('month', c.data_referencia), -1) AS mes_final
# MAGIC   FROM controle c
# MAGIC ),
# MAGIC meses AS (
# MAGIC   SELECT add_months(p.mes_inicial, seq.pos) AS mes_safra
# MAGIC   FROM periodo p
# MAGIC   LATERAL VIEW EXPLODE(SEQUENCE(0, GREATEST(CAST(months_between(p.mes_final, p.mes_inicial) AS INT), 0), 1)) seq AS pos
# MAGIC )
# MAGIC SELECT mes_safra, last_day(mes_safra) AS ultimo_dia_safra,
# MAGIC        add_months(mes_safra, -11) AS inicio_janela
# MAGIC FROM meses
# MAGIC WHERE mes_safra <= add_months(date_trunc('month', (SELECT data_referencia FROM controle_processamento)), -1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculo de Atraso (baseline original + truncamento no ponto de corte)

# COMMAND ----------

# DBTITLE 1,view_parcelas_com_atraso (multi-safra, baseline original)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_parcelas_com_atraso AS
# MAGIC SELECT p.cod_pessoa_cliente, p.nom_pessoa_cliente, p.numero_invoice, p.id_parcela,
# MAGIC   p.data_faturamento, p.data_vencimento, p.data_pagamento, p.data_chegada_navio,
# MAGIC   p.baseline_date, p.valor_final, p.tipo_risco, p.pais_cliente,
# MAGIC   p.pais_do_cliente_cadastro, p.pais_destino,
# MAGIC   p.data_fundacao, p.data_cadastro_minerva, p.dta_abertura_pedido, p.numero_classificacao,
# MAGIC   s.mes_safra, s.ultimo_dia_safra,
# MAGIC   -- *** CALCULO ORIGINAL: truncamento no ponto de corte unico ***
# MAGIC   CASE
# MAGIC     WHEN p.baseline_date > s.ultimo_dia_safra THEN 0
# MAGIC     WHEN p.data_pagamento IS NOT NULL AND p.data_pagamento <= s.ultimo_dia_safra
# MAGIC       THEN DATEDIFF(p.data_pagamento, p.baseline_date)
# MAGIC     WHEN p.data_pagamento IS NOT NULL AND p.data_pagamento > s.ultimo_dia_safra
# MAGIC       THEN DATEDIFF(s.ultimo_dia_safra, p.baseline_date)
# MAGIC     WHEN p.data_pagamento IS NULL
# MAGIC       THEN DATEDIFF(s.ultimo_dia_safra, p.baseline_date)
# MAGIC     ELSE 0
# MAGIC   END AS dias_atraso
# MAGIC FROM view_base_parcelas AS p
# MAGIC CROSS JOIN serie_safras AS s
# MAGIC WHERE date_trunc('month', p.dta_abertura_pedido) >= s.inicio_janela
# MAGIC   AND date_trunc('month', p.dta_abertura_pedido) <= s.mes_safra

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregacoes: Parcela → Invoice → Cliente/Mes → Consolidado

# COMMAND ----------

# DBTITLE 1,view_invoice_consolidado
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_invoice_consolidado AS
# MAGIC SELECT cod_pessoa_cliente, nom_pessoa_cliente, numero_invoice,
# MAGIC   mes_safra, ultimo_dia_safra,
# MAGIC   date_trunc('month', dta_abertura_pedido) AS mes_abertura_pedido,
# MAGIC   data_faturamento, dta_abertura_pedido,
# MAGIC   pais_cliente, pais_do_cliente_cadastro, pais_destino,
# MAGIC   data_fundacao, data_cadastro_minerva, tipo_risco,
# MAGIC   SUM(valor_final) AS valor_total_invoice,
# MAGIC   MAX(dias_atraso) AS maior_atraso_invoice,
# MAGIC   COALESCE(SUM(CASE WHEN dias_atraso > 7 THEN valor_final ELSE 0 END), 0) AS valor_atrasado_invoice,
# MAGIC   MAX(CASE WHEN numero_classificacao = 1 THEN dias_atraso END) AS atraso_1_parcela,
# MAGIC   MAX(CASE WHEN numero_classificacao > 1 THEN dias_atraso END) AS maior_atraso_parcelas_restantes
# MAGIC FROM view_parcelas_com_atraso
# MAGIC GROUP BY cod_pessoa_cliente, nom_pessoa_cliente, numero_invoice,
# MAGIC          mes_safra, ultimo_dia_safra, data_faturamento, dta_abertura_pedido,
# MAGIC          pais_cliente, pais_do_cliente_cadastro, pais_destino,
# MAGIC          data_fundacao, data_cadastro_minerva, tipo_risco

# COMMAND ----------

# DBTITLE 1,view_cliente_por_mes_abertura
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_cliente_por_mes_abertura AS
# MAGIC SELECT cod_pessoa_cliente, nom_pessoa_cliente, mes_abertura_pedido, mes_safra, ultimo_dia_safra,
# MAGIC   MAX(pais_do_cliente_cadastro) AS pais_do_cliente,
# MAGIC   MAX(pais_destino) AS pais_do_destino,
# MAGIC   MAX(data_fundacao) AS data_fundacao, MAX(data_cadastro_minerva) AS data_cadastro_minerva,
# MAGIC   COUNT(DISTINCT numero_invoice) AS qtd_pedidos,
# MAGIC   COALESCE(SUM(valor_total_invoice), 0) AS valor_total,
# MAGIC   COALESCE(MAX(maior_atraso_invoice), 0) AS max_delay,
# MAGIC   COALESCE(SUM(valor_atrasado_invoice), 0) AS valor_atraso_target,
# MAGIC   MAX(CASE WHEN valor_atrasado_invoice > 0 THEN 1 ELSE 0 END) AS target_mes,
# MAGIC   SUM(CASE WHEN valor_atrasado_invoice > 0 THEN 1 ELSE 0 END) AS qtd_invoices_atrasados
# MAGIC FROM view_invoice_consolidado
# MAGIC GROUP BY cod_pessoa_cliente, nom_pessoa_cliente, mes_abertura_pedido, mes_safra, ultimo_dia_safra

# COMMAND ----------

# DBTITLE 1,view_cliente_consolidado (4 faixas, escala %)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW view_cliente_consolidado AS
# MAGIC WITH cte_mes_com_pct AS (
# MAGIC   SELECT *, 
# MAGIC     CASE WHEN valor_total = 0 THEN 0 ELSE ROUND(valor_atraso_target / valor_total * 100, 2) END AS pct_atraso_mes
# MAGIC   FROM view_cliente_por_mes_abertura
# MAGIC   WHERE valor_total > 0
# MAGIC )
# MAGIC SELECT cod_pessoa_cliente, nom_pessoa_cliente, mes_safra, ultimo_dia_safra,
# MAGIC   MAX(pais_do_cliente) AS pais_do_cliente, MAX(pais_do_destino) AS pais_do_destino,
# MAGIC   MAX(data_fundacao) AS data_fundacao, MAX(data_cadastro_minerva) AS data_cadastro_minerva,
# MAGIC   SUM(qtd_pedidos) AS qtd_pedidos,
# MAGIC   COALESCE(SUM(valor_total), 0) AS valor_total,
# MAGIC   COALESCE(MAX(max_delay), 0) AS max_delay,
# MAGIC   COALESCE(SUM(valor_atraso_target), 0) AS valor_atraso_target,
# MAGIC   MAX(target_mes) AS target_risco_2,
# MAGIC   -- Meses com faturamento
# MAGIC   COUNT(DISTINCT mes_abertura_pedido) AS qtd_safras_com_faturamento,
# MAGIC   -- Meses inadimplentes: >7d E >=10%
# MAGIC   SUM(CASE WHEN target_mes = 1 AND pct_atraso_mes >= 10 THEN 1 ELSE 0 END) AS qtd_safras_inadimplentes,
# MAGIC   -- 4 faixas em ESCALA % (original)
# MAGIC   ROUND(SUM(CASE WHEN pct_atraso_mes >= 10 AND pct_atraso_mes < 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT mes_abertura_pedido), 2) AS pct_meses_10_20,
# MAGIC   ROUND(SUM(CASE WHEN pct_atraso_mes >= 20 AND pct_atraso_mes < 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT mes_abertura_pedido), 2) AS pct_meses_20_30,
# MAGIC   ROUND(SUM(CASE WHEN pct_atraso_mes >= 30 AND pct_atraso_mes < 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT mes_abertura_pedido), 2) AS pct_meses_30_50,
# MAGIC   ROUND(SUM(CASE WHEN pct_atraso_mes >= 50 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT mes_abertura_pedido), 2) AS pct_meses_50_mais,
# MAGIC   -- Flag transacionado
# MAGIC   MAX(CASE WHEN mes_abertura_pedido = mes_safra THEN 1 ELSE 0 END) AS flag_transacted
# MAGIC FROM cte_mes_com_pct
# MAGIC GROUP BY cod_pessoa_cliente, nom_pessoa_cliente, mes_safra, ultimo_dia_safra

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medianas, Score Individual e Portfolio

# COMMAND ----------

# DBTITLE 1,tab_medianas_atraso (4 faixas, escala %, PERCENTILE_CONT inline)
# MAGIC %sql
# MAGIC -- Mediana real por cluster: PERCENTILE_CONT com CASE inline
# MAGIC -- Padrao do 02_apply_model original
# MAGIC CREATE OR REPLACE TEMP VIEW tab_medianas_atraso AS
# MAGIC WITH dados_classificados AS (
# MAGIC   SELECT
# MAGIC     mes_safra AS safra,
# MAGIC     ROUND(valor_atraso_target / valor_total * 100, 2) AS overdue_pct,
# MAGIC     CASE
# MAGIC       WHEN (valor_atraso_target / valor_total * 100) >= 50 THEN '50plus'
# MAGIC       WHEN (valor_atraso_target / valor_total * 100) >= 30 THEN '30_50'
# MAGIC       WHEN (valor_atraso_target / valor_total * 100) >= 20 THEN '20_30'
# MAGIC       WHEN (valor_atraso_target / valor_total * 100) >= 10 THEN '10_20'
# MAGIC       ELSE NULL
# MAGIC     END AS cluster
# MAGIC   FROM view_cliente_consolidado
# MAGIC   WHERE valor_total > 0
# MAGIC )
# MAGIC SELECT
# MAGIC   safra,
# MAGIC   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN cluster = '10_20'  THEN overdue_pct END), 2) AS mediana_cluster_10,
# MAGIC   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN cluster = '20_30'  THEN overdue_pct END), 2) AS mediana_cluster_20,
# MAGIC   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN cluster = '30_50'  THEN overdue_pct END), 2) AS mediana_cluster_30,
# MAGIC   ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN cluster = '50plus' THEN overdue_pct END), 2) AS mediana_cluster_50
# MAGIC FROM dados_classificados
# MAGIC WHERE cluster IS NOT NULL
# MAGIC GROUP BY safra
# MAGIC ORDER BY safra

# COMMAND ----------

# DBTITLE 1,scores_individuais (escala original: pct*med/10000)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW scores_individuais AS
# MAGIC SELECT
# MAGIC   cc.cod_pessoa_cliente, cc.mes_safra,
# MAGIC   cc.qtd_safras_com_faturamento AS months_with_billing,
# MAGIC   cc.valor_total,
# MAGIC   -- Score = Σ(pct × mediana) / 10000  (ambos em escala %)
# MAGIC   ROUND(
# MAGIC     ((cc.pct_meses_10_20 * COALESCE(m.mediana_cluster_10, 0)) +
# MAGIC      (cc.pct_meses_20_30 * COALESCE(m.mediana_cluster_20, 0)) +
# MAGIC      (cc.pct_meses_30_50 * COALESCE(m.mediana_cluster_30, 0)) +
# MAGIC      (cc.pct_meses_50_mais * COALESCE(m.mediana_cluster_50, 0))) / 10000, 6
# MAGIC   ) AS score,
# MAGIC   -- Peso historico
# MAGIC   ROUND(LEAST(1.0, CAST(cc.qtd_safras_com_faturamento AS DOUBLE) / 3.0), 4) AS historical_weight
# MAGIC FROM view_cliente_consolidado cc
# MAGIC LEFT JOIN tab_medianas_atraso m ON cc.mes_safra = m.safra

# COMMAND ----------

# MAGIC %sql
# MAGIC CACHE TABLE scores_individuais

# COMMAND ----------

# DBTITLE 1,view_resultado_carteira (inadimplentes, escala %)
# MAGIC %sql
# MAGIC -- Portfolio = Σ(fracao_inadimpl_k × mediana_k) / 10000
# MAGIC -- Denominador = SUM(safras_inadimplentes) — bons pagadores NAO diluem
# MAGIC -- /10000 porque pct (0-100) × mediana (0-100) = escala 0-10000
# MAGIC CREATE OR REPLACE TEMP VIEW view_resultado_carteira AS
# MAGIC SELECT
# MAGIC   cc.mes_safra AS reference_month,
# MAGIC   -- Fracoes de inadimplencia por faixa
# MAGIC   ROUND(SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_10_20) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0), 4) AS fracao_inadimpl_10_7,
# MAGIC   ROUND(SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_20_30) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0), 4) AS fracao_inadimpl_20_7,
# MAGIC   ROUND(SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_30_50) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0), 4) AS fracao_inadimpl_30_7,
# MAGIC   ROUND(SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_50_mais) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0), 4) AS fracao_inadimpl_50_7,
# MAGIC   -- Portfolio score (escala 0-1)
# MAGIC   ROUND(
# MAGIC     ((SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_10_20) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0)) * AVG(m.mediana_cluster_10) +
# MAGIC      (SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_20_30) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0)) * AVG(m.mediana_cluster_20) +
# MAGIC      (SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_30_50) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0)) * AVG(m.mediana_cluster_30) +
# MAGIC      (SUM(cc.qtd_safras_com_faturamento * cc.pct_meses_50_mais) / NULLIF(SUM(cc.qtd_safras_inadimplentes), 0)) * AVG(m.mediana_cluster_50)
# MAGIC     ) / 10000, 4
# MAGIC   ) AS portfolio_score
# MAGIC FROM view_cliente_consolidado cc
# MAGIC LEFT JOIN tab_medianas_atraso m ON cc.mes_safra = m.safra
# MAGIC WHERE cc.valor_total > 0
# MAGIC GROUP BY cc.mes_safra

# COMMAND ----------

# DBTITLE 1,DROP + CREATE portfolio_abt_group_me_br_v7
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ds_catalog_dev.credit_engine.portfolio_abt_group_me_br;
# MAGIC CREATE TABLE ds_catalog_dev.credit_engine.portfolio_abt_group_me_br (
# MAGIC   reference_month          DATE,
# MAGIC   median_cluster_10        DOUBLE COMMENT 'Mediana 10-20% (escala %)',
# MAGIC   median_cluster_20        DOUBLE COMMENT 'Mediana 20-30% (escala %)',
# MAGIC   median_cluster_30        DOUBLE COMMENT 'Mediana 30-50% (escala %)',
# MAGIC   median_cluster_50        DOUBLE COMMENT 'Mediana 50%+ (escala %)',
# MAGIC   fracao_inadimpl_10_7     DOUBLE COMMENT 'Fracao safras inadimpl faixa 10-20%',
# MAGIC   fracao_inadimpl_20_7     DOUBLE COMMENT 'Fracao safras inadimpl faixa 20-30%',
# MAGIC   fracao_inadimpl_30_7     DOUBLE COMMENT 'Fracao safras inadimpl faixa 30-50%',
# MAGIC   fracao_inadimpl_50_7     DOUBLE COMMENT 'Fracao safras inadimpl faixa 50%+',
# MAGIC   portfolio_score          DOUBLE COMMENT 'Benchmark = sum(fracao_k * mediana_k) / 10000',
# MAGIC   updated_at               TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 1,MERGE portfolio_abt_group_me_br_v7
# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.credit_engine.portfolio_abt_group_me_br AS target
# MAGIC USING (
# MAGIC   SELECT m.safra AS reference_month,
# MAGIC     m.mediana_cluster_10 AS median_cluster_10,
# MAGIC     m.mediana_cluster_20 AS median_cluster_20,
# MAGIC     m.mediana_cluster_30 AS median_cluster_30,
# MAGIC     m.mediana_cluster_50 AS median_cluster_50,
# MAGIC     rc.fracao_inadimpl_10_7,
# MAGIC     rc.fracao_inadimpl_20_7,
# MAGIC     rc.fracao_inadimpl_30_7,
# MAGIC     rc.fracao_inadimpl_50_7,
# MAGIC     COALESCE(rc.portfolio_score, 0.30) AS portfolio_score,
# MAGIC     current_timestamp() AS updated_at
# MAGIC   FROM tab_medianas_atraso m
# MAGIC   LEFT JOIN view_resultado_carteira rc ON m.safra = rc.reference_month
# MAGIC ) AS source ON target.reference_month = source.reference_month
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 1,view_grupo_economico (id_customer_group_economic — logica me_compiled)
# MAGIC
# MAGIC %sql
# MAGIC -- Replica cod_pessoa_cliente_grupo_economico do me_compiled (view_base_ultimo_mes_6)
# MAGIC -- Fonte: de_data_lake_prd.dados_mestres.dbpessoa_tab_empresas_relacionadas, cod_tipo_relacionamento = 4
# MAGIC -- Regra: se cliente tem empresa pai, usa cod_pessoa_empresa_pai; senao usa cod_pessoa_cliente
# MAGIC CREATE OR REPLACE TEMP VIEW view_grupo_economico AS
# MAGIC SELECT
# MAGIC   x.cod_pessoa_empresa_filha AS cod_pessoa_cliente,
# MAGIC   MAX(x.cod_pessoa_empresa_pai)   AS cod_pessoa_empresa_pai
# MAGIC FROM de_data_lake_prd.dados_mestres.dbpessoa_tab_empresas_relacionadas AS x
# MAGIC WHERE x.cod_tipo_relacionamento = 4
# MAGIC GROUP BY x.cod_pessoa_empresa_filha

# COMMAND ----------

# MAGIC %md
# MAGIC ## Media de Exposicao (reference_value) e Prazo Medio (payment_term)
# MAGIC
# MAGIC Logica replicada EXATAMENTE do me_compiled.sql (view_base_ultimo_mes_5 e view_base_ultimo_mes_6).
# MAGIC
# MAGIC **`reference_value`** = media mensal de exposicao dos ultimos 24 meses:
# MAGIC - **Fonte**: `view_base_parcelas` (mesma fonte de `total_amount`, ja convertida para USD)
# MAGIC - **Data anchor**: `dta_abertura_pedido` (mesma data de `total_amount`)
# MAGIC - **Janela**: 24 meses anteriores a `mes_safra` inclusive
# MAGIC - **Calculo**: `AVG(soma_mensal)` sobre meses com dados na janela
# MAGIC - **Garantia**: se `total_amount > 0` → `reference_value > 0` (mesma fonte, imposivel divergir)
# MAGIC
# MAGIC **`reference_value_clp`**: reservado (0 para ME BR — sem operacoes em CLP)
# MAGIC
# MAGIC **`payment_term`** = `prazo_doas_fat_venc_24_m` / 30:
# MAGIC - Agrega ao nivel de **pedido** (numero_invoice): `MAX(GREATEST(dif_pago_fat, dif_venc_fat))`
# MAGIC - `safra_dt_ultimo_pgto` = `last_day(MAX(data_pagamento))` por pedido
# MAGIC - Janela: `datediff(month, safra_dt_ultimo_pgto, mes_safra) BETWEEN 1 AND 24` (calendario, nao add_months)
# MAGIC - Filtro estrito: `mes_safra > safra_dt_ultimo_pgto`
# MAGIC - Clip: se <= 0 ou >= 5, usa 1.0
# MAGIC
# MAGIC **`id_customer_group_economic`** = `cod_pessoa_cliente_grupo_economico` do motor:
# MAGIC - `COALESCE(cod_pessoa_empresa_pai, cod_pessoa_cliente)`
# MAGIC - Fonte: `de_data_lake_prd.dados_mestres.dbpessoa_tab_empresas_relacionadas` (cod_tipo_relacionamento = 4)

# COMMAND ----------

# DBTITLE 1,view_exposicao_maxima (reference_value v3 — total_amount + pedidos sem fatura)
# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- LOGICA v3: base total_amount + pedidos sem fatura (regra de negocio preservada)
# MAGIC -- =============================================================================
# MAGIC --
# MAGIC -- COMPONENTES:
# MAGIC --
# MAGIC -- [1] faturamento_mensal_24m — base principal (mesma logica do total_amount)
# MAGIC --     Fonte : view_base_parcelas (posicao financeira, ja convertida para USD)
# MAGIC --     Data  : dta_abertura_pedido
# MAGIC --     Janela: 24 meses
# MAGIC --     Garante: reference_value > 0 sempre que total_amount > 0 — mesma fonte, mesmo universo.
# MAGIC --
# MAGIC -- [2] sem_fatura_mensal — pedidos aprovados mas ainda nao faturados (regra de negocio)
# MAGIC --     Fonte : flat_orders_external_market
# MAGIC --     Filtro: invoice_date IS NULL AND credit_approval_date IS NOT NULL
# MAGIC --     Data  : credit_approval_date
# MAGIC --     Janela: 24 meses
# MAGIC --     Regra Israel: excluido (igual ao me_compiled original).
# MAGIC --     Comportamento: se o cliente nao existe em flat_orders, contribui com 0 (LEFT JOIN).
# MAGIC --     A base [1] ja garante que o reference_value nunca fica zerado por essa ausencia.
# MAGIC --
# MAGIC -- CALCULO FINAL (nao-Israel):
# MAGIC --   reference_value = AVG( soma_faturamento_mensal + total_pedido_sem_fatura_mensal )
# MAGIC --   Para meses sem pedido sem fatura, total_pedido_sem_fatura = 0 (nao contamina a media).
# MAGIC --
# MAGIC -- CALCULO FINAL (Israel):
# MAGIC --   reference_value = AVG( soma_faturamento_mensal ) — sem_fatura excluido, igual ao original.
# MAGIC CREATE OR REPLACE TEMP VIEW view_exposicao_maxima AS
# MAGIC WITH
# MAGIC
# MAGIC -- Componente 1: faturamento mensal — mesma fonte e logica do total_amount
# MAGIC faturamento_mensal_24m AS (
# MAGIC   SELECT
# MAGIC     p.cod_pessoa_cliente,
# MAGIC     date_trunc('month', p.dta_abertura_pedido) AS mes_pedido,
# MAGIC     SUM(p.valor_final)                          AS soma_mensal
# MAGIC   FROM view_base_parcelas p
# MAGIC   WHERE p.dta_abertura_pedido IS NOT NULL
# MAGIC     AND p.valor_final > 0
# MAGIC   GROUP BY p.cod_pessoa_cliente, date_trunc('month', p.dta_abertura_pedido)
# MAGIC ),
# MAGIC
# MAGIC -- Componente 2: pedidos sem fatura (credit_approval_date IS NOT NULL, invoice_date IS NULL)
# MAGIC -- Preserva a regra de negocio original: exposicao comprometida ainda nao faturada.
# MAGIC -- Fonte: flat_orders_external_market — unico lugar com credit_approval_date.
# MAGIC -- Conversao de moeda: mesmas regras do me_compiled original.
# MAGIC -- Excluido para Israel (regra original preservada).
# MAGIC sem_fatura_mensal AS (
# MAGIC   SELECT
# MAGIC     a.importer_id                            AS cod_pessoa_cliente,
# MAGIC     date_trunc('month', a.credit_approval_date) AS mes_pedido,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN a.currency_id = 'CNY'  AND tc.taxa_cambio IS NULL     THEN ROUND(a.total_order_value / 7, 2)
# MAGIC         WHEN a.currency_id = 'CNY'  AND tc.taxa_cambio IS NOT NULL THEN ROUND(a.total_order_value / tc.taxa_cambio, 2)
# MAGIC         WHEN a.currency_id = 'AED'  AND tc.taxa_cambio IS NOT NULL THEN ROUND(a.total_order_value / tc.taxa_cambio, 2)
# MAGIC         WHEN a.currency_id = 'ARS'  AND tc.taxa_cambio IS NOT NULL THEN ROUND(a.total_order_value / tc.taxa_cambio, 2)
# MAGIC         WHEN a.currency_id = 'REAL' AND tc.taxa_cambio IS NOT NULL THEN ROUND(a.total_order_value / tc.taxa_cambio, 2)
# MAGIC         WHEN a.currency_id = 'EUR'  AND tc.taxa_cambio IS NOT NULL THEN ROUND(a.total_order_value * tc.taxa_cambio, 2)
# MAGIC         WHEN a.currency_id = 'AUD'  AND tc.taxa_cambio IS NOT NULL THEN ROUND(a.total_order_value * tc.taxa_cambio, 2)
# MAGIC         ELSE a.total_order_value
# MAGIC       END
# MAGIC     ) AS total_pedido_sem_fatura
# MAGIC   FROM de_data_lake_prd.business_analytics.flat_orders_external_market AS a
# MAGIC   LEFT JOIN view_taxas_cambio AS tc
# MAGIC     ON  tc.data_taxa         = a.credit_approval_date
# MAGIC     AND tc.moeda_procedencia = CASE WHEN a.currency_id = 'REAL' THEN 'BRL' ELSE a.currency_id END
# MAGIC   WHERE a.invoice_date          IS NULL
# MAGIC     AND a.credit_approval_date  IS NOT NULL
# MAGIC     AND a.last_payment_date     IS NULL
# MAGIC     AND a.order_status          NOT IN ('Cancelado', 'Aguardando')
# MAGIC     AND a.importer_name         NOT LIKE 'MINER%'
# MAGIC     AND a.importer_name         NOT LIKE 'SWIF%'
# MAGIC   GROUP BY a.importer_id, date_trunc('month', a.credit_approval_date)
# MAGIC )
# MAGIC
# MAGIC -- SELECT FINAL: media mensal por cliente/safra
# MAGIC -- Para nao-Israel: AVG( faturamento + sem_fatura ) por mes
# MAGIC -- Para Israel    : AVG( faturamento ) por mes — sem_fatura excluido
# MAGIC -- LEFT JOIN duplo: se o cliente nao tiver sem_fatura (ausente em flat_orders),
# MAGIC --   COALESCE garante 0 para esse componente — o faturamento continua sendo usado.
# MAGIC SELECT
# MAGIC   cc.mes_safra,
# MAGIC   cc.cod_pessoa_cliente,
# MAGIC   ROUND(
# MAGIC     CASE
# MAGIC       WHEN MAX(cc.pais_do_destino) = 'ISRAEL'
# MAGIC         THEN COALESCE(AVG(
# MAGIC                CASE
# MAGIC                  WHEN f.mes_pedido >= add_months(cc.mes_safra, -24)
# MAGIC                   AND f.mes_pedido <= cc.mes_safra
# MAGIC                  THEN f.soma_mensal
# MAGIC                END
# MAGIC              ), 0)
# MAGIC       ELSE
# MAGIC         COALESCE(AVG(
# MAGIC             -- faturamento do mes (posicao financeira)
# MAGIC             COALESCE(
# MAGIC               CASE
# MAGIC                 WHEN f.mes_pedido >= add_months(cc.mes_safra, -24)
# MAGIC                  AND f.mes_pedido <= cc.mes_safra
# MAGIC                 THEN f.soma_mensal
# MAGIC               END,
# MAGIC             0)
# MAGIC             +
# MAGIC             -- pedidos sem fatura do mes (flat_orders)
# MAGIC             COALESCE(
# MAGIC               CASE
# MAGIC                 WHEN s.mes_pedido >= add_months(cc.mes_safra, -24)
# MAGIC                  AND s.mes_pedido <= cc.mes_safra
# MAGIC                 THEN s.total_pedido_sem_fatura
# MAGIC               END,
# MAGIC             0)
# MAGIC         ), 0)
# MAGIC     END
# MAGIC   , 4) AS reference_value
# MAGIC FROM view_cliente_consolidado cc
# MAGIC LEFT JOIN faturamento_mensal_24m f
# MAGIC   ON  f.cod_pessoa_cliente = cc.cod_pessoa_cliente
# MAGIC   AND f.mes_pedido >= add_months(cc.mes_safra, -24)
# MAGIC   AND f.mes_pedido <= cc.mes_safra
# MAGIC LEFT JOIN sem_fatura_mensal s
# MAGIC   ON  s.cod_pessoa_cliente = cc.cod_pessoa_cliente
# MAGIC   AND s.mes_pedido >= add_months(cc.mes_safra, -24)
# MAGIC   AND s.mes_pedido <= cc.mes_safra
# MAGIC GROUP BY cc.mes_safra, cc.cod_pessoa_cliente

# COMMAND ----------

# DBTITLE 1,view_prazo_medio (payment_term — logica EXATA me_compiled prazo_doas_fat_venc_24_m)
# MAGIC %sql
# MAGIC -- Replica EXATAMENTE prazo_doas_fat_venc_24_m do me_compiled (view_base_ultimo_mes_6)
# MAGIC --
# MAGIC -- Diferencas criticas em relacao a versao anterior:
# MAGIC --   1. Agrega ao nivel de PEDIDO (numero_invoice), nao de parcela
# MAGIC --      prazo_dias_fat_venc por pedido = MAX(GREATEST(dif_pago_fat, dif_venc_fat)) sobre as parcelas
# MAGIC --   2. Janela: datediff(month, safra_dt_ultimo_pgto, mes_safra) BETWEEN 1 AND 24
# MAGIC --      (datediff de calendario, nao add_months)
# MAGIC --   3. Filtro estrito: mes_safra > safra_dt_ultimo_pgto (pagamento em mes anterior a safra)
# MAGIC --   4. Clip identico: se resultado <= 0 ou >= 5, usa 1.0
# MAGIC CREATE OR REPLACE TEMP VIEW view_prazo_medio AS
# MAGIC WITH
# MAGIC
# MAGIC -- Agrupamento ao nivel de pedido (invoice)
# MAGIC -- equivale a view_base_cliente_pedido (customer_order_base) do me_compiled
# MAGIC prazo_por_pedido AS (
# MAGIC   SELECT
# MAGIC     p.cod_pessoa_cliente,
# MAGIC     p.numero_invoice,
# MAGIC     last_day(MAX(p.data_pagamento))          AS safra_dt_ultimo_pgto,  -- ancora de safra, mantida
# MAGIC     MAX(COALESCE(
# MAGIC       DATEDIFF(p.data_vencimento, p.data_faturamento), 0
# MAGIC     ))                                       AS prazo_dias_fat_venc    -- max prazo contratual do pedido
# MAGIC   FROM view_base_parcelas p
# MAGIC   WHERE p.data_faturamento IS NOT NULL
# MAGIC     AND p.data_vencimento  IS NOT NULL       -- filtro mudou: era data_pagamento
# MAGIC     AND p.valor_final > 0
# MAGIC   GROUP BY p.cod_pessoa_cliente, p.numero_invoice
# MAGIC ),
# MAGIC
# MAGIC -- Juncao com serie_safras aplicando a janela 1-24m por datediff de calendario
# MAGIC -- equivale ao LEFT JOIN da cte do view_base_ultimo_mes_6
# MAGIC prazo_janela AS (
# MAGIC   SELECT
# MAGIC     o.cod_pessoa_cliente,
# MAGIC     s.mes_safra,
# MAGIC     o.prazo_dias_fat_venc
# MAGIC   FROM prazo_por_pedido o
# MAGIC   CROSS JOIN serie_safras s
# MAGIC   WHERE s.mes_safra > o.safra_dt_ultimo_pgto
# MAGIC     AND datediff(month, o.safra_dt_ultimo_pgto, s.mes_safra) BETWEEN 1 AND 24
# MAGIC )
# MAGIC
# MAGIC SELECT mes_safra, cod_pessoa_cliente,
# MAGIC   ROUND(
# MAGIC     CASE
# MAGIC       WHEN COALESCE(AVG(prazo_dias_fat_venc), 0) / 30.0 <= 1 THEN 1.0
# MAGIC       WHEN COALESCE(AVG(prazo_dias_fat_venc), 0) / 30.0 >= 5 THEN 1.0
# MAGIC       ELSE COALESCE(AVG(prazo_dias_fat_venc), 0) / 30.0
# MAGIC     END
# MAGIC   , 4) AS payment_term
# MAGIC FROM prazo_janela
# MAGIC GROUP BY mes_safra, cod_pessoa_cliente

# COMMAND ----------

# DBTITLE 1,MERGE abt_inference_me_br_v7
# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.credit_engine.abt_inference_me_br AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     CAST(cc.cod_pessoa_cliente AS INT) AS id_customer,
# MAGIC     cc.nom_pessoa_cliente AS customer_name,
# MAGIC     cc.pais_do_cliente AS country,
# MAGIC     cc.mes_safra AS reference_month,
# MAGIC     ROUND(cc.valor_total, 2) AS total_amount,
# MAGIC     ROUND(cc.valor_atraso_target, 2) AS overdue_amount,
# MAGIC     CASE WHEN cc.valor_total = 0 THEN 0 ELSE ROUND(cc.valor_atraso_target / cc.valor_total * 100, 2) END AS overdue_pct,
# MAGIC     cc.target_risco_2 AS target,
# MAGIC     cc.qtd_safras_com_faturamento AS months_with_billing,
# MAGIC     cc.qtd_safras_inadimplentes AS months_defaulted,
# MAGIC     cc.pct_meses_10_20 AS pct_months_overdue_10_20,
# MAGIC     cc.pct_meses_20_30 AS pct_months_overdue_20_30,
# MAGIC     cc.pct_meses_30_50 AS pct_months_overdue_30_50,
# MAGIC     cc.pct_meses_50_mais AS pct_months_overdue_50_plus,
# MAGIC     si.score,
# MAGIC     si.historical_weight,
# MAGIC     cc.flag_transacted,
# MAGIC     -- Novas colunas: exposicao e prazo (logica motor de credito)
# MAGIC     COALESCE(em.reference_value, 0) AS reference_value,
# MAGIC     -- reference_value_clp: converte USD -> CLP usando taxa do dia (forward-fill)
# MAGIC     -- USD * taxa_clp_usd = CLP  (ex: 100 USD * 900 = 90.000 CLP)
# MAGIC     -- Se taxa nao disponivel, retorna 0 (COALESCE garante seguranca)
# MAGIC     ROUND(COALESCE(em.reference_value, 0) * COALESCE(clp.taxa_clp_usd, 0), 2) AS reference_value_clp,
# MAGIC     COALESCE(pm.payment_term, 1.0) AS payment_term,
# MAGIC     -- Grupo economico: cod_pessoa_empresa_pai se existir, senao cod_pessoa_cliente
# MAGIC     COALESCE(ge.cod_pessoa_empresa_pai, cc.cod_pessoa_cliente) AS id_customer_group_economic,
# MAGIC     (SELECT data_atualizacao FROM controle_processamento) AS updated_at
# MAGIC   FROM view_cliente_consolidado cc
# MAGIC   LEFT JOIN scores_individuais si ON cc.cod_pessoa_cliente = si.cod_pessoa_cliente AND cc.mes_safra = si.mes_safra
# MAGIC   LEFT JOIN view_exposicao_maxima em ON cc.cod_pessoa_cliente = em.cod_pessoa_cliente AND cc.mes_safra = em.mes_safra
# MAGIC   LEFT JOIN view_prazo_medio pm ON cc.cod_pessoa_cliente = pm.cod_pessoa_cliente AND cc.mes_safra = pm.mes_safra
# MAGIC   LEFT JOIN view_grupo_economico ge ON ge.cod_pessoa_cliente = cc.cod_pessoa_cliente
# MAGIC   CROSS JOIN view_taxa_clp_hoje clp  -- taxa unica do dia, sem JOIN por data
# MAGIC   WHERE cc.valor_total > 0
# MAGIC ) AS source
# MAGIC ON target.reference_month = source.reference_month AND target.id_customer = source.id_customer
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 0,Sanity checks
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.credit_engine.abt_inference_me_br) AS total_linhas,
# MAGIC   (SELECT MIN(reference_month) FROM ds_catalog_dev.credit_engine.abt_inference_me_br) AS safra_min,
# MAGIC   (SELECT MAX(reference_month) FROM ds_catalog_dev.credit_engine.abt_inference_me_br) AS safra_max,
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM ds_catalog_dev.credit_engine.abt_inference_me_br) AS total_safras,
# MAGIC   (SELECT COUNT(DISTINCT id_customer) FROM ds_catalog_dev.credit_engine.abt_inference_me_br) AS total_clientes,
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC     SELECT reference_month, id_customer, COUNT(*) AS cnt
# MAGIC     FROM ds_catalog_dev.credit_engine.abt_inference_me_br GROUP BY reference_month, id_customer HAVING cnt > 1
# MAGIC   )) AS duplicatas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE WHEN reference_value = 0 THEN 'zerado' ELSE 'preenchido' END AS status_reference_value,
# MAGIC   COUNT(*)                                                           AS qtd,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)                 AS pct
# MAGIC FROM ds_catalog_dev.credit_engine.abt_inference_me_br
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reference_month,
# MAGIC   COUNT(*)                                                                    AS total,
# MAGIC   SUM(CASE WHEN reference_value = 0 THEN 1 ELSE 0 END)                       AS zerados,
# MAGIC   ROUND(SUM(CASE WHEN reference_value = 0 THEN 1 ELSE 0 END) * 100.0
# MAGIC         / COUNT(*), 2)                                                        AS pct_zerado
# MAGIC FROM ds_catalog_dev.credit_engine.abt_inference_me_br
# MAGIC GROUP BY reference_month
# MAGIC ORDER BY reference_month DESC