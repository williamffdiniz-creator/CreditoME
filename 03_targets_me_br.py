# Databricks notebook source
# DBTITLE 0,Pipeline: Targets ME BR v7
# MAGIC %md
# MAGIC ## Targets — ME BR v7
# MAGIC
# MAGIC Targets de performance usando as **mesmas regras de negocio do NB01**.
# MAGIC
# MAGIC **Nota**: Este notebook e auto-contido — recria as views necessarias
# MAGIC (view_taxas_cambio, view_principal_p_base_unificada, view_posicao_financeira)
# MAGIC porque cada notebook roda em sessao isolada via `dbutils.notebook.run()`.

# COMMAND ----------

# DBTITLE 0,Parametros
from datetime import date
dbutils.widgets.text("data_referencia", "", "Data de Referencia")
data_referencia = dbutils.widgets.get("data_referencia")
effective_date = data_referencia if data_referencia else str(date.today())
spark.sql(f"CREATE OR REPLACE TEMP VIEW config_pipeline AS SELECT CAST('{effective_date}' AS DATE) AS data_referencia")
print(f"Data de referencia: {effective_date}")

# COMMAND ----------

# DBTITLE 0,DROP + CREATE targets_me_br_v7
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ds_catalog_dev.default.targets_me_br;
# MAGIC CREATE TABLE ds_catalog_dev.default.targets_me_br (
# MAGIC   id_customer        INT,
# MAGIC   customer_name      STRING,
# MAGIC   country            STRING,
# MAGIC   reference_month    DATE,
# MAGIC   percent7mob1       INT,
# MAGIC   percent7mob3       INT,
# MAGIC   percent7mob6       INT,
# MAGIC   percent7mob9       INT,
# MAGIC   percent7mob12      INT,
# MAGIC   billed_1m          DOUBLE,
# MAGIC   overdue_1m         DOUBLE,
# MAGIC   overdue_pct_1m     DOUBLE,
# MAGIC   billed_3m          DOUBLE,
# MAGIC   overdue_3m         DOUBLE,
# MAGIC   overdue_pct_3m     DOUBLE,
# MAGIC   billed_6m          DOUBLE,
# MAGIC   overdue_6m         DOUBLE,
# MAGIC   overdue_pct_6m     DOUBLE,
# MAGIC   billed_12m         DOUBLE,
# MAGIC   overdue_12m        DOUBLE,
# MAGIC   overdue_pct_12m    DOUBLE,
# MAGIC   updated_at         TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')

# COMMAND ----------

# DBTITLE 0,Pre-check
# MAGIC %sql
# MAGIC SELECT CASE WHEN COUNT(*) = 0 THEN 'ERRO: abt_inference_me_br vazia'
# MAGIC   ELSE CONCAT('OK: ', COUNT(*), ' linhas') END AS status
# MAGIC FROM ds_catalog_dev.default.abt_inference_me_br

# COMMAND ----------

# DBTITLE 0,Base completa: safras retroativas 24m
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW base_completa AS
# MAGIC SELECT reference_month, id_customer, customer_name, country
# MAGIC FROM ds_catalog_dev.default.abt_inference_me_br
# MAGIC WHERE reference_month >= LEAST(
# MAGIC   add_months(date_trunc('month', current_date()), -24),
# MAGIC   COALESCE(add_months((SELECT MAX(reference_month) FROM ds_catalog_dev.default.targets_me_br), -1), DATE '1900-01-01'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Views auxiliares (recriadas — sessao isolada)

# COMMAND ----------

# DBTITLE 1,view_taxas_cambio (6 moedas, forward-fill)
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

# DBTITLE 1,view_principal_p_base_unificada (pedidos + tipo_risco)
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

# DBTITLE 1,view_posicao_financeira (parcelas financeiras)
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
# MAGIC ## Faturas para Targets (baseline original + 6 moedas + granularidade pedido)

# COMMAND ----------

# DBTITLE 1,faturas_targets
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW faturas_targets AS
# MAGIC WITH cte_raw AS (
# MAGIC   SELECT
# MAGIC     CAST(ped.cod_pessoa_cliente AS INT) AS cod_pessoa,
# MAGIC     ped.nom_pessoa_cliente,
# MAGIC     fp.numero_invoice, fp.id_parcela, fp.data_faturamento,
# MAGIC     date_trunc('month', ped.dta_abertura_pedido) AS mes_abertura_pedido,
# MAGIC     fp.data_vencimento, fp.data_pagamento, fp.data_chegada_navio,
# MAGIC     fp.total_fatura, fp.moeda, ped.tipo_risco, ped.dta_abertura_pedido,
# MAGIC     -- BASELINE por tipo_risco
# MAGIC     CASE ped.tipo_risco
# MAGIC       WHEN 'RISCO PERFORMANCE' THEN COALESCE(fp.data_chegada_navio, fp.data_vencimento)
# MAGIC       ELSE COALESCE(fp.data_vencimento, fp.data_chegada_navio)
# MAGIC     END AS baseline_date,
# MAGIC     -- Conversao 6 moedas
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
# MAGIC     CASE ped.tipo_risco
# MAGIC       WHEN 'RISCO PERFORMANCE' THEN DATEDIFF(fp.data_pagamento, COALESCE(fp.data_chegada_navio, fp.data_vencimento))
# MAGIC       ELSE DATEDIFF(fp.data_pagamento, COALESCE(fp.data_vencimento, fp.data_chegada_navio))
# MAGIC     END AS raw_delay_for_filter
# MAGIC   FROM view_posicao_financeira AS fp
# MAGIC   INNER JOIN view_principal_p_base_unificada AS ped
# MAGIC     ON ped.num_pedido_comex = fp.numero_invoice AND ped.cod_pessoa_cliente = fp.cod_pessoa_cliente
# MAGIC   LEFT JOIN view_taxas_cambio AS tx
# MAGIC     ON tx.data_taxa = fp.data_faturamento
# MAGIC     AND tx.moeda_procedencia = CASE WHEN fp.moeda = 'REAL' THEN 'BRL' ELSE fp.moeda END
# MAGIC   WHERE fp.nom_pessoa_cliente NOT LIKE 'MINER%' AND fp.nom_pessoa_cliente NOT LIKE 'SWIF%'
# MAGIC     AND fp.total_fatura > 0
# MAGIC     AND (ped.dta_vencimento IS NOT NULL OR ped.dta_chegada_destino IS NOT NULL)
# MAGIC )
# MAGIC SELECT * FROM cte_raw
# MAGIC WHERE (raw_delay_for_filter > -180 OR raw_delay_for_filter IS NULL) AND valor_final IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Agregacao parcela → invoice
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW faturas_targets_invoice AS
# MAGIC SELECT cod_pessoa, numero_invoice, mes_abertura_pedido,
# MAGIC   SUM(valor_final) AS valor_total_invoice,
# MAGIC   COALESCE(SUM(CASE
# MAGIC     WHEN data_pagamento IS NOT NULL AND DATEDIFF(data_pagamento, baseline_date) > 7 THEN valor_final
# MAGIC     WHEN data_pagamento IS NULL AND baseline_date <= (SELECT data_referencia FROM config_pipeline)
# MAGIC       AND DATEDIFF((SELECT data_referencia FROM config_pipeline), baseline_date) > 7 THEN valor_final
# MAGIC     ELSE 0 END), 0) AS valor_atrasado_invoice
# MAGIC FROM faturas_targets
# MAGIC GROUP BY cod_pessoa, numero_invoice, mes_abertura_pedido

# COMMAND ----------

# DBTITLE 1,Agregacao invoice → cliente/mes
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW faturas_targets_cliente_mes AS
# MAGIC SELECT cod_pessoa, mes_abertura_pedido,
# MAGIC   COALESCE(SUM(valor_total_invoice), 0) AS valor_total_mes,
# MAGIC   COALESCE(SUM(valor_atrasado_invoice), 0) AS valor_atraso_mes
# MAGIC FROM faturas_targets_invoice
# MAGIC GROUP BY cod_pessoa, mes_abertura_pedido

# COMMAND ----------

# MAGIC %md
# MAGIC ## Targets por janela futura

# COMMAND ----------

# DBTITLE 1,targets_percentmob
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW targets_percentmob AS
# MAGIC WITH atraso_por_janela AS (
# MAGIC   SELECT bc.reference_month, bc.id_customer,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido = add_months(bc.reference_month, 1) THEN ft.valor_total_mes ELSE 0 END) AS faturado_1m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido = add_months(bc.reference_month, 1) THEN ft.valor_atraso_mes ELSE 0 END) AS atraso_1m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 3) THEN ft.valor_total_mes ELSE 0 END) AS faturado_3m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 3) THEN ft.valor_atraso_mes ELSE 0 END) AS atraso_3m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 6) THEN ft.valor_total_mes ELSE 0 END) AS faturado_6m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 6) THEN ft.valor_atraso_mes ELSE 0 END) AS atraso_6m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 9) THEN ft.valor_total_mes ELSE 0 END) AS faturado_9m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 9) THEN ft.valor_atraso_mes ELSE 0 END) AS atraso_9m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 12) THEN ft.valor_total_mes ELSE 0 END) AS faturado_12m,
# MAGIC     SUM(CASE WHEN ft.mes_abertura_pedido > bc.reference_month AND ft.mes_abertura_pedido <= add_months(bc.reference_month, 12) THEN ft.valor_atraso_mes ELSE 0 END) AS atraso_12m
# MAGIC   FROM base_completa bc
# MAGIC   LEFT JOIN faturas_targets_cliente_mes ft ON bc.id_customer = ft.cod_pessoa
# MAGIC   GROUP BY bc.reference_month, bc.id_customer
# MAGIC )
# MAGIC SELECT bc.id_customer, bc.customer_name, bc.country,
# MAGIC   add_months(bc.reference_month, 1) AS reference_month,
# MAGIC   CASE WHEN apj.faturado_1m = 0 OR apj.faturado_1m IS NULL THEN NULL ELSE CASE WHEN (apj.atraso_1m / apj.faturado_1m) > 0.10 THEN 1 ELSE 0 END END AS percent7mob1,
# MAGIC   CASE WHEN apj.faturado_3m = 0 OR apj.faturado_3m IS NULL THEN NULL ELSE CASE WHEN (apj.atraso_3m / apj.faturado_3m) > 0.10 THEN 1 ELSE 0 END END AS percent7mob3,
# MAGIC   CASE WHEN apj.faturado_6m = 0 OR apj.faturado_6m IS NULL THEN NULL ELSE CASE WHEN (apj.atraso_6m / apj.faturado_6m) > 0.10 THEN 1 ELSE 0 END END AS percent7mob6,
# MAGIC   CASE WHEN apj.faturado_9m = 0 OR apj.faturado_9m IS NULL THEN NULL ELSE CASE WHEN (apj.atraso_9m / apj.faturado_9m) > 0.10 THEN 1 ELSE 0 END END AS percent7mob9,
# MAGIC   CASE WHEN apj.faturado_12m = 0 OR apj.faturado_12m IS NULL THEN NULL ELSE CASE WHEN (apj.atraso_12m / apj.faturado_12m) > 0.10 THEN 1 ELSE 0 END END AS percent7mob12,
# MAGIC   apj.faturado_1m AS billed_1m, apj.atraso_1m AS overdue_1m, ROUND(apj.atraso_1m / NULLIF(apj.faturado_1m, 0), 4) AS overdue_pct_1m,
# MAGIC   apj.faturado_3m AS billed_3m, apj.atraso_3m AS overdue_3m, ROUND(apj.atraso_3m / NULLIF(apj.faturado_3m, 0), 4) AS overdue_pct_3m,
# MAGIC   apj.faturado_6m AS billed_6m, apj.atraso_6m AS overdue_6m, ROUND(apj.atraso_6m / NULLIF(apj.faturado_6m, 0), 4) AS overdue_pct_6m,
# MAGIC   apj.faturado_12m AS billed_12m, apj.atraso_12m AS overdue_12m, ROUND(apj.atraso_12m / NULLIF(apj.faturado_12m, 0), 4) AS overdue_pct_12m
# MAGIC FROM base_completa bc
# MAGIC LEFT JOIN atraso_por_janela apj ON bc.reference_month = apj.reference_month AND bc.id_customer = apj.id_customer

# COMMAND ----------

# DBTITLE 0,MERGE retroativo
# MAGIC %sql
# MAGIC MERGE INTO ds_catalog_dev.default.targets_me_br AS target
# MAGIC USING (SELECT *, current_timestamp() AS updated_at FROM targets_percentmob) AS source
# MAGIC ON target.reference_month = source.reference_month AND target.id_customer = source.id_customer
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 0,Sanity checks
# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM ds_catalog_dev.default.targets_me_br) AS total_linhas,
# MAGIC   (SELECT MIN(reference_month) FROM ds_catalog_dev.default.targets_me_br) AS safra_min,
# MAGIC   (SELECT MAX(reference_month) FROM ds_catalog_dev.default.targets_me_br) AS safra_max,
# MAGIC   (SELECT COUNT(DISTINCT reference_month) FROM ds_catalog_dev.default.targets_me_br) AS total_safras,
# MAGIC   (SELECT COUNT(DISTINCT id_customer) FROM ds_catalog_dev.default.targets_me_br) AS total_clientes,
# MAGIC   (SELECT COUNT(*) FROM (
# MAGIC     SELECT reference_month, id_customer, COUNT(*) AS cnt
# MAGIC     FROM ds_catalog_dev.default.targets_me_br GROUP BY reference_month, id_customer HAVING cnt > 1
# MAGIC   )) AS duplicatas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reference_month,
# MAGIC   COUNT(*) AS registros,
# MAGIC   SUM(CASE WHEN percent7mob1 = 1 THEN 1 ELSE 0 END) AS com_atraso_t1,
# MAGIC   SUM(CASE WHEN percent7mob1 = 0 THEN 1 ELSE 0 END) AS sem_atraso_t1,
# MAGIC   SUM(CASE WHEN percent7mob1 IS NULL THEN 1 ELSE 0 END) AS sem_resultado_t1,
# MAGIC   ROUND(AVG(overdue_pct_1m), 2) AS avg_overdue_pct_1m,
# MAGIC   ROUND(SUM(billed_1m), 2) AS total_billed_1m,
# MAGIC   --ROUND(SUM(overdue_amount_1m), 2) AS total_overdue_1m,
# MAGIC   MAX(updated_at) AS ultima_atualizacao
# MAGIC FROM ds_catalog_dev.default.targets_me_br
# MAGIC GROUP BY reference_month
# MAGIC ORDER BY reference_month DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reference_month,
# MAGIC   COUNT(*) AS registros,
# MAGIC   SUM(CASE WHEN percent7mob6 = 1 THEN 1 ELSE 0 END) AS com_atraso_t1,
# MAGIC   SUM(CASE WHEN percent7mob6 = 0 THEN 1 ELSE 0 END) AS sem_atraso_t1,
# MAGIC   SUM(CASE WHEN percent7mob6 IS NULL THEN 1 ELSE 0 END) AS sem_resultado_t1,
# MAGIC   ROUND(AVG(overdue_pct_1m), 2) AS avg_overdue_pct_1m,
# MAGIC   ROUND(SUM(billed_1m), 2) AS total_billed_1m,
# MAGIC   --ROUND(SUM(overdue_amount_1m), 2) AS total_overdue_1m,
# MAGIC   MAX(updated_at) AS ultima_atualizacao
# MAGIC FROM ds_catalog_dev.default.targets_me_br
# MAGIC GROUP BY reference_month
# MAGIC ORDER BY reference_month DESC