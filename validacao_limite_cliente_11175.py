# Databricks notebook source

# MAGIC %md
# MAGIC ## Validação de Limite — Cliente 11175 (FRIMA S.A. / CHILE)
# MAGIC
# MAGIC Safras analisadas: **2026-04-01** e **2026-05-01**
# MAGIC
# MAGIC ### Fórmula completa
# MAGIC
# MAGIC ```
# MAGIC abt_inference (mes M):
# MAGIC   reference_value = AVG(faturamento_mensal_usd) nas últimas 24 safras (dta_abertura_pedido)
# MAGIC   payment_term    = AVG(prazo_dias / 30) dos pedidos pagos nos últimos 24 meses | clip [1, 5]
# MAGIC
# MAGIC apply_model (mes M) — usa features de abt_inference (mes M-1):
# MAGIC   adjusted_score  = historical_weight × score + (1 − historical_weight) × portfolio_score
# MAGIC   limit_me_usd    = reference_value × payment_term × multiplicador_banda
# MAGIC                     BAIXO (≤0.02) = 1.2 | MEDIO (≤0.30) = 1.1 | ALTO (>0.30) = 0.9
# MAGIC   credit_limit    = limit_mi_usd + limit_me_usd   (cliente integrado MI+ME)
# MAGIC ```
# MAGIC
# MAGIC | Safra | Features de | reference_value | payment_term | mult | limit_me_usd | limit_mi_usd | credit_limit |
# MAGIC |---|---|---|---|---|---|---|---|
# MAGIC | 2026-04 | abt 2026-03 | 1.933.645 | 1.9144 | 1.1 | 4.071.948 | 2.972.232 | **7.044.180** |
# MAGIC | 2026-05 | abt 2026-04 | 3.328.595 | 1.921  | 1.1 | 7.033.655 | 3.074.345 | **10.107.999** |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1 — Prova da fórmula completa
# MAGIC
# MAGIC Recalcula `limit_me_usd` e soma com `limit_mi_usd`. Diferença vs armazenado deve ser 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   am.reference_month,
# MAGIC   am.adjusted_score,
# MAGIC   am.integrated_score_band                                        AS banda,
# MAGIC
# MAGIC   -- Features vindas de abt_inference (mes M-1)
# MAGIC   inf.reference_month                                             AS features_de_mes,
# MAGIC   inf.reference_value                                             AS rv_me,
# MAGIC   inf.payment_term                                                AS pt_me,
# MAGIC   inf.historical_weight,
# MAGIC   inf.score                                                       AS score_individual,
# MAGIC   inf.months_with_billing,
# MAGIC
# MAGIC   -- Multiplicador de banda
# MAGIC   CASE
# MAGIC     WHEN am.adjusted_score <= 0.02 THEN 1.2
# MAGIC     WHEN am.adjusted_score <= 0.30 THEN 1.1
# MAGIC     ELSE 0.9
# MAGIC   END                                                             AS multiplicador,
# MAGIC
# MAGIC   -- Limite ME recalculado
# MAGIC   ROUND(inf.reference_value * inf.payment_term *
# MAGIC     CASE
# MAGIC       WHEN am.adjusted_score <= 0.02 THEN 1.2
# MAGIC       WHEN am.adjusted_score <= 0.30 THEN 1.1
# MAGIC       ELSE 0.9
# MAGIC     END, 4)                                                       AS limit_me_calculado,
# MAGIC
# MAGIC   -- Componente MI (preenchido pelo NB02b)
# MAGIC   am.limit_mi_usd,
# MAGIC   am.adjusted_score_mi                                            AS score_mi,
# MAGIC
# MAGIC   -- Total calculado vs armazenado
# MAGIC   ROUND(am.limit_mi_usd +
# MAGIC     inf.reference_value * inf.payment_term *
# MAGIC     CASE
# MAGIC       WHEN am.adjusted_score <= 0.02 THEN 1.2
# MAGIC       WHEN am.adjusted_score <= 0.30 THEN 1.1
# MAGIC       ELSE 0.9
# MAGIC     END, 4)                                                       AS credit_limit_calculado,
# MAGIC
# MAGIC   am.credit_limit                                                 AS credit_limit_armazenado,
# MAGIC
# MAGIC   -- Diferença (deve ser 0 ou micro-arredondamento)
# MAGIC   ROUND(am.credit_limit -
# MAGIC     (am.limit_mi_usd +
# MAGIC       inf.reference_value * inf.payment_term *
# MAGIC       CASE
# MAGIC         WHEN am.adjusted_score <= 0.02 THEN 1.2
# MAGIC         WHEN am.adjusted_score <= 0.30 THEN 1.1
# MAGIC         ELSE 0.9
# MAGIC       END
# MAGIC     ), 4)                                                         AS diferenca
# MAGIC
# MAGIC FROM ds_catalog_dev.credit_engine.apply_model_me_br am
# MAGIC JOIN ds_catalog_dev.credit_engine.abt_inference_me_br inf
# MAGIC   ON  inf.id_customer     = am.id_customer
# MAGIC   AND inf.reference_month = add_months(am.reference_month, -1)
# MAGIC WHERE am.id_customer      = 11175
# MAGIC   AND am.reference_month IN ('2026-04-01', '2026-05-01')
# MAGIC ORDER BY am.reference_month

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2 — Faturamento mensal na janela 24m
# MAGIC
# MAGIC Mostra mês a mês qual foi o faturamento que entrou na média do `reference_value`.
# MAGIC A coluna `avg_acumulado` mostra como o AVG vai se formando.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH
# MAGIC parcelas AS (
# MAGIC   SELECT
# MAGIC     ped.importer_id                                               AS cod_pessoa_cliente,
# MAGIC     fp.invoice_number,
# MAGIC     date_trunc('month', ped.order_opening_date)                   AS mes_pedido,
# MAGIC     CASE
# MAGIC       WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
# MAGIC       ELSE fp.total_invoice
# MAGIC     END                                                           AS valor_usd
# MAGIC   FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
# MAGIC   INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
# MAGIC     ON  ped.comex_order_number = fp.invoice_number
# MAGIC     AND ped.importer_id        = 11175
# MAGIC   WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
# MAGIC     AND ped.order_opening_date IS NOT NULL
# MAGIC     AND fp.total_invoice > 0
# MAGIC ),
# MAGIC
# MAGIC fat_mensal AS (
# MAGIC   SELECT
# MAGIC     mes_pedido,
# MAGIC     COUNT(DISTINCT invoice_number)  AS qtd_pedidos,
# MAGIC     ROUND(SUM(valor_usd), 2)        AS soma_mensal_usd
# MAGIC   FROM parcelas
# MAGIC   WHERE mes_pedido BETWEEN '2022-04-01' AND '2026-04-01'
# MAGIC   GROUP BY mes_pedido
# MAGIC ),
# MAGIC
# MAGIC safras AS (
# MAGIC   SELECT DATE '2026-03-01' AS mes_safra, 'abt 2026-03 → features apply_model 2026-04' AS descricao
# MAGIC   UNION ALL
# MAGIC   SELECT DATE '2026-04-01', 'abt 2026-04 → features apply_model 2026-05'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   s.mes_safra,
# MAGIC   s.descricao,
# MAGIC   f.mes_pedido,
# MAGIC   f.qtd_pedidos,
# MAGIC   f.soma_mensal_usd,
# MAGIC   ROUND(AVG(f.soma_mensal_usd) OVER (
# MAGIC     PARTITION BY s.mes_safra
# MAGIC     ORDER BY f.mes_pedido
# MAGIC     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC   ), 4)                                                           AS avg_acumulado
# MAGIC
# MAGIC FROM safras s
# MAGIC JOIN fat_mensal f
# MAGIC   ON f.mes_pedido >= add_months(s.mes_safra, -24)
# MAGIC  AND f.mes_pedido <= s.mes_safra
# MAGIC
# MAGIC ORDER BY s.mes_safra, f.mes_pedido

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3 — Pedidos individuais na janela 24m de abt_inference 2026-04-01
# MAGIC
# MAGIC Lista cada invoice que compõe o faturamento do `reference_value = 3.328.595`
# MAGIC usado pelo apply_model de **2026-05-01**.
# MAGIC Janela: `2024-04-01` a `2026-04-01`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('month', ped.order_opening_date)                     AS mes_pedido,
# MAGIC   ped.order_opening_date                                          AS dta_abertura_pedido,
# MAGIC   fp.invoice_number,
# MAGIC   fp.billing_date                                                 AS data_faturamento,
# MAGIC   fp.due_date                                                     AS data_vencimento,
# MAGIC   fp.payment_date                                                 AS data_pagamento,
# MAGIC   fp.financial_position                                           AS posicao_financeira,
# MAGIC   fp.currency                                                     AS moeda,
# MAGIC   fp.total_invoice                                                AS valor_original,
# MAGIC   CASE
# MAGIC     WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
# MAGIC     ELSE fp.total_invoice
# MAGIC   END                                                             AS valor_usd
# MAGIC
# MAGIC FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
# MAGIC INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
# MAGIC   ON  ped.comex_order_number = fp.invoice_number
# MAGIC   AND ped.importer_id        = 11175
# MAGIC WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
# MAGIC   AND ped.order_opening_date IS NOT NULL
# MAGIC   AND fp.total_invoice > 0
# MAGIC   AND ped.importer_id        = 11175
# MAGIC   AND ped.order_opening_date >= '2024-04-01'
# MAGIC   AND ped.order_opening_date <= '2026-04-01'
# MAGIC
# MAGIC ORDER BY date_trunc('month', ped.order_opening_date), fp.invoice_number

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4 — Confirma o AVG vs reference_value armazenado
# MAGIC
# MAGIC Recalcula o `reference_value` a partir dos dados brutos e compara com o valor
# MAGIC que está em `abt_inference_me_br`. Diferença deve ser **zero** (ou micro-arredondamento).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH
# MAGIC parcelas AS (
# MAGIC   SELECT
# MAGIC     fp.invoice_number,
# MAGIC     date_trunc('month', ped.order_opening_date)                   AS mes_pedido,
# MAGIC     CASE
# MAGIC       WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
# MAGIC       ELSE fp.total_invoice
# MAGIC     END                                                           AS valor_usd
# MAGIC   FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
# MAGIC   INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
# MAGIC     ON  ped.comex_order_number = fp.invoice_number
# MAGIC     AND ped.importer_id        = 11175
# MAGIC   WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
# MAGIC     AND ped.order_opening_date IS NOT NULL
# MAGIC     AND fp.total_invoice > 0
# MAGIC ),
# MAGIC
# MAGIC fat_mensal AS (
# MAGIC   SELECT mes_pedido, SUM(valor_usd) AS soma_mensal
# MAGIC   FROM parcelas
# MAGIC   GROUP BY mes_pedido
# MAGIC ),
# MAGIC
# MAGIC abt_stored AS (
# MAGIC   SELECT reference_month, reference_value
# MAGIC   FROM ds_catalog_dev.credit_engine.abt_inference_me_br
# MAGIC   WHERE id_customer = 11175
# MAGIC     AND reference_month IN ('2026-03-01', '2026-04-01')
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   CASE a.reference_month
# MAGIC     WHEN '2026-03-01' THEN 'abt 2026-03 → features apply_model 2026-04'
# MAGIC     WHEN '2026-04-01' THEN 'abt 2026-04 → features apply_model 2026-05'
# MAGIC   END                                                             AS safra_alvo,
# MAGIC   COUNT(f.mes_pedido)                                             AS qtd_meses_com_faturamento,
# MAGIC   ROUND(SUM(f.soma_mensal), 2)                                    AS total_24m_usd,
# MAGIC   ROUND(AVG(f.soma_mensal), 4)                                    AS reference_value_recalculado,
# MAGIC   a.reference_value                                               AS reference_value_armazenado,
# MAGIC   ROUND(AVG(f.soma_mensal) - a.reference_value, 4)               AS diferenca
# MAGIC
# MAGIC FROM abt_stored a
# MAGIC JOIN fat_mensal f
# MAGIC   ON f.mes_pedido >= add_months(a.reference_month, -24)
# MAGIC  AND f.mes_pedido <= a.reference_month
# MAGIC
# MAGIC GROUP BY a.reference_month, a.reference_value
# MAGIC ORDER BY a.reference_month
