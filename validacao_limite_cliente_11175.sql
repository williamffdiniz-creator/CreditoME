-- ============================================================
-- VALIDACAO: cliente 11175 (FRIMA S.A. - CHILE)
-- Safras 2026-04 e 2026-05
--
-- FORMULA COMPLETA:
--   abt_inference (mes M):
--     reference_value = AVG(faturamento_mensal) nas ultimas 24 safras (dta_abertura_pedido)
--     payment_term    = AVG(prazo_dias / 30) dos pedidos pagos nos ultimos 24 meses, clip [1,5]
--
--   apply_model (mes M) — usa features de abt_inference (mes M-1):
--     adjusted_score  = historical_weight * score + (1-historical_weight) * portfolio_score
--     limit_me_usd    = reference_value * payment_term * multiplicador_banda
--                       (BAIXO=1.2, MEDIO=1.1, ALTO=0.9)
--     credit_limit    = limit_mi_usd + limit_me_usd  (para clientes MI+ME)
--
--   Por que 10M em 2026-05 e 7M em 2026-04?
--     • apply_model 2026-04 usa abt_inference 2026-03: reference_value = 1.933.645
--     • apply_model 2026-05 usa abt_inference 2026-04: reference_value = 3.328.595
--       (quase dobrou pq em marco/26 entrou um mes de faturamento alto na janela 24m)
-- ============================================================


-- =============================================================
-- QUERY 1: PROVA DO CALCULO — credit_limit 2026-04 e 2026-05
-- Mostra todos os componentes da formula lado a lado
-- =============================================================

SELECT
  am.reference_month,
  am.adjusted_score,
  am.integrated_score_band                             AS banda,

  -- Componente ME (calculado pelo NB02, baseado em features do mes M-1)
  inf.reference_value                                  AS rv_me,
  inf.payment_term                                     AS pt_me,
  CASE
    WHEN am.adjusted_score <= 0.02 THEN 1.2
    WHEN am.adjusted_score <= 0.30 THEN 1.1
    ELSE 0.9
  END                                                  AS multiplicador,
  ROUND(inf.reference_value * inf.payment_term *
    CASE
      WHEN am.adjusted_score <= 0.02 THEN 1.2
      WHEN am.adjusted_score <= 0.30 THEN 1.1
      ELSE 0.9
    END, 4)                                            AS limit_me_calculado,

  -- Componente MI (preenchido pelo NB02b)
  am.limit_mi_usd                                      AS limit_mi,

  -- Soma = credit_limit armazenado
  ROUND(am.limit_mi_usd +
    inf.reference_value * inf.payment_term *
    CASE
      WHEN am.adjusted_score <= 0.02 THEN 1.2
      WHEN am.adjusted_score <= 0.30 THEN 1.1
      ELSE 0.9
    END, 4)                                            AS credit_limit_calculado,

  am.credit_limit                                      AS credit_limit_armazenado,

  -- Diferenca (deve ser 0 ou micro-arredondamento)
  ROUND(am.credit_limit - (am.limit_mi_usd +
    inf.reference_value * inf.payment_term *
    CASE
      WHEN am.adjusted_score <= 0.02 THEN 1.2
      WHEN am.adjusted_score <= 0.30 THEN 1.1
      ELSE 0.9
    END), 4)                                           AS diferenca,

  -- Features de origem (de abt_inference mes M-1)
  inf.reference_month                                  AS features_de_mes,
  inf.historical_weight,
  inf.score                                            AS score_individual,
  inf.months_with_billing,
  am.adjusted_score_mi                                 AS score_mi

FROM ds_catalog_dev.credit_engine.apply_model_me_br am

-- Features vem de abt_inference mes M-1
JOIN ds_catalog_dev.credit_engine.abt_inference_me_br inf
  ON  inf.id_customer    = am.id_customer
  AND inf.reference_month = add_months(am.reference_month, -1)

WHERE am.id_customer     = 11175
  AND am.reference_month IN ('2026-04-01', '2026-05-01')

ORDER BY am.reference_month;


-- =============================================================
-- QUERY 2: COMPOSICAO DO REFERENCE_VALUE
-- Mostra o faturamento mensal por mes dentro da janela 24m
-- que formou o reference_value em cada abt_inference
--
-- reference_value = AVG(soma_mensal) dos meses com faturamento
--                   na janela [safra-24m, safra]
-- =============================================================

WITH
-- Parcelas faturadas (fonte original do reference_value)
-- usando flat_financial_position_order_cambio_sys + flat_orders_external_market
-- replicando a logica de view_base_parcelas
parcelas AS (
  SELECT
    ped.importer_id                                                AS cod_pessoa_cliente,
    fp.invoice_number                                             AS numero_invoice,
    ped.order_opening_date                                        AS dta_abertura_pedido,
    date_trunc('month', ped.order_opening_date)                   AS mes_pedido,
    -- Conversao cambial identica ao notebook
    CASE
      WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
      ELSE fp.total_invoice  -- USD direto para Chile
    END                                                           AS valor_final
  FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
  INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
    ON  ped.comex_order_number = fp.invoice_number
    AND ped.importer_id        = 11175
  WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
    AND ped.order_opening_date IS NOT NULL
    AND fp.total_invoice > 0
    AND ped.importer_id = 11175
),

-- Faturamento agrupado por mes de abertura do pedido
fat_mensal AS (
  SELECT
    mes_pedido,
    COUNT(DISTINCT numero_invoice) AS qtd_pedidos,
    ROUND(SUM(valor_final), 2)     AS soma_mensal_usd
  FROM parcelas
  WHERE mes_pedido BETWEEN '2022-04-01' AND '2026-04-01'  -- janela maxima necessaria
  GROUP BY mes_pedido
),

-- Safras alvo
safras AS (
  SELECT '2026-03-01' AS mes_safra, 'abt_inference 2026-03 (features de apply_model 2026-04)' AS descricao
  UNION ALL
  SELECT '2026-04-01', 'abt_inference 2026-04 (features de apply_model 2026-05)'
)

SELECT
  s.mes_safra,
  s.descricao,
  f.mes_pedido,
  f.qtd_pedidos,
  f.soma_mensal_usd,
  -- Indicador se o mes esta dentro da janela 24m
  'SIM' AS dentro_janela_24m,
  -- Running AVG ate cada mes (preview do reference_value)
  ROUND(AVG(f.soma_mensal_usd) OVER (
    PARTITION BY s.mes_safra
    ORDER BY f.mes_pedido
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ), 4) AS avg_acumulado

FROM safras s
JOIN fat_mensal f
  ON f.mes_pedido >= add_months(CAST(s.mes_safra AS DATE), -24)
 AND f.mes_pedido <= CAST(s.mes_safra AS DATE)

ORDER BY s.mes_safra, f.mes_pedido;


-- =============================================================
-- QUERY 3: PEDIDOS INDIVIDUAIS que compoem o reference_value
-- Para o mes de abt_inference 2026-04-01 (referencia de apply_model 2026-05)
-- Janela: 2024-04-01 a 2026-04-01
-- =============================================================

SELECT
  ped.order_opening_date                                          AS dta_abertura_pedido,
  date_trunc('month', ped.order_opening_date)                    AS mes_pedido,
  fp.invoice_number                                              AS numero_invoice,
  fp.billing_date                                               AS data_faturamento,
  fp.due_date                                                   AS data_vencimento,
  fp.payment_date                                               AS data_pagamento,
  fp.currency                                                   AS moeda,
  fp.total_invoice                                              AS valor_original,
  CASE
    WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
    ELSE fp.total_invoice
  END                                                           AS valor_usd,
  fp.financial_position                                         AS posicao_financeira

FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
  ON  ped.comex_order_number = fp.invoice_number
  AND ped.importer_id        = 11175
WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
  AND ped.order_opening_date IS NOT NULL
  AND fp.total_invoice > 0
  AND ped.importer_id = 11175
  AND ped.order_opening_date >= '2024-04-01'
  AND ped.order_opening_date <= '2026-04-01'   -- janela 24m para abt_inference 2026-04

ORDER BY date_trunc('month', ped.order_opening_date), fp.invoice_number;


-- =============================================================
-- QUERY 4: RESUMO DO reference_value — confirma o AVG
-- Mostra a conta final: meses, soma total e media
-- Deve bater com abt_inference.reference_value
-- =============================================================

WITH parcelas AS (
  SELECT
    ped.importer_id                                              AS cod_pessoa_cliente,
    fp.invoice_number,
    date_trunc('month', ped.order_opening_date)                 AS mes_pedido,
    CASE
      WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
      ELSE fp.total_invoice
    END                                                         AS valor_usd
  FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
  INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
    ON  ped.comex_order_number = fp.invoice_number
    AND ped.importer_id        = 11175
  WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
    AND ped.order_opening_date IS NOT NULL
    AND fp.total_invoice > 0
),

fat_mensal AS (
  SELECT mes_pedido, SUM(valor_usd) AS soma_mensal
  FROM parcelas
  GROUP BY mes_pedido
)

SELECT
  'abt_inference 2026-03 (features apply_model 2026-04)' AS safra_alvo,
  COUNT(*)                                               AS qtd_meses_com_faturamento,
  ROUND(SUM(soma_mensal), 2)                             AS total_24m_usd,
  ROUND(AVG(soma_mensal), 4)                             AS reference_value_calculado,
  (SELECT reference_value FROM ds_catalog_dev.credit_engine.abt_inference_me_br
   WHERE id_customer = 11175 AND reference_month = '2026-03-01')
                                                         AS reference_value_armazenado,
  ROUND(
    AVG(soma_mensal) -
    (SELECT reference_value FROM ds_catalog_dev.credit_engine.abt_inference_me_br
     WHERE id_customer = 11175 AND reference_month = '2026-03-01')
  , 4)                                                   AS diferenca
FROM fat_mensal
WHERE mes_pedido >= add_months(DATE '2026-03-01', -24)   -- 2024-03-01
  AND mes_pedido <= DATE '2026-03-01'

UNION ALL

SELECT
  'abt_inference 2026-04 (features apply_model 2026-05)',
  COUNT(*),
  ROUND(SUM(soma_mensal), 2),
  ROUND(AVG(soma_mensal), 4),
  (SELECT reference_value FROM ds_catalog_dev.credit_engine.abt_inference_me_br
   WHERE id_customer = 11175 AND reference_month = '2026-04-01'),
  ROUND(
    AVG(soma_mensal) -
    (SELECT reference_value FROM ds_catalog_dev.credit_engine.abt_inference_me_br
     WHERE id_customer = 11175 AND reference_month = '2026-04-01')
  , 4)
FROM fat_mensal
WHERE mes_pedido >= add_months(DATE '2026-04-01', -24)   -- 2024-04-01
  AND mes_pedido <= DATE '2026-04-01';


-- =============================================================
-- QUERY 5: PEDIDOS SEM FATURA (sem_fatura_mensal)
-- Identifica os pedidos aprovados mas nao faturados que
-- explicam a discrepancia do reference_value na Q4.
--
-- A "view_exposicao_maxima" soma faturamento_mensal + sem_fatura_mensal.
-- O cross-join com os meses de billing dilui o sem_fatura pelo
-- numero de meses, adicionando-o ao AVG:
--   reference_value = avg_billing + sem_fatura_mensal_no_mes_da_safra
--
-- abt 2026-03: 1.655.345 + 278.300  = 1.933.645  ✓
-- abt 2026-04: 1.655.345 + 1.673.250 = 3.328.595 ✓
-- =============================================================

SELECT
  ped.comex_order_number                                     AS numero_pedido,
  ped.order_opening_date                                     AS dta_abertura_pedido,
  ped.credit_approval_date                                   AS dta_aprovacao_credito,
  ped.invoice_date                                           AS dta_fatura,
  ped.order_value_usd                                        AS valor_pedido_usd,
  ped.order_value_usd / 30                                   AS sem_fatura_mes_usd,
  CASE
    WHEN ped.credit_approval_date < '2026-03-01'
     AND (ped.invoice_date IS NULL OR ped.invoice_date >= '2026-03-01')
    THEN 'SIM'
    ELSE 'NAO'
  END                                                        AS contribui_abt_2026_03,
  CASE
    WHEN ped.credit_approval_date < '2026-04-01'
     AND (ped.invoice_date IS NULL OR ped.invoice_date >= '2026-04-01')
    THEN 'SIM'
    ELSE 'NAO'
  END                                                        AS contribui_abt_2026_04

FROM de_data_lake_prd.business_analytics.flat_orders_external_market ped
WHERE ped.importer_id = 11175
  AND ped.credit_approval_date IS NOT NULL
  AND (ped.invoice_date IS NULL OR ped.invoice_date >= '2026-03-01')
  AND ped.credit_approval_date >= '2024-03-01'

ORDER BY ped.credit_approval_date DESC;


-- =============================================================
-- QUERY 6: RECONCILIACAO TOTAL billing + sem_fatura = reference_value
-- Diferenca deve ser ZERO para confirmar a formula completa.
-- =============================================================

WITH
parcelas AS (
  SELECT
    fp.invoice_number,
    date_trunc('month', ped.order_opening_date)             AS mes_pedido,
    CASE
      WHEN fp.currency = 'CNY' THEN ROUND(fp.total_invoice / 7, 2)
      ELSE fp.total_invoice
    END                                                     AS valor_usd
  FROM de_data_lake_prd.business_analytics.flat_financial_position_order_cambio_sys fp
  INNER JOIN de_data_lake_prd.business_analytics.flat_orders_external_market ped
    ON  ped.comex_order_number = fp.invoice_number
    AND ped.importer_id        = 11175
  WHERE fp.financial_position IN ('A RECEBER', 'JUDICIAL')
    AND ped.order_opening_date IS NOT NULL
    AND fp.total_invoice > 0
),

fat_mensal AS (
  SELECT mes_pedido, SUM(valor_usd) AS soma_mensal
  FROM parcelas
  GROUP BY mes_pedido
),

sem_fatura AS (
  SELECT
    date_trunc('month', credit_approval_date)               AS mes_aprovacao,
    SUM(order_value_usd) / 30                               AS sem_fatura_mensal
  FROM de_data_lake_prd.business_analytics.flat_orders_external_market
  WHERE importer_id = 11175
    AND credit_approval_date IS NOT NULL
    AND invoice_date IS NULL
  GROUP BY date_trunc('month', credit_approval_date)
),

abt_stored AS (
  SELECT reference_month, reference_value
  FROM ds_catalog_dev.credit_engine.abt_inference_me_br
  WHERE id_customer = 11175
    AND reference_month IN ('2026-03-01', '2026-04-01')
)

SELECT
  a.reference_month,
  COUNT(f.mes_pedido)                                       AS qtd_meses_billing,
  ROUND(AVG(f.soma_mensal), 4)                              AS avg_billing_puro,
  ROUND(SUM(sf.sem_fatura_mensal), 4)                       AS total_sem_fatura,
  ROUND(AVG(f.soma_mensal) + COALESCE(SUM(sf.sem_fatura_mensal), 0), 4)
                                                            AS reference_value_reconciliado,
  a.reference_value                                         AS reference_value_armazenado,
  ROUND(
    (AVG(f.soma_mensal) + COALESCE(SUM(sf.sem_fatura_mensal), 0)) - a.reference_value
  , 4)                                                      AS diferenca_final

FROM abt_stored a
LEFT JOIN fat_mensal f
  ON f.mes_pedido >= add_months(a.reference_month, -24)
 AND f.mes_pedido <= a.reference_month
LEFT JOIN sem_fatura sf
  ON sf.mes_aprovacao <= a.reference_month
 AND sf.mes_aprovacao >= add_months(a.reference_month, -24)

GROUP BY a.reference_month, a.reference_value
ORDER BY a.reference_month;
