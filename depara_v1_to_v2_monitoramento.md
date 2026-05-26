# Depara: Excel / v1 (16 tabelas) → v2 (5 tabelas) — Monitoramento ME BR

**Data:** 2026-05-26
**Branch:** `claude/push-and-analyze-76D8u`
**Catálogo destino:** `ds_catalog_dev.credit_engine`
**Objetivo:** Guia oficial para o time de BI reapontar o Power BI da estrutura
antiga (16 tabelas Excel / 16 tabelas Delta v1) para a estrutura consolidada
v2 (5 tabelas Delta).

---

## 0. Visão geral da consolidação

```
ANTES (v1)                                 DEPOIS (v2)
───────────────────────────────            ───────────────────────────────
monitoring_me_br             ───────────►  monitoring_me_br              (mantida)
performance_me_br            ───┐
                                 ├──────►  monitoring_metrics_me_br      (NOVA — wide, 1 linha/safra)
(novas KPIs agregadas)       ───┘

dist_por_decil_me_br             ───┐
dist_por_decil_diff_pp_me_br     ───┤
perc_bad_decil_me_br             ───┤
perc_bad_decil_diff_pp_me_br     ───┼──►  monitoring_band_me_br          (long, 1 linha/banda)
df_dist_bad_decil_me_br          ───┤
df_dist_bad_decil_diff_pp_me_br  ───┤
iep_por_decil_me_br              ───┘

dist_vars_me_br                  ───┐
dist_vars_diff_pp_me_br          ───┤
vol_vars_me_br                   ───┤
iep_me_br                        ───┼──►  monitoring_features_me_br      (long, 1 linha/feature×bin)
iep_vars_me_br                   ───┤
risco_relativo_me_br             ───┤
risco_relativo_trigger_me_br     ───┘

decile_migrations_me_br          ──────►  monitoring_band_migrations_me_br (long, 1 linha/transição)
```

**Resumo:**

| | v1 (antes) | v2 (depois) |
|---|---|---|
| Tabelas Delta | 17 (1 detalhe + 16 métricas) | 5 (1 detalhe + 4 métricas) |
| Catálogo | `teste.*` (rascunho) | `ds_catalog_dev.credit_engine.*` |
| Modo de carga | MERGE incremental por chave | OVERWRITE da tabela inteira |
| Histórico | Preservado via MERGE | Reconstruído a cada execução |
| Formato | wide com `value` único | long em tabela única por dimensão |

> **Nota sobre OVERWRITE vs MERGE:** As 4 tabelas de métricas v2 fazem OVERWRITE
> porque o cálculo lê toda a `monitoring_me_br` (histórico completo) e recalcula
> a partir do zero. Isso é seguro porque `monitoring_me_br` continua sendo o
> "source of truth" com histórico preservado. Reexecutar não perde nada.

---

## 1. Lista oficial de tabelas v2

| # | Tabela Delta v2 | Granularidade | Linhas típicas |
|---|---|---|---|
| 1 | `ds_catalog_dev.credit_engine.monitoring_me_br` | cliente × safra | ~83k (cresce com o tempo) |
| 2 | `ds_catalog_dev.credit_engine.monitoring_metrics_me_br` | 1 linha por safra | 30 (1 Train + 29 OOT) |
| 3 | `ds_catalog_dev.credit_engine.monitoring_band_me_br` | safra × banda | 120 (30 safras × 4 bandas) |
| 4 | `ds_catalog_dev.credit_engine.monitoring_features_me_br` | safra × feature × bin | 870 (30 safras × 29 bins) |
| 5 | `ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br` | safra × prev_band × curr_band | 256 (29 safras OOT) |

---

## 2. Tabela 1 — `monitoring_me_br` (detalhe cliente × safra)

**Status:** **mantida** (mesma tabela do v1, sem mudança estrutural).
Continua sendo a base de onde tudo é calculado.

**Schema completo:**

| # | Coluna | Tipo | Descrição |
|---|---|---|---|
| 1 | id_customer | INT | PK do cliente |
| 2 | customer_name | STRING | Nome |
| 3 | country | STRING | País |
| 4 | reference_month | DATE | Safra |
| 5 | feature_reference_month | DATE | Safra de features (m-1) |
| 6 | total_amount | DOUBLE | Valor total da carteira |
| 7 | overdue_amount | DOUBLE | Valor em atraso |
| 8 | overdue_pct | DOUBLE | % atraso |
| 9 | months_with_billing | INT | Meses com faturamento |
| 10 | months_defaulted | INT | Meses inadimplentes |
| 11–14 | pct_months_overdue_10_20 / 20_30 / 30_50 / 50_plus | DOUBLE | % meses em cada faixa de atraso |
| 15 | flag_transacted | INT | Flag transacionou |
| 16 | score | DOUBLE | Score individual |
| 17 | historical_weight | DOUBLE | Peso histórico |
| 18 | reference_value | DOUBLE | Valor de referência (USD) |
| 19 | reference_value_clp | DOUBLE | Valor de referência (CLP) |
| 20 | payment_term | DOUBLE | Prazo de pagamento |
| 21–24 | median_cluster_10 / 20 / 30 / 50 | DOUBLE | Medianas dos clusters |
| 25 | portfolio_score | DOUBLE | Score do grupo econômico |
| 26 | adjusted_score | DOUBLE | Score ajustado |
| 27 | score_band | STRING | Banda do score individual |
| 28 | integrated_score | DOUBLE | **Score integrado (base do monitoramento)** |
| 29 | integrated_score_band | STRING | **Banda do score integrado (1-BAIXO/2-MEDIO/3-ALTO)** |
| 30 | credit_limit | DOUBLE | Limite proposto (USD) |
| 31 | credit_limit_clp | DOUBLE | Limite proposto (CLP) |
| 32 | credit_limit_end | DOUBLE | Limite final (USD) |
| 33 | credit_limit_end_clp | DOUBLE | Limite final (CLP) |
| 34–38 | target_percent7mob1..12 | INT | Targets binários |
| 39+ | target_billed/overdue/pct_1m..12m | DOUBLE | Targets financeiros |
| último | updated_at | TIMESTAMP | Carimbo de carga |

**Para o Power BI:** Nenhuma mudança de query. Apenas trocar o prefixo do banco
de `teste.monitoring_me_br` para `ds_catalog_dev.credit_engine.monitoring_me_br`.

---

## 3. Tabela 2 — `monitoring_metrics_me_br` (resumo wide por safra)

**Substitui:** `performance_me_br` (parcial) + agrega KPIs novos.
**Granularidade:** 1 linha por `(reference_month, period)`.
**Carga:** OVERWRITE.

### Schema completo

| # | Coluna | Tipo | Conteúdo | Equivalência v1 |
|---|---|---|---|---|
| 1 | reference_month | DATE | Safra (ex: 2026-05-01) | `reference_year`+`reference_month` |
| 2 | period | STRING | 'Train' ou 'YYYY/MM' | `period` |
| 3 | total_clients | INT | Total de clientes na safra | — (novo) |
| 4 | pop_baixo | INT | Clientes em banda BAIXO | — (novo) |
| 5 | pop_medio | INT | Clientes em banda MEDIO | — (novo) |
| 6 | pop_alto | INT | Clientes em banda ALTO | — (novo) |
| 7 | pct_baixo | DOUBLE | % população BAIXO | `dist_por_decil` decil=1 |
| 8 | pct_medio | DOUBLE | % população MEDIO | `dist_por_decil` decil=2 |
| 9 | pct_alto | DOUBLE | % população ALTO | `dist_por_decil` decil=3 |
| 10 | avg_score | DOUBLE | Score médio | — (novo) |
| 11 | median_score | DOUBLE | Score mediano | — (novo) |
| 12 | avg_credit_limit | DOUBLE | Limite médio (USD) | — (novo) |
| 13 | median_credit_limit | DOUBLE | Limite mediano (USD) | — (novo) |
| 14 | total_credit_limit | DOUBLE | Soma de limites (USD) | — (novo) |
| 15 | avg_credit_limit_clp | DOUBLE | Limite médio (CLP) | — (novo) |
| 16 | median_credit_limit_clp | DOUBLE | Limite mediano (CLP) | — (novo) |
| 17 | total_credit_limit_clp | DOUBLE | Soma de limites (CLP) | — (novo) |
| 18 | total_billed_usd | DOUBLE | Faturamento total (USD) | — (novo) |
| 19 | overdue_usd | DOUBLE | Inadimplência total (USD) | — (novo) |
| 20 | overdue_pct | DOUBLE | % atraso da safra | — (novo) |
| 21 | bad_rate_overall | DOUBLE | **Bad rate geral em % (0–100)** | — (novo) |
| 22 | bad_rate_baixo | DOUBLE | Bad rate BAIXO em % | `perc_bad_decil` decil=1 |
| 23 | bad_rate_medio | DOUBLE | Bad rate MEDIO em % | `perc_bad_decil` decil=2 |
| 24 | bad_rate_alto | DOUBLE | Bad rate ALTO em % | `perc_bad_decil` decil=3 |
| 25 | lift_baixo | DOUBLE | Lift BAIXO | — (novo) |
| 26 | lift_medio | DOUBLE | Lift MEDIO | — (novo) |
| 27 | lift_alto | DOUBLE | Lift ALTO | — (novo) |
| 28 | ks | DOUBLE | KS em [0,1] | `performance` performance='ks' |
| 29 | roc_auc | DOUBLE | ROC AUC em [0,1] | `performance` performance='roc' |
| 30 | gini | DOUBLE | Gini = 2×AUC-1 | `performance` performance='gini' |
| 31 | psi_vs_train | DOUBLE | PSI vs período Train | `iep_por_decil` decil=100 |
| 32 | psi_vs_train_classification | STRING | Estavel/Moderado/Significativo | — (novo, derivado) |
| 33 | psi_rolling | DOUBLE | PSI vs mês anterior | — (novo) |
| 34 | psi_rolling_classification | STRING | Estavel/Moderado/Significativo | — (novo) |
| 35 | pct_improved | DOUBLE | % clientes que melhoraram de banda | — (novo) |
| 36 | pct_maintained | DOUBLE | % clientes que mantiveram banda | — (novo) |
| 37 | pct_worsened | DOUBLE | % clientes que pioraram banda | — (novo) |
| 38 | market_name | STRING | 'me' | `market_name` |
| 39 | metric_key | STRING | 'monitoring' | `metric_key` |
| 40 | updated_at | TIMESTAMP | Carimbo | `updated_at` |

### ⚠️ Atenção — unidades

- **bad_rate_***: armazenado em **% (0–100)** no v2 — no v1 vinha como proporção (0–1) em algumas tabelas e em % em outras. **Padronizado em %.**
- **ks/roc_auc/gini**: continuam em [0,1].
- **psi**: continua adimensional (escala PSI).
- **pct_***: tudo em **% (0–100)** no v2.

### Reapontamento Power BI (queries equivalentes)

```sql
-- ANTES (v1): performance_me_br
SELECT period, performance, value, market_name, reference_year, reference_month
FROM teste.performance_me_br;

-- DEPOIS (v2): unpivot a partir de monitoring_metrics_me_br
SELECT period,
       'ks'   AS performance, ks   AS value, market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
UNION ALL
SELECT period, 'roc',  roc_auc, market_name, YEAR(reference_month), MONTH(reference_month)
FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br
UNION ALL
SELECT period, 'gini', gini,    market_name, YEAR(reference_month), MONTH(reference_month)
FROM ds_catalog_dev.credit_engine.monitoring_metrics_me_br;
```

---

## 4. Tabela 3 — `monitoring_band_me_br` (long por banda)

**Substitui 7 tabelas Excel v1:**
`dist_por_decil`, `dist_por_decil_diff_pp`, `perc_bad_decil`,
`perc_bad_decil_diff_pp`, `df_dist_bad_decil`, `df_dist_bad_decil_diff_pp`,
`iep_por_decil`.

**Granularidade:** 1 linha por `(reference_month, period, band)`.
4 linhas por período: band=1, 2, 3 e 100 (TOTAL). Eventual band=-1 quando há cliente sem banda.

### Schema

| # | Coluna | Tipo | Descrição |
|---|---|---|---|
| 1 | reference_month | DATE | Safra |
| 2 | period | STRING | 'Train' ou 'YYYY/MM' |
| 3 | band | INT | 1, 2, 3, -1 (missing), 100 (TOTAL) |
| 4 | band_name | STRING | '1-BAIXO', '2-MEDIO', '3-ALTO', 'Missing', 'TOTAL' |
| 5 | pct_pop | DOUBLE | % população (0–100) |
| 6 | pct_pop_diff_pp | DOUBLE | Diferença em pp vs Train |
| 7 | bad_rate | DOUBLE | % bad rate da banda (0–100) |
| 8 | bad_rate_diff_pp | DOUBLE | Diferença em pp vs Train |
| 9 | pct_bad_share | DOUBLE | % dos bads concentrado nesta banda |
| 10 | pct_bad_share_diff_pp | DOUBLE | Diferença em pp vs Train |
| 11 | psi_contribution | DOUBLE | Contribuição PSI da banda (band=100 é o total) |
| 12 | market_name | STRING | 'me' |
| 13 | metric_key | STRING | 'monitoring' |
| 14 | updated_at | TIMESTAMP | Carimbo |

### Depara de tabelas Excel v1 → coluna v2

| Excel v1 (tabela) | Coluna `value` v1 representava... | Coluna equivalente em v2 | Filtro v2 |
|---|---|---|---|
| `dist_por_decil_me_br` | % população na banda | `pct_pop` | `band IN (1,2,3,-1)` |
| `dist_por_decil_diff_pp_me_br` | diff pp vs Train | `pct_pop_diff_pp` | `band IN (1,2,3,-1)` |
| `perc_bad_decil_me_br` | bad rate (%) da banda | `bad_rate` | `band IN (1,2,3)` |
| `perc_bad_decil_diff_pp_me_br` | diff pp vs Train | `bad_rate_diff_pp` | `band IN (1,2,3)` |
| `df_dist_bad_decil_me_br` | % bads concentrado | `pct_bad_share` | `band IN (1,2,3)` |
| `df_dist_bad_decil_diff_pp_me_br` | diff pp vs Train | `pct_bad_share_diff_pp` | `band IN (1,2,3)` |
| `iep_por_decil_me_br` | PSI contribution por banda | `psi_contribution` | `band IN (1,2,3,100)` |

### Reapontamento Power BI

```sql
-- ANTES (v1): dist_por_decil_me_br
SELECT decil, period, value, market_name, reference_year, reference_month
FROM teste.dist_por_decil_me_br;

-- DEPOIS (v2):
SELECT band                    AS decil,
       period,
       pct_pop                 AS value,
       market_name,
       YEAR(reference_month)   AS reference_year,
       MONTH(reference_month)  AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
WHERE band IN (1, 2, 3, -1);
```

```sql
-- ANTES (v1): perc_bad_decil_diff_pp_me_br
SELECT decil, period, value, ...
FROM teste.perc_bad_decil_diff_pp_me_br;

-- DEPOIS (v2):
SELECT band AS decil, period,
       bad_rate_diff_pp AS value,
       market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
WHERE band IN (1, 2, 3);
```

```sql
-- ANTES (v1): iep_por_decil_me_br  (incluía decil=100 como total)
SELECT decil, period, value, ...
FROM teste.iep_por_decil_me_br;

-- DEPOIS (v2):
SELECT band AS decil, period,
       psi_contribution AS value,
       market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_band_me_br
WHERE band IN (1, 2, 3, 100);
```

---

## 5. Tabela 4 — `monitoring_features_me_br` (long por feature × bin)

**Substitui 7 tabelas Excel v1:**
`dist_vars`, `dist_vars_diff_pp`, `vol_vars`, `iep`, `iep_vars`,
`risco_relativo`, `risco_relativo_trigger`.

**Granularidade:** 1 linha por `(reference_month, period, feature_name, bin_label)`.
29 bins (5 bins × 5 features contínuas + 1 bin × discretas) × 30 safras = 870 linhas.

### Schema

| # | Coluna | Tipo | Descrição |
|---|---|---|---|
| 1 | reference_month | DATE | Safra |
| 2 | period | STRING | 'Train' ou 'YYYY/MM' |
| 3 | feature_name | STRING | Nome da feature (ex: `pct_months_overdue_10_20`) |
| 4 | grupo | STRING | Grupo de negócio |
| 5 | coeficiente | DOUBLE | Peso efetivo (média mediana_cluster); NULL se não aplicável |
| 6 | bin_label | STRING | Ex: `(-inf, 0.5]`, `(0.5, inf]`, `Missing` |
| 7 | pct_pop | DOUBLE | % população nesta faixa (0–100) |
| 8 | pct_pop_diff_pp | DOUBLE | Diferença em pp vs Train |
| 9 | volume | DOUBLE | Contagem absoluta |
| 10 | lift | DOUBLE | bad_rate(faixa) / bad_rate(geral) |
| 11 | lift_trigger | STRING | `'true'` se lift fora [0.5, 2.0], senão `'false'` |
| 12 | psi_feature | DOUBLE | PSI da feature inteira (repetido em todos os bins) |
| 13 | psi_within_group | DOUBLE | Share da feature no PSI do grupo (0–1) |
| 14 | market_name | STRING | 'me' |
| 15 | metric_key | STRING | 'monitoring' |
| 16 | updated_at | TIMESTAMP | Carimbo |

### Grupos de features

| Grupo | Features |
|---|---|
| severidade_atraso | pct_months_overdue_10_20, 20_30, 30_50, 50_plus, overdue_pct |
| peso_historico | months_with_billing, months_defaulted, historical_weight |
| exposicao | reference_value, payment_term |
| benchmark | portfolio_score |
| score_individual | score |

### Depara de tabelas Excel v1 → coluna v2

| Excel v1 (tabela) | Coluna `value` v1 representava... | Coluna equivalente em v2 | Granularidade v2 |
|---|---|---|---|
| `dist_vars_me_br` | % população na faixa | `pct_pop` | 1 linha por bin |
| `dist_vars_diff_pp_me_br` | diff pp vs Train | `pct_pop_diff_pp` | 1 linha por bin |
| `vol_vars_me_br` | contagem absoluta na faixa | `volume` | 1 linha por bin |
| `risco_relativo_me_br` | lift da faixa | `lift` | 1 linha por bin |
| `risco_relativo_trigger_me_br` | 'true'/'false' | `lift_trigger` | 1 linha por bin |
| `iep_me_br` | PSI por feature | `psi_feature` (DISTINCT por feature) | repetido em cada bin → usar DISTINCT |
| `iep_vars_me_br` | PSI da feature dentro do grupo | `psi_within_group` (DISTINCT por feature) | repetido em cada bin → usar DISTINCT |

> ⚠️ **`psi_feature` e `psi_within_group` são repetidos em todos os bins da
> feature** (o PSI é uma propriedade da feature inteira, não do bin). Em
> Power BI use `DISTINCT` ou `MAX()` agrupando por `feature_name`.

### Reapontamento Power BI

```sql
-- ANTES (v1): dist_vars_me_br  (variaveis = 'feature=bin')
SELECT variaveis, grupo, coeficientes, period, value AS pct_pop_value, ...
FROM teste.dist_vars_me_br;

-- DEPOIS (v2): reconstruir o "variaveis" concatenando
SELECT CONCAT(feature_name, '=', bin_label) AS variaveis,
       grupo,
       coeficiente            AS coeficientes,
       period,
       pct_pop                AS value,
       market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br;
```

```sql
-- ANTES (v1): vol_vars_me_br
SELECT variaveis, grupo, coeficientes, period, value, ...
FROM teste.vol_vars_me_br;

-- DEPOIS (v2):
SELECT CONCAT(feature_name, '=', bin_label) AS variaveis,
       grupo, coeficiente AS coeficientes, period,
       volume AS value,
       market_name,
       YEAR(reference_month) AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br;
```

```sql
-- ANTES (v1): risco_relativo_me_br
SELECT variaveis, grupo, period, value, ...
FROM teste.risco_relativo_me_br;

-- DEPOIS (v2):
SELECT CONCAT(feature_name, '=', bin_label) AS variaveis,
       grupo, period,
       lift AS value,
       market_name,
       YEAR(reference_month) AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br;
```

```sql
-- ANTES (v1): risco_relativo_trigger_me_br
SELECT variaveis, grupo, period, value, ...
FROM teste.risco_relativo_trigger_me_br;

-- DEPOIS (v2):
SELECT CONCAT(feature_name, '=', bin_label) AS variaveis,
       grupo, period,
       lift_trigger AS value,
       market_name,
       YEAR(reference_month) AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br;
```

```sql
-- ANTES (v1): iep_me_br  (PSI por feature, sem dimensão de bin)
SELECT variaveis, period, value, ...
FROM teste.iep_me_br;

-- DEPOIS (v2): pegar PSI distinto por feature (mesmo valor em todos os bins)
SELECT DISTINCT
       feature_name           AS variaveis,
       period,
       psi_feature            AS value,
       market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br;
```

```sql
-- ANTES (v1): iep_vars_me_br
SELECT variaveis, grupo, period, value, ...
FROM teste.iep_vars_me_br;

-- DEPOIS (v2):
SELECT DISTINCT
       feature_name           AS variaveis,
       grupo,
       period,
       psi_within_group       AS value,
       market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month
FROM ds_catalog_dev.credit_engine.monitoring_features_me_br;
```

### ⚠️ Mudança semântica em `psi_within_group`

- **v1 (`iep_vars`)**: `value` era o PSI da feature, **mesma escala absoluta** (0–∞) que `iep_me_br`. A separação era só para filtrar por grupo no dashboard.
- **v2 (`psi_within_group`)**: é o **share fracionário** (0–1) da feature dentro do PSI total do grupo. Soma 1,0 dentro de cada grupo.

Se o BI dependia do valor absoluto, use `psi_feature` em vez de `psi_within_group`.
Se queria saber o peso relativo de cada feature dentro do grupo, `psi_within_group` é o correto.

---

## 6. Tabela 5 — `monitoring_band_migrations_me_br`

**Substitui:** `decile_migrations_me_br`.
**Granularidade:** 1 linha por `(reference_month, previous_band, current_band)`.
Existe apenas em períodos OOT (não em Train).

### Schema

| # | Coluna | Tipo | Descrição |
|---|---|---|---|
| 1 | reference_month | DATE | Safra |
| 2 | period | STRING | 'YYYY/MM' |
| 3 | previous_band | INT | Banda no mês anterior. -1 = sem banda anterior |
| 4 | current_band | INT | Banda atual |
| 5 | count | INT | Quantidade de clientes na transição |
| 6 | pct_of_pop | DOUBLE | % da população (0–100) |
| 7 | market_name | STRING | 'me' |
| 8 | metric_key | STRING | 'monitoring' |
| 9 | updated_at | TIMESTAMP | Carimbo |

### Mudanças em relação ao v1

| Aspecto | v1 (`decile_migrations_me_br`) | v2 (`monitoring_band_migrations_me_br`) |
|---|---|---|
| Coluna `previous_decile` | INT | renomeada → `previous_band` |
| Coluna `current_decile` | INT | renomeada → `current_band` |
| Coluna `pct_of_pop` | não existia | **nova** |
| Coluna `update_at` (typo) | mantida | **removida** |
| Banda missing | não havia | `-1` para cliente novo (sem banda anterior) |

### Reapontamento Power BI

```sql
-- ANTES (v1): decile_migrations_me_br
SELECT previous_decile, current_decile, count, period,
       market_name, reference_year, reference_month, updated_at, update_at
FROM teste.decile_migrations_me_br;

-- DEPOIS (v2):
SELECT previous_band          AS previous_decile,
       current_band           AS current_decile,
       count,
       period,
       market_name,
       YEAR(reference_month)  AS reference_year,
       MONTH(reference_month) AS reference_month,
       updated_at
       /* update_at não existe mais — remover do dashboard */
FROM ds_catalog_dev.credit_engine.monitoring_band_migrations_me_br;
```

---

## 7. Resumo executivo: depara das 16 tabelas Excel/v1

| # | Tabela Excel / v1 | Tabela Delta v2 | Coluna v2 que substitui `value` | Filtro / Transformação |
|---|---|---|---|---|
| 1 | performance | monitoring_metrics_me_br | `ks`, `roc_auc`, `gini` | UNPIVOT |
| 2 | dist_por_decil | monitoring_band_me_br | `pct_pop` | `band IN (1,2,3,-1)` |
| 3 | dist_por_decil_diff_pp | monitoring_band_me_br | `pct_pop_diff_pp` | `band IN (1,2,3,-1)` |
| 4 | perc_bad_decil | monitoring_band_me_br | `bad_rate` | `band IN (1,2,3)` |
| 5 | perc_bad_decil_diff_pp | monitoring_band_me_br | `bad_rate_diff_pp` | `band IN (1,2,3)` |
| 6 | df_dist_bad_decil | monitoring_band_me_br | `pct_bad_share` | `band IN (1,2,3)` |
| 7 | df_dist_bad_decil_diff_pp | monitoring_band_me_br | `pct_bad_share_diff_pp` | `band IN (1,2,3)` |
| 8 | iep_por_decil | monitoring_band_me_br | `psi_contribution` | `band IN (1,2,3,100)` |
| 9 | decile_migrations | monitoring_band_migrations_me_br | (sem `value` — usar `count`/`pct_of_pop`) | renomear colunas band |
| 10 | iep | monitoring_features_me_br | `psi_feature` | `DISTINCT feature_name, period` |
| 11 | iep_vars | monitoring_features_me_br | `psi_within_group` ⚠️ semântica nova | `DISTINCT feature_name, grupo, period` |
| 12 | dist_vars | monitoring_features_me_br | `pct_pop` | concatenar `feature=bin` |
| 13 | dist_vars_diff_pp | monitoring_features_me_br | `pct_pop_diff_pp` | concatenar `feature=bin` |
| 14 | vol_vars | monitoring_features_me_br | `volume` | concatenar `feature=bin` |
| 15 | risco_relativo | monitoring_features_me_br | `lift` | concatenar `feature=bin` |
| 16 | risco_relativo_trigger | monitoring_features_me_br | `lift_trigger` | concatenar `feature=bin` |

---

## 8. Campos NOVOS em v2 (não existiam no Excel/v1)

### Em `monitoring_metrics_me_br`
- Populações absolutas: `total_clients`, `pop_baixo`, `pop_medio`, `pop_alto`
- Estatísticas de score: `avg_score`, `median_score`
- Estatísticas de limite: `avg_credit_limit`, `median_credit_limit`, `total_credit_limit` (USD e CLP)
- Carteira: `total_billed_usd`, `overdue_usd`, `overdue_pct`
- Bad rate / lift por banda em colunas wide: `bad_rate_baixo/medio/alto`, `lift_baixo/medio/alto`, `bad_rate_overall`
- Classificações PSI: `psi_vs_train_classification`, `psi_rolling_classification` (Estavel/Moderado/Significativo)
- PSI rolling: `psi_rolling` (vs mês anterior, não Train)
- Estabilidade individual: `pct_improved`, `pct_maintained`, `pct_worsened`

### Em `monitoring_band_migrations_me_br`
- `pct_of_pop` (% da população na transição)

### Em `monitoring_features_me_br`
- `bin_label` separado de `feature_name` (no v1 era um campo concatenado)
- `psi_within_group` com **nova semântica** (share fracionário 0–1, em vez do PSI absoluto)

---

## 9. Campos REMOVIDOS / RENOMEADOS

| Campo v1 | Status v2 | Observação |
|---|---|---|
| `update_at` (typo) em `decile_migrations` | **removido** | Só existe `updated_at` agora |
| `reference_year` (INT separado) | **derivado** | Use `YEAR(reference_month)` |
| `reference_month` (INT separado) | **derivado** | Use `MONTH(reference_month)`. ⚠️ nome reciclado: agora é DATE |
| `metric_key` (variado: 'roc', 'ks', etc) | **simplificado** | Sempre 'monitoring' nas tabelas v2 |
| `variaveis` (formato `feature=bin`) | **separado em 2 colunas** | `feature_name` + `bin_label` |
| `coeficientes` (plural) | **renomeado** | `coeficiente` (singular) |
| `previous_decile`, `current_decile` | **renomeados** | `previous_band`, `current_band` |
| `decil` | **renomeado** | `band` |

### ⚠️ Conflito de nome: `reference_month`

No v1 havia **duas** colunas chamadas `reference_month`:
- A da tabela `monitoring_me_br`: **DATE** (safra completa)
- A das 16 tabelas de métricas: **INT** (apenas o número do mês: 1–12)

**Em v2, todas as 5 tabelas usam `reference_month` como DATE.** Se o Power BI esperava INT, derivar com `MONTH(reference_month)`.

---

## 10. Checklist de migração Power BI

- [ ] Atualizar **connection string / catalog** de `teste.*` para `ds_catalog_dev.credit_engine.*`
- [ ] Substituir as 16 fontes de dados pelas **5 novas tabelas** conforme tabela do item 7
- [ ] Adicionar derivações `YEAR()` e `MONTH()` nos relatórios que liam `reference_year`/`reference_month` (INT)
- [ ] **Bad rate**: confirmar que está em **% (0–100)** em todos os visuais (era misto no v1)
- [ ] Trocar `previous_decile`/`current_decile` → `previous_band`/`current_band` no dashboard de migrações
- [ ] Remover referências à coluna `update_at` (typo) — só usar `updated_at`
- [ ] **PSI por feature**: usar `psi_feature` se quiser o valor absoluto; usar `psi_within_group` se quiser share dentro do grupo
- [ ] Aproveitar campos novos: `psi_vs_train_classification`, `pct_improved/maintained/worsened`, `lift_*` por banda
- [ ] Validar período Train: 1 linha em `monitoring_metrics_me_br` (vs 1 linha por métrica em v1)
- [ ] Validar contagens: 30 linhas em metrics, 120 em band, 870 em features, 256 em migrations

---

## 11. Notas finais

- O notebook v2 está em `04_monitoring_me_br_v2.ipynb` (executável no Databricks via `.py` espelhado em `04_monitoring_me_br_v2.py`).
- Catálogo único: **`ds_catalog_dev.credit_engine`**.
- Reexecução é segura: `monitoring_me_br` faz MERGE (preserva histórico); as 4 de métricas fazem OVERWRITE (recalculam tudo a partir do detalhe).
- Para depara do v0 (Excel) → v1 (16 tabelas Delta antigas), ver `depara_monitoramento_me_br.md`.
