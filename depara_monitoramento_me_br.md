# Depara: Monitoramento_me.xlsx × 04_monitoring_me_br.py

**Data:** 2026-05-18  
**Analista:** revisão automática coluna a coluna  
**Escopo:** 17 tabelas (1 detalhe + 16 métricas)

---

## Contexto: adaptação do modelo logístico → fórmula ponderada

O Excel padrão corporativo monitora um **modelo logístico** (dummies, coeficientes,
decis 1-10). O modelo ME BR é uma **fórmula ponderada por severidade de atraso e
grupo econômico** (`adjusted_score = C×score + (1-C)×portfolio_score`).

O mapeamento aplicado mantém todos os nomes de tabela e coluna; apenas o
conteúdo semântico é adaptado onde não há equivalente direto:

| Conceito logístico (Excel) | Equivalente ME BR (notebook) |
|---|---|
| Decil 1–10 | Banda de score: 1-BAIXO / 2-MEDIO / 3-ALTO |
| Dummy do modelo | Feature da fórmula (pct_months_overdue_*, historical_weight, etc.) |
| Coeficiente logístico | Peso efetivo = média da mediana do cluster (NULL se não aplicável) |
| Grupo de dummies | Grupo de negócio (severidade_atraso, peso_historico, exposicao, benchmark) |
| Decil 100 em iep_por_decil | Total PSI (soma das contribuições por banda) |

---

## 1. Tabela detalhe: `monitoring_me_br`

> Não existe no Excel (é a base de onde todas as métricas são calculadas).

| Coluna | Tipo Delta | Descrição |
|---|---|---|
| id_customer | INT | PK do cliente |
| customer_name | STRING | Nome |
| country | STRING | País |
| reference_month | DATE | Safra |
| feature_reference_month | DATE | Safra de features (m-1) |
| total_amount | DOUBLE | Valor total da carteira |
| overdue_amount | DOUBLE | Valor em atraso |
| overdue_pct | DOUBLE | % atraso sobre total |
| months_with_billing | INT | Meses com faturamento |
| months_defaulted | INT | Meses inadimplentes |
| pct_months_overdue_10_20 | DOUBLE | % meses atraso 10-20% |
| pct_months_overdue_20_30 | DOUBLE | % meses atraso 20-30% |
| pct_months_overdue_30_50 | DOUBLE | % meses atraso 30-50% |
| pct_months_overdue_50_plus | DOUBLE | % meses atraso >50% |
| flag_transacted | INT | Flag transacionou |
| score | DOUBLE | Score individual ponderado |
| historical_weight | DOUBLE | Peso histórico |
| reference_value | DOUBLE | Valor de referência (USD) |
| reference_value_clp | DOUBLE | Valor de referência (CLP) |
| payment_term | DOUBLE | Prazo de pagamento |
| median_cluster_10..50 | DOUBLE×4 | Medianas dos clusters de atraso |
| portfolio_score | DOUBLE | Score da carteira (grupo econômico) |
| adjusted_score | DOUBLE | Score ajustado final |
| score_band | STRING | Banda do score individual |
| integrated_score | DOUBLE | Score integrado (base do monitoramento) |
| integrated_score_band | STRING | Banda do score integrado |
| credit_limit | DOUBLE | Limite de crédito (USD) |
| credit_limit_clp | DOUBLE | Limite de crédito (CLP) |
| credit_limit_end | DOUBLE | Limite final aprovado (USD) |
| credit_limit_end_clp | DOUBLE | Limite final aprovado (CLP) |
| target_percent7mob1..12 | INT×5 | Targets binários de inadimplência |
| target_billed/overdue/pct _1m..12m | DOUBLE×12 | Targets financeiros |
| updated_at | TIMESTAMP | Timestamp de carga |

**Ingestão:** `CREATE TABLE IF NOT EXISTS` + `MERGE ON (reference_month, id_customer)`.
Histórico jamais é apagado.

---

## 2. Depara coluna a coluna — 16 tabelas de métricas

### Legenda de status

| Símbolo | Significado |
|---|---|
| ✅ | Coluna idêntica (nome + tipo) |
| 🔄 | Adaptado: mesmo nome, conteúdo semântico adaptado ao modelo ponderado |
| ➕ | Coluna extra gerada (não está no Excel, mas agrega valor) |
| ⚠️ | Diferença identificada e documentada |

---

### 2.1 `performance_me_br`

**Excel:** `performance` | **Delta:** `teste.performance_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | performance | STRING | performance | STRING | ✅ | |
| 2 | period | STRING | period | STRING | ✅ | 'Train' ou 'YYYY/MM' |
| 3 | value | DOUBLE | value | DOUBLE | ✅ | ROC/KS em [0,1] |
| 4 | market_name | STRING | market_name | STRING | ✅ | Fixo 'me' |
| 5 | metric_key | STRING | metric_key | STRING | ✅ | Fixo 'performance' |
| 6 | reference_year | INT | reference_year | INT | ✅ | |
| 7 | reference_month | INT | reference_month | INT | ✅ | |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

**Métricas Excel:** ROC, KS  
**Métricas geradas:** ROC, KS, Gini ➕

> O Gini (2×AUC−1) é adicionado como métrica complementar padrão de ciência de dados. Não quebra compatibilidade.

**MERGE key:** `(performance, period, reference_year, reference_month)` ✅

---

### 2.2 `dist_por_decil_me_br`

**Excel:** `dist_por_decil` | **Delta:** `teste.dist_por_decil_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | decil | INT | decil | INT | 🔄 | Excel: 1-10. ME BR: 1=BAIXO, 2=MEDIO, 3=ALTO, -1=sem score |
| 2 | period | STRING | period | STRING | ✅ | |
| 3 | value | DOUBLE | value | DOUBLE | ✅ | % população (0-100) |
| 4 | market_name | STRING | market_name | STRING | ✅ | |
| 5 | metric_key | STRING | metric_key | STRING | ✅ | |
| 6 | reference_year | INT | reference_year | INT | ✅ | |
| 7 | reference_month | INT | reference_month | INT | ✅ | |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.3 `dist_por_decil_diff_pp_me_br`

**Excel:** `dist_por_decil_diff_pp` | **Delta:** `teste.dist_por_decil_diff_pp_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status |
|---|---|---|---|---|---|
| 1 | decil | INT | decil | INT | 🔄 |
| 2 | period | STRING | period | STRING | ✅ |
| 3 | value | DOUBLE | value | DOUBLE | ✅ |
| 4 | market_name | STRING | market_name | STRING | ✅ |
| 5 | metric_key | STRING | metric_key | STRING | ✅ |
| 6 | reference_year | INT | reference_year | INT | ✅ |
| 7 | reference_month | INT | reference_month | INT | ✅ |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ |

> `value` = distribuição atual (pp) − distribuição Train (pp). Baseline = período 'Train'.

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.4 `perc_bad_decil_me_br`

**Excel:** `perc_bad_decil` | **Delta:** `teste.perc_bad_decil_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status |
|---|---|---|---|---|---|
| 1 | decil | INT | decil | INT | 🔄 |
| 2 | period | STRING | period | STRING | ✅ |
| 3 | value | DOUBLE | value | DOUBLE | ✅ |
| 4 | market_name | STRING | market_name | STRING | ✅ |
| 5 | metric_key | STRING | metric_key | STRING | ✅ |
| 6 | reference_year | INT | reference_year | INT | ✅ |
| 7 | reference_month | INT | reference_month | INT | ✅ |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ |

> `value` = bad rate (%) por banda. Target: `target_percent7mob1`.

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.5 `perc_bad_decil_diff_pp_me_br`

**Excel:** `perc_bad_decil_diff_pp` | **Delta:** `teste.perc_bad_decil_diff_pp_me_br`

Esquema idêntico ao 2.4. `value` = bad rate atual − bad rate Train (pp). ✅

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.6 `df_dist_bad_decil_me_br`

**Excel:** `df_dist_bad_decil` | **Delta:** `teste.df_dist_bad_decil_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status |
|---|---|---|---|---|---|
| 1 | decil | INT | decil | INT | 🔄 |
| 2 | period | STRING | period | STRING | ✅ |
| 3 | value | DOUBLE | value | DOUBLE | ✅ |
| 4 | market_name | STRING | market_name | STRING | ✅ |
| 5 | metric_key | STRING | metric_key | STRING | ✅ |
| 6 | reference_year | INT | reference_year | INT | ✅ |
| 7 | reference_month | INT | reference_month | INT | ✅ |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ |

> `value` = % dos bads concentrado em cada banda (distribuição de bads).

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.7 `df_dist_bad_decil_diff_pp_me_br`

**Excel:** `df_dist_bad_decil_diff_pp` | **Delta:** `teste.df_dist_bad_decil_diff_pp_me_br`

Esquema idêntico ao 2.6. `value` = concentração atual − concentração Train (pp). ✅

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.8 `iep_por_decil_me_br`

**Excel:** `iep_por_decil` | **Delta:** `teste.iep_por_decil_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | decil | INT | decil | INT | 🔄 | Excel: 1-10 + 100 (total). ME BR: 1,2,3 (bandas) + 100 (total PSI) |
| 2 | period | STRING | period | STRING | ✅ | |
| 3 | value | DOUBLE | value | DOUBLE | ✅ | PSI contribution por banda; decil=100 é o PSI total |
| 4 | market_name | STRING | market_name | STRING | ✅ | |
| 5 | metric_key | STRING | metric_key | STRING | ✅ | |
| 6 | reference_year | INT | reference_year | INT | ✅ | |
| 7 | reference_month | INT | reference_month | INT | ✅ | |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

> **Correção aplicada (v2):** versão anterior gerava um único registro por período
> com `decil=0` (PSI agregado). Corrigido para gerar a contribuição PSI por banda
> (decil=1, 2, 3) + linha de total (decil=100), alinhado à estrutura do Excel.

**MERGE key:** `(decil, period, reference_year, reference_month)` ✅

---

### 2.9 `decile_migrations_me_br`

**Excel:** `decile_migrations` | **Delta:** `teste.decile_migrations_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | previous_decile | INT | previous_decile | INT | 🔄 | Banda do mês anterior |
| 2 | current_decile | INT | current_decile | INT | 🔄 | Banda do mês atual |
| 3 | count | INT | count | INT | ✅ | Qtde de clientes nessa transição |
| 4 | period | STRING | period | STRING | ✅ | |
| 5 | market_name | STRING | market_name | STRING | ✅ | |
| 6 | metric_key | STRING | metric_key | STRING | ✅ | |
| 7 | reference_year | INT | reference_year | INT | ✅ | |
| 8 | reference_month | INT | reference_month | INT | ✅ | |
| 9 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |
| 10 | update_at | TIMESTAMP | update_at | TIMESTAMP | ✅ | Mantido (typo do padrão corporativo) |

> O Excel contém a coluna `update_at` (sem 'd') como duplicata de `updated_at`.
> A coluna foi preservada para compatibilidade exata com o padrão.

**MERGE key:** `(previous_decile, current_decile, period, reference_year, reference_month)` ✅

---

### 2.10 `iep_me_br`

**Excel:** `iep` | **Delta:** `teste.iep_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | variaveis | STRING | variaveis | STRING | 🔄 | Excel: nome do grupo de dummies (ex: `valor_atraso_4_6m`). ME BR: nome da feature (ex: `pct_months_overdue_10_20`) |
| 2 | period | STRING | period | STRING | ✅ | |
| 3 | value | DOUBLE | value | DOUBLE | ✅ | PSI da feature vs Train |
| 4 | market_name | STRING | market_name | STRING | ✅ | |
| 5 | metric_key | STRING | metric_key | STRING | ✅ | |
| 6 | reference_year | INT | reference_year | INT | ✅ | |
| 7 | reference_month | INT | reference_month | INT | ✅ | |
| 8 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

> No modelo logístico, `iep` agrupa dummies por categoria (PSI do grupo).
> No modelo ponderado, cada feature já é uma unidade atômica — o PSI é calculado
> por feature individual. Semântica equivalente, nomenclatura adaptada.

**Interpretação PSI:** < 0.10 Estável | 0.10–0.25 Moderado | ≥ 0.25 Significativo

**MERGE key:** `(variaveis, period, reference_year, reference_month)` ✅

---

### 2.11 `iep_vars_me_br`

**Excel:** `iep_vars` | **Delta:** `teste.iep_vars_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | variaveis | STRING | variaveis | STRING | 🔄 | Excel: dummy (ex: `dummy4_valor_atraso_4_6m_SA`). ME BR: nome da feature |
| 2 | grupo | STRING | grupo | STRING | 🔄 | Excel: grupo de dummies. ME BR: grupo de negócio |
| 3 | period | STRING | period | STRING | ✅ | |
| 4 | value | DOUBLE | value | DOUBLE | ✅ | PSI da feature dentro do grupo |
| 5 | market_name | STRING | market_name | STRING | ✅ | |
| 6 | metric_key | STRING | metric_key | STRING | ✅ | |
| 7 | reference_year | INT | reference_year | INT | ✅ | |
| 8 | reference_month | INT | reference_month | INT | ✅ | |
| 9 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

> Mesmo PSI da feature calculado em `iep_me_br`, mas com dimensão adicional `grupo`.
> Permite filtros de dashboard por categoria de variável.

**MERGE key:** `(variaveis, grupo, period, reference_year, reference_month)` ✅

---

### 2.12 `dist_vars_me_br`

**Excel:** `dist_vars` | **Delta:** `teste.dist_vars_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | variaveis | STRING | variaveis | STRING | 🔄 | Excel: dummy (ex: `dummy4_valor_atraso_4_6m_SA`). ME BR: `feature=bin` (ex: `pct_months_overdue_10_20=(-inf,0.2]`) |
| 2 | grupo | STRING | grupo | STRING | 🔄 | Excel: grupo de dummies. ME BR: grupo de negócio |
| 3 | coeficientes | DOUBLE | coeficientes | DOUBLE | 🔄 | Excel: coeficiente logístico. ME BR: média da mediana do cluster (NULL para features sem peso direto) |
| 4 | period | STRING | period | STRING | ✅ | |
| 5 | value | DOUBLE | value | DOUBLE | ✅ | % população na faixa (0-100) |
| 6 | market_name | STRING | market_name | STRING | ✅ | |
| 7 | metric_key | STRING | metric_key | STRING | ✅ | |
| 8 | reference_year | INT | reference_year | INT | ✅ | |
| 9 | reference_month | INT | reference_month | INT | ✅ | |
| 10 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

> **Faixas:** aprendidas por quantis no período Train e reaplicadas em todos os períodos OOT
> (garante comparabilidade de bins ao longo do tempo).  
> **coeficientes:** NULL para features sem peso direto no score (overdue_pct, months_with_billing,
> months_defaulted, reference_value, payment_term, portfolio_score, score).

**MERGE key:** `(variaveis, grupo, period, reference_year, reference_month)` ✅

---

### 2.13 `dist_vars_diff_pp_me_br`

**Excel:** `dist_vars_diff_pp` | **Delta:** `teste.dist_vars_diff_pp_me_br`

Esquema idêntico ao 2.12. `value` = % faixa atual − % faixa Train (pp). ✅

**MERGE key:** `(variaveis, grupo, period, reference_year, reference_month)` ✅

---

### 2.14 `vol_vars_me_br`

**Excel:** `vol_vars` | **Delta:** `teste.vol_vars_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | variaveis | STRING | variaveis | STRING | 🔄 | Mesmo padrão `feature=bin` |
| 2 | grupo | STRING | grupo | STRING | 🔄 | |
| 3 | coeficientes | DOUBLE | coeficientes | DOUBLE | 🔄 | |
| 4 | period | STRING | period | STRING | ✅ | |
| 5 | value | DOUBLE | value | DOUBLE | ✅ | Contagem absoluta de clientes na faixa |
| 6 | market_name | STRING | market_name | STRING | ✅ | |
| 7 | metric_key | STRING | metric_key | STRING | ✅ | |
| 8 | reference_year | INT | reference_year | INT | ✅ | |
| 9 | reference_month | INT | reference_month | INT | ✅ | |
| 10 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

**MERGE key:** `(variaveis, grupo, period, reference_year, reference_month)` ✅

---

### 2.15 `risco_relativo_me_br`

**Excel:** `risco_relativo` | **Delta:** `teste.risco_relativo_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | variaveis | STRING | variaveis | STRING | 🔄 | Excel: dummy. ME BR: `feature=bin` |
| 2 | grupo | STRING | grupo | STRING | ⚠️ | Excel: sempre NULL. ME BR: sempre preenchido com grupo de negócio |
| 3 | period | STRING | period | STRING | ✅ | |
| 4 | value | DOUBLE | value | DOUBLE | ✅ | Lift = bad_rate(faixa) / bad_rate(geral) |
| 5 | market_name | STRING | market_name | STRING | ✅ | |
| 6 | metric_key | STRING | metric_key | STRING | ✅ | |
| 7 | reference_year | INT | reference_year | INT | ✅ | |
| 8 | reference_month | INT | reference_month | INT | ✅ | |
| 9 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

> **⚠️ grupo:** No modelo logístico original, risco_relativo não tem agrupamento
> (dummies são atômicas). Na adaptação ME BR, o campo `grupo` é preenchido com o
> grupo de negócio da feature (consistente com iep_vars e dist_vars). Isso enriquece
> o monitoramento e não quebra compatibilidade — o campo existe no esquema do Excel,
> só estava vazio.

**MERGE key:** `(variaveis, grupo, period, reference_year, reference_month)` ✅

---

### 2.16 `risco_relativo_trigger_me_br`

**Excel:** `risco_relativo_trigger` | **Delta:** `teste.risco_relativo_trigger_me_br`

| # | Coluna Excel | Tipo Excel | Coluna Delta | Tipo Delta | Status | Observação |
|---|---|---|---|---|---|---|
| 1 | variaveis | STRING | variaveis | STRING | 🔄 | Excel: dummy. ME BR: `feature=bin` |
| 2 | grupo | STRING | grupo | STRING | ✅ | |
| 3 | period | STRING | period | STRING | ✅ | |
| 4 | value | STRING | value | STRING | ✅ | `'true'`/`'false'` — alinhado ao Excel |
| 5 | market_name | STRING | market_name | STRING | ✅ | |
| 6 | metric_key | STRING | metric_key | STRING | ✅ | |
| 7 | reference_year | INT | reference_year | INT | ✅ | |
| 8 | reference_month | INT | reference_month | INT | ✅ | |
| 9 | updated_at | TIMESTAMP | updated_at | TIMESTAMP | ✅ | |

> **Regra do trigger:** `'true'` quando lift < 0.5 ou lift > 2.0 (faixa fora do intervalo
> aceitável de [0.5, 2.0]). Corrigido de DOUBLE (1.0/0.0) para STRING ('true'/'false')
> em conformidade com o padrão do Excel.

**MERGE key:** `(variaveis, grupo, period, reference_year, reference_month)` ✅

---

## 3. Resumo executivo: status por tabela

| # | Tabela Excel | Tabela Delta (_me_br) | Colunas Excel | Colunas Geradas | Status Geral |
|---|---|---|---|---|---|
| — | *(não existe)* | monitoring_me_br | — | 39 | ✅ Base detalhe |
| 1 | performance | performance_me_br | 8 | 8 | ✅ +Gini extra |
| 2 | dist_por_decil | dist_por_decil_me_br | 8 | 8 | ✅ |
| 3 | dist_por_decil_diff_pp | dist_por_decil_diff_pp_me_br | 8 | 8 | ✅ |
| 4 | perc_bad_decil | perc_bad_decil_me_br | 8 | 8 | ✅ |
| 5 | perc_bad_decil_diff_pp | perc_bad_decil_diff_pp_me_br | 8 | 8 | ✅ |
| 6 | df_dist_bad_decil | df_dist_bad_decil_me_br | 8 | 8 | ✅ |
| 7 | df_dist_bad_decil_diff_pp | df_dist_bad_decil_diff_pp_me_br | 8 | 8 | ✅ |
| 8 | iep_por_decil | iep_por_decil_me_br | 8 | 8 | ✅ Corrigido v2 |
| 9 | decile_migrations | decile_migrations_me_br | 10 | 10 | ✅ |
| 10 | iep | iep_me_br | 8 | 8 | ✅ |
| 11 | iep_vars | iep_vars_me_br | 9 | 9 | ✅ |
| 12 | dist_vars | dist_vars_me_br | 10 | 10 | ✅ |
| 13 | dist_vars_diff_pp | dist_vars_diff_pp_me_br | 10 | 10 | ✅ |
| 14 | vol_vars | vol_vars_me_br | 10 | 10 | ✅ |
| 15 | risco_relativo | risco_relativo_me_br | 9 | 9 | ✅ |
| 16 | risco_relativo_trigger | risco_relativo_trigger_me_br | 9 | 9 | ✅ Corrigido v2 |

**Total de tabelas:** 16 métricas + 1 detalhe = **17 tabelas** ✅  
**Total de colunas verificadas:** todas as colunas de todas as 16 tabelas de métricas ✅

---

## 4. Correções aplicadas nesta revisão (v2)

### 4.1 `iep_por_decil_me_br` — granularidade por banda

**Problema:** versão anterior calculava um único PSI agregado por período e
gravava com `decil=0`.

**Solução:** Calcula a contribuição individual de cada banda ao PSI total:

```
contrib(banda_b) = (pct_atual_b − pct_train_b) × ln(pct_atual_b / pct_train_b)
```

Gera N+1 linhas por período (N = número de bandas distintas + 1 linha de total
com `decil=100`), exatamente como o Excel gera 10+1 linhas (decis 1-10 + total).

### 4.2 `risco_relativo_trigger_me_br` — tipo da coluna `value`

**Problema:** DDL declarava `value DOUBLE` e o código gravava `1.0`/`0.0`.

**Solução:**
- DDL alterado: `value STRING`
- Código alterado: grava `'true'`/`'false'`
- Schema PySpark atualizado: tipo `StringType()` para `value`

Ambos os fixes estão no commit atual (branch `claude/push-and-analyze-76D8u`).

---

## 5. Comportamento de ingestão (garantia de histórico)

Todas as 17 tabelas usam o padrão:

```sql
CREATE TABLE IF NOT EXISTS teste.<tabela> (...)
USING DELTA
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
```

```sql
MERGE INTO teste.<tabela> AS tgt
USING <source> AS src
ON <chave natural>
WHEN MATCHED THEN UPDATE SET <todas colunas não-chave>
WHEN NOT MATCHED THEN INSERT *
```

**Garantias:**
- Reexecutar a mesma safra **atualiza** sem duplicar
- Safras anteriores permanecem **intactas**
- Não há `DROP TABLE` em nenhum lugar do notebook

---

## 6. Features monitoradas

| Feature | Grupo | Coeficiente efetivo |
|---|---|---|
| pct_months_overdue_10_20 | severidade_atraso | média mediana_cluster_10 |
| pct_months_overdue_20_30 | severidade_atraso | média mediana_cluster_20 |
| pct_months_overdue_30_50 | severidade_atraso | média mediana_cluster_30 |
| pct_months_overdue_50_plus | severidade_atraso | média mediana_cluster_50 |
| overdue_pct | severidade_atraso | NULL |
| months_with_billing | peso_historico | NULL |
| months_defaulted | peso_historico | NULL |
| historical_weight | peso_historico | NULL |
| reference_value | exposicao | NULL |
| payment_term | exposicao | NULL |
| portfolio_score | benchmark | NULL |
| score | score_individual | NULL |

**Binning:** quantis (5 bins) aprendidos no período Train, reaplicados a todos os OOT.
Categorias com ≤ 6 valores únicos são tratadas como discretas.
