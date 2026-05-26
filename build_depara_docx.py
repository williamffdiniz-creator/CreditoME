"""Gera documento Word profissional com depara coluna-a-coluna v1 -> v2.

Para cada uma das 16 tabelas v1, lista TODAS as colunas e indica:
  IDENTICO       — mesmo nome, mesma tabela
  RENOMEADO      — mesmo conteudo, novo nome
  MOVIDO         — colune existe em outra tabela
  TRANSFORMADO   — conteudo passa a estar em coluna(s) diferente(s) por unpivot/concat
  REMOVIDO       — nao existe mais
  DERIVADO       — pode ser reconstruido (ex: reference_year = YEAR(reference_month))
"""
from docx import Document
from docx.shared import Pt, Cm, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_ALIGN_VERTICAL, WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

CATALOG_V2 = "ds_catalog_dev.credit_engine"
CATALOG_V1 = "teste"

# Cores das tarjas de status
STATUS_COLORS = {
    "IDENTICO":     "2E7D32",  # verde
    "RENOMEADO":    "1565C0",  # azul
    "MOVIDO":       "6A1B9A",  # roxo
    "TRANSFORMADO": "EF6C00",  # laranja
    "DERIVADO":     "00838F",  # ciano
    "REMOVIDO":     "C62828",  # vermelho
}

# Estrutura: cada tabela v1 com suas colunas e o mapeamento
TABELAS = [
    {
        "nome_v1": "performance_me_br",
        "descricao": "Performance do modelo (KS, ROC) por safra",
        "tabela_v2": "monitoring_metrics_me_br",
        "obs_geral": (
            "Tabela longa (1 linha por metrica) virou wide: cada metrica vira uma coluna "
            "(ks, roc_auc, gini) em uma unica linha por safra. Use UNPIVOT para reconstruir "
            "o formato antigo se necessario."
        ),
        "colunas": [
            ("performance",      "STRING",    "Nome da metrica (ks/roc)",        "TRANSFORMADO", "monitoring_metrics_me_br", "—",          "Virou coluna: 'ks' -> coluna ks, 'roc' -> coluna roc_auc, novo 'gini' adicionado"),
            ("period",           "STRING",    "'Train' ou 'YYYY/MM'",            "IDENTICO",     "monitoring_metrics_me_br", "period",     ""),
            ("value",            "DOUBLE",    "Valor da metrica",                "TRANSFORMADO", "monitoring_metrics_me_br", "ks / roc_auc / gini", "Distribuido em colunas tipadas; valor em [0,1]"),
            ("market_name",      "STRING",    "Mercado ('me')",                  "IDENTICO",     "monitoring_metrics_me_br", "market_name", ""),
            ("metric_key",       "STRING",    "Chave da metrica",                "TRANSFORMADO", "monitoring_metrics_me_br", "metric_key", "Antes variava ('roc','ks',...). Agora fixo 'monitoring'"),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_metrics_me_br", "YEAR(reference_month)", "Derivar do DATE"),
            ("reference_month",  "INT",       "Mes (1-12)",                      "DERIVADO",     "monitoring_metrics_me_br", "MONTH(reference_month)", "ATENCAO: nome reciclado. Em v2 'reference_month' eh DATE"),
            ("updated_at",       "TIMESTAMP", "Carimbo de carga",                "IDENTICO",     "monitoring_metrics_me_br", "updated_at", ""),
        ],
    },
    {
        "nome_v1": "dist_por_decil_me_br",
        "descricao": "Distribuicao da populacao por banda (decil 1-3 + -1)",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": (
            "Coluna 'value' virou 'pct_pop'. Coluna 'decil' virou 'band' (renomeada). "
            "Filtrar band IN (1, 2, 3, -1) para reproduzir a tabela antiga."
        ),
        "colunas": [
            ("decil",            "INT",       "Banda (1=BAIXO, 2=MEDIO, 3=ALTO, -1=Missing)", "RENOMEADO", "monitoring_band_me_br", "band",                ""),
            ("period",           "STRING",    "'Train' ou 'YYYY/MM'",            "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "% populacao na banda",            "RENOMEADO",    "monitoring_band_me_br", "pct_pop",               "Mesma escala (0-100)"),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            "Fixo 'monitoring' agora"),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", "Coluna 'reference_month' em v2 eh DATE"),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "dist_por_decil_diff_pp_me_br",
        "descricao": "Diferenca em pp da distribuicao vs Train",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": "Mesma tabela_v2 que dist_por_decil; 'value' virou coluna 'pct_pop_diff_pp'.",
        "colunas": [
            ("decil",            "INT",       "Banda",                           "RENOMEADO",    "monitoring_band_me_br", "band",                  ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Diff em pp vs Train",             "RENOMEADO",    "monitoring_band_me_br", "pct_pop_diff_pp",       ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            "Fixo 'monitoring'"),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "perc_bad_decil_me_br",
        "descricao": "Bad rate (%) por banda",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": "'value' virou 'bad_rate'. ATENCAO: v2 padroniza em % (0-100).",
        "colunas": [
            ("decil",            "INT",       "Banda",                           "RENOMEADO",    "monitoring_band_me_br", "band",                  ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Bad rate (%)",                    "RENOMEADO",    "monitoring_band_me_br", "bad_rate",              "% (0-100) padronizado"),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "perc_bad_decil_diff_pp_me_br",
        "descricao": "Diferenca em pp do bad rate por banda vs Train",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": "'value' virou 'bad_rate_diff_pp'.",
        "colunas": [
            ("decil",            "INT",       "Banda",                           "RENOMEADO",    "monitoring_band_me_br", "band",                  ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Diff em pp vs Train",             "RENOMEADO",    "monitoring_band_me_br", "bad_rate_diff_pp",      ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "df_dist_bad_decil_me_br",
        "descricao": "Concentracao de bads (% dos bads em cada banda)",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": "'value' virou 'pct_bad_share'.",
        "colunas": [
            ("decil",            "INT",       "Banda",                           "RENOMEADO",    "monitoring_band_me_br", "band",                  ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "% dos bads na banda",             "RENOMEADO",    "monitoring_band_me_br", "pct_bad_share",         ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "df_dist_bad_decil_diff_pp_me_br",
        "descricao": "Diferenca em pp da concentracao de bads vs Train",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": "'value' virou 'pct_bad_share_diff_pp'.",
        "colunas": [
            ("decil",            "INT",       "Banda",                           "RENOMEADO",    "monitoring_band_me_br", "band",                  ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Diff em pp vs Train",             "RENOMEADO",    "monitoring_band_me_br", "pct_bad_share_diff_pp", ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "iep_por_decil_me_br",
        "descricao": "Contribuicao PSI por banda (decil=100 eh o total)",
        "tabela_v2": "monitoring_band_me_br",
        "obs_geral": "'value' virou 'psi_contribution'. decil=100 (total) preservado em band=100.",
        "colunas": [
            ("decil",            "INT",       "Banda (100 = total)",             "RENOMEADO",    "monitoring_band_me_br", "band",                  "band=100 = PSI total"),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Contribuicao PSI",                "RENOMEADO",    "monitoring_band_me_br", "psi_contribution",      ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "decile_migrations_me_br",
        "descricao": "Matriz de migracao banda(m-1) -> banda(m)",
        "tabela_v2": "monitoring_band_migrations_me_br",
        "obs_geral": "Colunas 'previous_decile'/'current_decile' renomeadas para 'previous_band'/'current_band'. Coluna 'update_at' (typo) REMOVIDA.",
        "colunas": [
            ("previous_decile",  "INT",       "Banda anterior",                  "RENOMEADO",    "monitoring_band_migrations_me_br", "previous_band", ""),
            ("current_decile",   "INT",       "Banda atual",                     "RENOMEADO",    "monitoring_band_migrations_me_br", "current_band",  ""),
            ("count",            "INT",       "Qtde de clientes",                "IDENTICO",     "monitoring_band_migrations_me_br", "count",         ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_band_migrations_me_br", "period",        ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_band_migrations_me_br", "market_name",   ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_band_migrations_me_br", "metric_key",    "Fixo 'monitoring'"),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_band_migrations_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_band_migrations_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_band_migrations_me_br", "updated_at",    ""),
            ("update_at",        "TIMESTAMP", "Duplicata (typo do padrao)",      "REMOVIDO",     "—",                                "—",             "Coluna duplicada (typo) eliminada"),
        ],
    },
    {
        "nome_v1": "iep_me_br",
        "descricao": "PSI por feature (sem dimensao de bin)",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": (
            "'value' virou 'psi_feature'. ATENCAO: em v2 o PSI eh repetido em todos os bins "
            "da feature. Use SELECT DISTINCT (feature_name, period, psi_feature)."
        ),
        "colunas": [
            ("variaveis",        "STRING",    "Nome da feature",                 "RENOMEADO",    "monitoring_features_me_br", "feature_name",          "Coluna 'variaveis' separada em 'feature_name' + 'bin_label'"),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "DOUBLE",    "PSI da feature",                  "RENOMEADO",    "monitoring_features_me_br", "psi_feature",           "Repetido em todos os bins; usar DISTINCT"),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "iep_vars_me_br",
        "descricao": "PSI da feature dentro do grupo (mesma metrica de iep, com grupo)",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": (
            "ATENCAO MUDANCA DE SEMANTICA: 'value' virou 'psi_within_group' que agora eh o "
            "share fracionario (0-1) da feature dentro do PSI do grupo. Se voce queria o PSI "
            "absoluto, use 'psi_feature'. Tambem repetido em todos os bins (usar DISTINCT)."
        ),
        "colunas": [
            ("variaveis",        "STRING",    "Nome da feature",                 "RENOMEADO",    "monitoring_features_me_br", "feature_name",          ""),
            ("grupo",            "STRING",    "Grupo de negocio",                "IDENTICO",     "monitoring_features_me_br", "grupo",                 ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "DOUBLE",    "PSI dentro do grupo",             "TRANSFORMADO", "monitoring_features_me_br", "psi_within_group",      "MUDANCA SEMANTICA: era PSI absoluto, agora eh share 0-1. Para valor absoluto: use psi_feature"),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "dist_vars_me_br",
        "descricao": "Distribuicao da populacao por feature x bin",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": (
            "Coluna 'variaveis' continha 'feature=bin' concatenado. Em v2 separada em "
            "'feature_name' + 'bin_label'. Para reconstruir: CONCAT(feature_name, '=', bin_label)."
        ),
        "colunas": [
            ("variaveis",        "STRING",    "feature=bin (concatenado)",       "TRANSFORMADO", "monitoring_features_me_br", "feature_name + bin_label", "CONCAT(feature_name,'=',bin_label) para reconstruir"),
            ("grupo",            "STRING",    "Grupo de negocio",                "IDENTICO",     "monitoring_features_me_br", "grupo",                 ""),
            ("coeficientes",     "DOUBLE",    "Peso/coeficiente",                "RENOMEADO",    "monitoring_features_me_br", "coeficiente",           "Singular agora; mesmo conteudo"),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "DOUBLE",    "% populacao na faixa",            "RENOMEADO",    "monitoring_features_me_br", "pct_pop",               ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "dist_vars_diff_pp_me_br",
        "descricao": "Diferenca em pp da distribuicao por feature x bin vs Train",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": "'value' virou 'pct_pop_diff_pp'.",
        "colunas": [
            ("variaveis",        "STRING",    "feature=bin",                     "TRANSFORMADO", "monitoring_features_me_br", "feature_name + bin_label", ""),
            ("grupo",            "STRING",    "Grupo",                           "IDENTICO",     "monitoring_features_me_br", "grupo",                 ""),
            ("coeficientes",     "DOUBLE",    "Peso",                            "RENOMEADO",    "monitoring_features_me_br", "coeficiente",           ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Diff pp vs Train",                "RENOMEADO",    "monitoring_features_me_br", "pct_pop_diff_pp",       ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "vol_vars_me_br",
        "descricao": "Volume (contagem absoluta) por feature x bin",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": "'value' virou 'volume'.",
        "colunas": [
            ("variaveis",        "STRING",    "feature=bin",                     "TRANSFORMADO", "monitoring_features_me_br", "feature_name + bin_label", ""),
            ("grupo",            "STRING",    "Grupo",                           "IDENTICO",     "monitoring_features_me_br", "grupo",                 ""),
            ("coeficientes",     "DOUBLE",    "Peso",                            "RENOMEADO",    "monitoring_features_me_br", "coeficiente",           ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Contagem absoluta",               "RENOMEADO",    "monitoring_features_me_br", "volume",                ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "risco_relativo_me_br",
        "descricao": "Lift (risco relativo) por feature x bin",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": "'value' virou 'lift'.",
        "colunas": [
            ("variaveis",        "STRING",    "feature=bin",                     "TRANSFORMADO", "monitoring_features_me_br", "feature_name + bin_label", ""),
            ("grupo",            "STRING",    "Grupo",                           "IDENTICO",     "monitoring_features_me_br", "grupo",                 "Em v1 vinha NULL; em v2 vem preenchido"),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "DOUBLE",    "Lift = bad_rate(bin)/bad_rate",   "RENOMEADO",    "monitoring_features_me_br", "lift",                  ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
    {
        "nome_v1": "risco_relativo_trigger_me_br",
        "descricao": "Flag de alerta de lift (true se lift fora [0.5, 2.0])",
        "tabela_v2": "monitoring_features_me_br",
        "obs_geral": "'value' virou 'lift_trigger' (STRING 'true'/'false').",
        "colunas": [
            ("variaveis",        "STRING",    "feature=bin",                     "TRANSFORMADO", "monitoring_features_me_br", "feature_name + bin_label", ""),
            ("grupo",            "STRING",    "Grupo",                           "IDENTICO",     "monitoring_features_me_br", "grupo",                 ""),
            ("period",           "STRING",    "Periodo",                         "IDENTICO",     "monitoring_features_me_br", "period",                ""),
            ("value",            "STRING",    "'true' / 'false'",                "RENOMEADO",    "monitoring_features_me_br", "lift_trigger",          ""),
            ("market_name",      "STRING",    "Mercado",                         "IDENTICO",     "monitoring_features_me_br", "market_name",           ""),
            ("metric_key",       "STRING",    "Chave",                           "TRANSFORMADO", "monitoring_features_me_br", "metric_key",            ""),
            ("reference_year",   "INT",       "Ano",                             "DERIVADO",     "monitoring_features_me_br", "YEAR(reference_month)", ""),
            ("reference_month",  "INT",       "Mes",                             "DERIVADO",     "monitoring_features_me_br", "MONTH(reference_month)", ""),
            ("updated_at",       "TIMESTAMP", "Carimbo",                         "IDENTICO",     "monitoring_features_me_br", "updated_at",            ""),
        ],
    },
]


# ============================================================
# Helpers de formatacao
# ============================================================
def set_cell_bg(cell, hex_color):
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:fill'), hex_color)
    tc_pr.append(shd)

def set_cell_borders(cell, color="BFBFBF", size="4"):
    tc_pr = cell._tc.get_or_add_tcPr()
    tcBorders = OxmlElement('w:tcBorders')
    for border_name in ('top', 'left', 'bottom', 'right'):
        b = OxmlElement(f'w:{border_name}')
        b.set(qn('w:val'), 'single')
        b.set(qn('w:sz'), size)
        b.set(qn('w:color'), color)
        tcBorders.append(b)
    tc_pr.append(tcBorders)

def set_table_borders_all(table, color="BFBFBF"):
    for row in table.rows:
        for cell in row.cells:
            set_cell_borders(cell, color=color)

def add_paragraph_run(par, text, bold=False, color=None, size=None, font_name="Calibri"):
    run = par.add_run(text)
    run.font.name = font_name
    if bold: run.bold = True
    if color: run.font.color.rgb = RGBColor.from_string(color)
    if size: run.font.size = Pt(size)
    return run

def style_header_cell(cell, text, bg="1F3864"):
    cell.text = ""
    p = cell.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.LEFT
    add_paragraph_run(p, text, bold=True, color="FFFFFF", size=10, font_name="Calibri")
    set_cell_bg(cell, bg)
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

def style_status_cell(cell, status):
    cell.text = ""
    p = cell.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    add_paragraph_run(p, status, bold=True, color="FFFFFF", size=9, font_name="Calibri")
    bg = STATUS_COLORS.get(status, "555555")
    set_cell_bg(cell, bg)
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

def style_data_cell(cell, text, bold=False, mono=False, size=9, alt_row=False):
    cell.text = ""
    p = cell.paragraphs[0]
    add_paragraph_run(p, text, bold=bold, size=size, font_name=("Consolas" if mono else "Calibri"))
    if alt_row:
        set_cell_bg(cell, "F2F2F2")
    cell.vertical_alignment = WD_ALIGN_VERTICAL.CENTER

def add_horizontal_line(doc):
    p = doc.add_paragraph()
    pPr = p._p.get_or_add_pPr()
    pBdr = OxmlElement('w:pBdr')
    bottom = OxmlElement('w:bottom')
    bottom.set(qn('w:val'), 'single')
    bottom.set(qn('w:sz'), '6')
    bottom.set(qn('w:color'), '1F3864')
    pBdr.append(bottom)
    pPr.append(pBdr)

def set_col_widths(table, widths_cm):
    for row in table.rows:
        for i, w in enumerate(widths_cm):
            row.cells[i].width = Cm(w)

# ============================================================
# Build document
# ============================================================
doc = Document()

# Page margins
for section in doc.sections:
    section.left_margin = Cm(1.8)
    section.right_margin = Cm(1.8)
    section.top_margin = Cm(2.0)
    section.bottom_margin = Cm(2.0)

# Default font
style = doc.styles['Normal']
style.font.name = "Calibri"
style.font.size = Pt(11)

# ============================================================
# CAPA
# ============================================================
title = doc.add_paragraph()
title.alignment = WD_ALIGN_PARAGRAPH.CENTER
add_paragraph_run(title, "\n\n\n", size=14)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
add_paragraph_run(p, "DEPARA TECNICO", bold=True, color="1F3864", size=24)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
add_paragraph_run(p, "Monitoramento ME BR — Migracao v1 para v2", bold=True, color="1F3864", size=18)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
add_paragraph_run(p, "Mapeamento coluna a coluna das 16 tabelas antigas\npara as 5 tabelas consolidadas", color="595959", size=13)

doc.add_paragraph()
doc.add_paragraph()

# Caixa de informacao
info_tbl = doc.add_table(rows=5, cols=2)
info_tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
info_rows = [
    ("Documento", "Depara v1 -> v2 — Monitoramento ME BR"),
    ("Versao",    "1.0"),
    ("Data",      "2026-05-26"),
    ("Destinatario", "Time de Business Intelligence"),
    ("Finalidade", "Reapontamento de fontes do Power BI"),
]
for i, (k, v) in enumerate(info_rows):
    style_data_cell(info_tbl.rows[i].cells[0], k, bold=True, size=11)
    style_data_cell(info_tbl.rows[i].cells[1], v, size=11)
    set_cell_bg(info_tbl.rows[i].cells[0], "D9E2F3")
set_table_borders_all(info_tbl, color="1F3864")
set_col_widths(info_tbl, [5, 10])

doc.add_page_break()

# ============================================================
# 1. INTRODUCAO
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "1. Introducao", bold=True, color="1F3864", size=20)
add_horizontal_line(doc)

doc.add_paragraph(
    "Este documento apresenta o mapeamento tecnico, coluna a coluna, das 16 tabelas "
    "do antigo padrao Excel (replicadas em Delta no schema 'teste') para as 5 tabelas "
    "consolidadas do novo padrao em 'ds_catalog_dev.credit_engine'."
)

doc.add_paragraph(
    "A consolidacao foi motivada por duas razoes principais: (1) reducao de "
    "duplicidade — varias das tabelas antigas armazenavam metricas correlatas em "
    "linhas separadas com a coluna 'value' generica; (2) padronizacao de tipos e "
    "unidades — bad rate agora sempre em % (0-100), reference_month sempre em DATE, "
    "metric_key sempre fixo em 'monitoring'."
)

doc.add_paragraph(
    "Cada tabela v1 tem uma secao dedicada listando TODAS as suas colunas e indicando "
    "exatamente onde elas estao na nova estrutura, ou se foram removidas/transformadas."
)

# ============================================================
# 2. CATALOGOS
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "2. Catalogos e nomes plenamente qualificados", bold=True, color="1F3864", size=16)

tbl = doc.add_table(rows=3, cols=2)
style_header_cell(tbl.rows[0].cells[0], "Ambiente")
style_header_cell(tbl.rows[0].cells[1], "Catalogo / Schema")
style_data_cell(tbl.rows[1].cells[0], "Antes (v1)", bold=True)
style_data_cell(tbl.rows[1].cells[1], f"{CATALOG_V1}.<tabela>", mono=True)
style_data_cell(tbl.rows[2].cells[0], "Depois (v2)", bold=True)
style_data_cell(tbl.rows[2].cells[1], f"{CATALOG_V2}.<tabela>", mono=True)
set_table_borders_all(tbl)
set_col_widths(tbl, [4, 12])

doc.add_paragraph()

# ============================================================
# 3. LEGENDA DE STATUS
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "3. Legenda de status das colunas", bold=True, color="1F3864", size=16)

legend = [
    ("IDENTICO",     "Mesmo nome, mesma tabela, mesmo tipo. Nenhuma acao necessaria."),
    ("RENOMEADO",    "Mesmo conteudo, mas com novo nome na v2. Atualizar referencia."),
    ("MOVIDO",       "Coluna agora vive em outra tabela. Trocar fonte de dados."),
    ("TRANSFORMADO", "Conteudo passou a ser representado de forma diferente (unpivot, concat, etc.). Requer ajuste de query."),
    ("DERIVADO",     "Coluna nao existe explicitamente; pode ser calculada (ex: YEAR(reference_month))."),
    ("REMOVIDO",     "Coluna nao tem equivalente na v2. Deve ser eliminada do dashboard."),
]
tbl = doc.add_table(rows=len(legend)+1, cols=2)
style_header_cell(tbl.rows[0].cells[0], "Status")
style_header_cell(tbl.rows[0].cells[1], "Significado")
for i, (s, desc) in enumerate(legend):
    style_status_cell(tbl.rows[i+1].cells[0], s)
    style_data_cell(tbl.rows[i+1].cells[1], desc, size=10)
set_table_borders_all(tbl)
set_col_widths(tbl, [3.2, 13])

doc.add_paragraph()

# ============================================================
# 4. RESUMO EXECUTIVO
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "4. Resumo executivo — visao 16 -> 5", bold=True, color="1F3864", size=16)

resumo = [
    ("performance_me_br",                  "monitoring_metrics_me_br"),
    ("dist_por_decil_me_br",               "monitoring_band_me_br"),
    ("dist_por_decil_diff_pp_me_br",       "monitoring_band_me_br"),
    ("perc_bad_decil_me_br",               "monitoring_band_me_br"),
    ("perc_bad_decil_diff_pp_me_br",       "monitoring_band_me_br"),
    ("df_dist_bad_decil_me_br",            "monitoring_band_me_br"),
    ("df_dist_bad_decil_diff_pp_me_br",    "monitoring_band_me_br"),
    ("iep_por_decil_me_br",                "monitoring_band_me_br"),
    ("decile_migrations_me_br",            "monitoring_band_migrations_me_br"),
    ("iep_me_br",                          "monitoring_features_me_br"),
    ("iep_vars_me_br",                     "monitoring_features_me_br"),
    ("dist_vars_me_br",                    "monitoring_features_me_br"),
    ("dist_vars_diff_pp_me_br",            "monitoring_features_me_br"),
    ("vol_vars_me_br",                     "monitoring_features_me_br"),
    ("risco_relativo_me_br",               "monitoring_features_me_br"),
    ("risco_relativo_trigger_me_br",       "monitoring_features_me_br"),
]
tbl = doc.add_table(rows=len(resumo)+1, cols=3)
style_header_cell(tbl.rows[0].cells[0], "#")
style_header_cell(tbl.rows[0].cells[1], "Tabela v1 (antiga)")
style_header_cell(tbl.rows[0].cells[2], "Tabela v2 (nova)")
for i, (v1, v2) in enumerate(resumo):
    alt = (i % 2 == 1)
    style_data_cell(tbl.rows[i+1].cells[0], str(i+1), size=9, alt_row=alt)
    style_data_cell(tbl.rows[i+1].cells[1], v1, mono=True, size=9, alt_row=alt)
    style_data_cell(tbl.rows[i+1].cells[2], v2, mono=True, size=9, alt_row=alt)
set_table_borders_all(tbl)
set_col_widths(tbl, [1.2, 7.5, 7.5])

doc.add_page_break()

# ============================================================
# 5. DEPARA COLUNA A COLUNA — 16 SECOES
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "5. Depara coluna a coluna", bold=True, color="1F3864", size=20)
add_horizontal_line(doc)

doc.add_paragraph(
    "As 16 secoes a seguir listam, para cada tabela v1, todas as suas colunas "
    "originais e a correspondencia exata na estrutura v2."
)

for idx, t in enumerate(TABELAS, start=1):
    if idx > 1:
        doc.add_paragraph()

    # Subtitulo da tabela
    h = doc.add_paragraph()
    add_paragraph_run(h, f"5.{idx}. ", bold=True, color="1F3864", size=14)
    add_paragraph_run(h, t["nome_v1"], bold=True, color="000000", size=14, font_name="Consolas")

    # Box de descricao
    desc_tbl = doc.add_table(rows=4, cols=2)
    style_data_cell(desc_tbl.rows[0].cells[0], "Descricao", bold=True, size=10)
    style_data_cell(desc_tbl.rows[0].cells[1], t["descricao"], size=10)
    set_cell_bg(desc_tbl.rows[0].cells[0], "D9E2F3")

    style_data_cell(desc_tbl.rows[1].cells[0], "Tabela v1", bold=True, size=10)
    style_data_cell(desc_tbl.rows[1].cells[1], f"{CATALOG_V1}.{t['nome_v1']}", mono=True, size=10)
    set_cell_bg(desc_tbl.rows[1].cells[0], "D9E2F3")

    style_data_cell(desc_tbl.rows[2].cells[0], "Tabela v2 destino", bold=True, size=10)
    style_data_cell(desc_tbl.rows[2].cells[1], f"{CATALOG_V2}.{t['tabela_v2']}", mono=True, size=10)
    set_cell_bg(desc_tbl.rows[2].cells[0], "D9E2F3")

    style_data_cell(desc_tbl.rows[3].cells[0], "Observacao geral", bold=True, size=10)
    style_data_cell(desc_tbl.rows[3].cells[1], t["obs_geral"], size=10)
    set_cell_bg(desc_tbl.rows[3].cells[0], "D9E2F3")
    set_table_borders_all(desc_tbl)
    set_col_widths(desc_tbl, [4, 13])

    doc.add_paragraph()

    # Tabela com colunas
    cols = t["colunas"]
    headers = ["#", "Coluna v1", "Tipo v1", "Descricao v1", "Status", "Tabela v2", "Coluna v2", "Observacao"]
    tbl = doc.add_table(rows=len(cols)+1, cols=len(headers))
    for i, hd in enumerate(headers):
        style_header_cell(tbl.rows[0].cells[i], hd)

    for i, (col_v1, tipo_v1, desc_v1, status, tab_v2, col_v2, obs) in enumerate(cols):
        alt = (i % 2 == 1)
        row = tbl.rows[i+1]
        style_data_cell(row.cells[0], str(i+1), size=8, alt_row=alt)
        style_data_cell(row.cells[1], col_v1, mono=True, bold=True, size=8, alt_row=alt)
        style_data_cell(row.cells[2], tipo_v1, mono=True, size=8, alt_row=alt)
        style_data_cell(row.cells[3], desc_v1, size=8, alt_row=alt)
        style_status_cell(row.cells[4], status)
        style_data_cell(row.cells[5], tab_v2, mono=True, size=8, alt_row=alt)
        style_data_cell(row.cells[6], col_v2, mono=True, bold=True, size=8, alt_row=alt)
        style_data_cell(row.cells[7], obs, size=8, alt_row=alt)

    set_table_borders_all(tbl)
    # Largura total ~17cm
    set_col_widths(tbl, [0.6, 2.5, 1.4, 2.6, 2.0, 2.6, 2.4, 3.0])

    if idx % 2 == 0 and idx < len(TABELAS):
        doc.add_page_break()

doc.add_page_break()

# ============================================================
# 6. SECAO ESPECIAL — TABELA DETALHE
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "6. Tabela detalhe (mantida)", bold=True, color="1F3864", size=20)
add_horizontal_line(doc)

p = doc.add_paragraph()
add_paragraph_run(p, "A tabela ", size=11)
add_paragraph_run(p, "monitoring_me_br", font_name="Consolas", size=11)
add_paragraph_run(p, " (detalhe cliente x safra) NAO foi consolidada. Continua existindo na v2 com o mesmo nome e o mesmo schema. Apenas mudou de catalogo:", size=11)

tbl = doc.add_table(rows=3, cols=2)
style_header_cell(tbl.rows[0].cells[0], "Ambiente")
style_header_cell(tbl.rows[0].cells[1], "Nome plenamente qualificado")
style_data_cell(tbl.rows[1].cells[0], "v1", bold=True)
style_data_cell(tbl.rows[1].cells[1], f"{CATALOG_V1}.monitoring_me_br", mono=True)
style_data_cell(tbl.rows[2].cells[0], "v2", bold=True)
style_data_cell(tbl.rows[2].cells[1], f"{CATALOG_V2}.monitoring_me_br", mono=True)
set_table_borders_all(tbl)
set_col_widths(tbl, [3, 13])

doc.add_paragraph()

p = doc.add_paragraph()
add_paragraph_run(p, "Todas as colunas existentes em v1 permanecem em v2 com nome e tipo identicos. Para o Power BI, basta atualizar a string de conexao do catalogo. Para o schema completo, consultar o ", size=11)
add_paragraph_run(p, "depara_monitoramento_me_br.md", font_name="Consolas", size=11)
add_paragraph_run(p, " no repositorio.", size=11)

doc.add_page_break()

# ============================================================
# 7. CHECKLIST DE MIGRACAO
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "7. Checklist para o time de BI", bold=True, color="1F3864", size=20)
add_horizontal_line(doc)

checklist = [
    "Atualizar a connection string / catalogo de 'teste.*' para 'ds_catalog_dev.credit_engine.*' em todas as fontes.",
    "Substituir as 16 fontes de dados pelas 5 novas tabelas, conforme item 4 (resumo executivo).",
    "Para queries que referenciam 'reference_year' (INT), substituir por YEAR(reference_month).",
    "Para queries que referenciam 'reference_month' (INT 1-12), substituir por MONTH(reference_month). ATENCAO: o nome 'reference_month' foi reciclado para DATE em v2.",
    "Validar que todos os visuais de 'bad rate' usam % (0-100). v2 padroniza essa unidade.",
    "Substituir 'previous_decile' / 'current_decile' por 'previous_band' / 'current_band' no dashboard de migracoes.",
    "Remover qualquer referencia a coluna 'update_at' (typo) — usar apenas 'updated_at'.",
    "Em queries que usavam 'iep_me_br' (PSI por feature), aplicar SELECT DISTINCT para nao duplicar o valor (PSI eh repetido em todos os bins).",
    "Conferir se 'iep_vars' era usado pelo valor absoluto do PSI. Se sim, trocar para 'psi_feature' em vez de 'psi_within_group' (mudou de semantica).",
    "Em queries de 'dist_vars', 'vol_vars', 'risco_relativo*': reconstruir o campo 'variaveis' com CONCAT(feature_name, '=', bin_label).",
    "Validar contagens esperadas: 30 linhas em metrics, 120 em band, 870 em features, 256 em migrations.",
    "Aproveitar campos novos disponiveis em monitoring_metrics_me_br: psi_vs_train_classification, pct_improved/maintained/worsened, lift_baixo/medio/alto.",
]
for i, item in enumerate(checklist, 1):
    p = doc.add_paragraph()
    add_paragraph_run(p, f"  [ ]  {i:02d}. ", bold=True, color="1F3864", size=11)
    add_paragraph_run(p, item, size=11)

doc.add_page_break()

# ============================================================
# 8. CONTATO
# ============================================================
h = doc.add_paragraph()
add_paragraph_run(h, "8. Contato e referencias", bold=True, color="1F3864", size=20)
add_horizontal_line(doc)

tbl = doc.add_table(rows=4, cols=2)
items = [
    ("Repositorio",      "williamffdiniz-creator/CreditoME"),
    ("Branch",           "claude/push-and-analyze-76D8u"),
    ("Notebook v2",      "04_monitoring_me_br_v2.ipynb"),
    ("Versao .py",       "04_monitoring_me_br_v2.py (formato Databricks)"),
]
for i, (k, v) in enumerate(items):
    style_data_cell(tbl.rows[i].cells[0], k, bold=True, size=11)
    style_data_cell(tbl.rows[i].cells[1], v, mono=True, size=11)
    set_cell_bg(tbl.rows[i].cells[0], "D9E2F3")
set_table_borders_all(tbl)
set_col_widths(tbl, [4, 12])

doc.add_paragraph()
doc.add_paragraph()
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
add_paragraph_run(p, "— Fim do documento —", color="595959", size=10)

# Save
output = "/home/user/CreditoME/depara_v1_to_v2_coluna_a_coluna.docx"
doc.save(output)
print(f"OK: {output}")
