IF OBJECT_ID('TEMPDB..#TAB_COMPRADOR_PECUARISTA') IS NOT NULL
BEGIN
    DROP TABLE #TAB_COMPRADOR_PECUARISTA;
END;

SELECT DISTINCT
    a.Ordem_Compra,
    a.Data_Compra,
    eomonth(a.Data_Abate) as periodo_abate,
    a.Cod_Comprador,
    comprador.nom_pessoa AS Nome_Comprador,
    a.Cod_Pecuarista,
    pecuarista.nom_pessoa AS Nome_Pecuarista,
    a.Cod_Empresa,
    a.Cod_Fazenda
INTO #TAB_COMPRADOR_PECUARISTA
FROM dw.advanced_analytics.tab_Abate AS a
LEFT JOIN IKOL.dbpessoa.dbo.tab_pessoa AS comprador ON comprador.cod_pessoa = a.Cod_Comprador
LEFT JOIN IKOL.dbpessoa.dbo.tab_pessoa AS pecuarista ON pecuarista.cod_pessoa = a.Cod_Pecuarista
WHERE a.Data_Compra > '2022-12-31';


IF OBJECT_ID ('TEMPDB..#BASE_UNICA_ABATE') IS NOT NULL DROP TABLE #BASE_UNICA_ABATE;
with main_table as( select
		  a.Cod_Empresa,
		  Cod_Fazenda,
		  Cod_Pecuarista,
		  Num_Registro_Geral,
		  eomonth(Data_Abate) as periodo_abate,
		  nom_pessoa as nome_pecuarista,
		  Ordem_Compra,
		  Terminacao,
		  a.Raca_Animal,
		  Arrobas,
		  Animal,
		  Hematoma,
		  nom_pessoa,
		  Sigla_Acabamento,
		  Maturidade_Etiqueta,
		  a.PH,
		  tle.nom_localidade as estado,
		  d.nom_localidade  as cidade ,
		  Raio,
		  Sexo,
		  Km,
		  Kgs,
		  Habilitacao,
		  Negociacao,
		  MesInicialMaturidade,
		  MesFinalMaturidade,
		  Mat2,
		  qtd_area_propriedade  ,
		  cast(Estancia92 as float) as Estancia92,
		 cast(Organico as float) as Organico,
	     cast(Hilton as float) as Hilton,
		 cast(Angus as float) as flag_raca_angus,
		  Data_Abate,
		  year(Data_Abate) as ano_abate,
		  vlr_arroba_previsto,
		  vlr_arroba_realizado,
		  cast(h.dta_cadastramento as date) as dta_cadastramento,
		  vlr_arroba_realizado*Arrobas as preco_compra_realizado,
		case when vlr_arroba_realizado   <= 205 then 'at  205'
			 when vlr_arroba_realizado <= 225 then   '206 a 225'
			 when vlr_arroba_realizado <= 250 then   '226 a 250'
			 when vlr_arroba_realizado > 250 then    'acima 250'
			 else 'vazio' end as faixa_valor_arroba

	  from dw.advanced_analytics.tab_Abate a WITH (NOLOCK)

		 left join  ikol.dbpessoa.dbo.tab_logradouro c WITH (NOLOCK)
		 ON c.cod_pessoa = a.Cod_Pecuarista AND c.seq_logradouro = a.Cod_Fazenda
		 left join  ikol.dbpessoa.dbo.tab_localidade d WITH (NOLOCK)
		 on d.cod_localidade = c.cod_localidade_cidade
		 left join ikol.dbpessoa.dbo.tab_localidade AS tle WITH (NOLOCK)
		 ON tle.cod_localidade = c.cod_localidade_estado
		 left join IKOL.dbpessoa.dbo.tab_propriedade_rural f WITH (NOLOCK)
		 on a.Cod_Pecuarista = f.cod_pessoa and a.Cod_Fazenda = f.seq_logradouro_propriedade
		 left join ikol.dbpcpfri.dbo.tab_resultado_gado g
		 on g.num_ordem_compra =a.ordem_compra and a.cod_empresa =g.cod_empresa
		 left join IKOL.dbpessoa.dbo.tab_pessoa h WITH (NOLOCK)
		 on a.Cod_Pecuarista = h.cod_pessoa
	where Data_Abate > = '2022-12-31' and
		   a.Cod_Empresa != '304'
	      --and Cod_Doenca = 0
		  and Data_Abate < GETDATE()
),
transpose_habilitaca as (
SELECT Cod_Empresa,
	   Num_Registro_Geral,
	   max(case when value in ('CH-C', 'CH-U', 'CH-V', 'CHILE') then 1 else 0 end) as flag_habilitacao_chile,
	   max(case when value in ('CHINA') then 1
	            when a.Cod_Empresa = '27'
	                 AND a.PH <= 5.88
	                 AND a.Maturidade_Etiqueta in ('2 Dentes','Dt. Leite','4 Dentes','Dt. Leite Nivelado')
	                 then 1
	            else 0
	       end) as flag_habilitacao_china,
	   max(case when value in ('EUA') then 1 else 0 end) as flag_habilitacao_EUA,
	   max(case when value in ('UE') then 1 else 0 end) as flag_habilitacao_UE,
	   max(case when value in ('GF') then 1 else 0 end) as flag_habilitacao_grain_fed,
	   max(case when value in ('NE') then 1 else 0 end) as flag_nao_exportavel,
	   max(case when value in ('OM') then 1 else 0 end) as flag_habilitacao_outros_mercados
FROM main_table as a
CROSS APPLY STRING_SPLIT(a.Habilitacao, '/')
group by Cod_Empresa,
	   Num_Registro_Geral
),
transpose_Maturidade as (
SELECT Cod_Empresa,
	   Num_Registro_Geral,
	   max(case when Maturidade_Etiqueta in ('Dt. Leite', 'Dt. Leite Nivelado') then 1 else 0 end) as flag_dt_leite,
	   max(case when Maturidade_Etiqueta ='2 Dentes' then 1 else 0 end) as flag_2_dentes,
	   max(case when Maturidade_Etiqueta ='4 Dentes' then 1 else 0 end) as flag_4_dentes,
	   max(case when Maturidade_Etiqueta ='6 Dentes' then 1 else 0 end) as flag_6_dentes,
	   max(case when Maturidade_Etiqueta in ('8 Dentes' , '8 Dentes Nivelado', '8+ Dentes' ) then 1 else 0 end) as flag_8_dentes
FROM main_table
group by Cod_Empresa,
	   Num_Registro_Geral
),
transpose_faixa_preco as (
SELECT Cod_Empresa,
	   Num_Registro_Geral,
	   max(case when faixa_valor_arroba ='at  205'then 1 else 0 end) as   flag_val_arroba_205,
	   max(case when faixa_valor_arroba ='206 a 225'then 1 else 0 end) as flag_val_arroba_225,
	   max(case when faixa_valor_arroba ='226 a 250'then 1 else 0 end) as flag_val_arroba_250,
	   max(case when faixa_valor_arroba ='acima 250'then 1 else 0 end) as flag_val_arroba_A250,
	   max(case when faixa_valor_arroba ='vazio' then 1 else 0 end) as    flag_val_arroba_vazio
FROM main_table
group by Cod_Empresa,
	   Num_Registro_Geral
),
transpose_acabamento as (
SELECT Cod_Empresa,
	   Num_Registro_Geral,
	   max(case when Sigla_Acabamento ='Ausente' then 1 else 0 end) as flag_acabemento_ausente,
	   max(case when Sigla_Acabamento ='Escassa' then 1 else 0 end) as flag_acabemento_escassa,
	   max(case when Sigla_Acabamento ='Mediana' then 1 else 0 end) as flag_acabemento_mediana,
	   max(case when Sigla_Acabamento ='Uniforme'then 1 else 0 end) as flag_acabemento_uniforme,
	   max(case when Sigla_Acabamento ='Excessiva'then 1 else 0 end) as flag_acabemento_excessiva,
	   max(case when Sigla_Acabamento in ('MED. FALHA', 'UN C FALHA', 'EXC. FALHA', 'Aus. fall.' )then 1 else 0 end) as flag_acabemento_falha
FROM main_table
group by Cod_Empresa,
	   Num_Registro_Geral
),
transpose_terminacao as (
SELECT Cod_Empresa,
	   Num_Registro_Geral,
	   max(case when Terminacao ='Confinamento a Pasto' then 1 else 0 end) as flag_terminacao_confinamento_pasto,
	   max(case when Terminacao ='Confinamento' then 1 else 0 end) as	      flag_terminacao_confinamento,
	   max(case when Terminacao ='Semi-Confinamento' then 1 else 0 end) as    flag_terminacao_semi_confinamento,
	   max(case when Terminacao ='Pasto'then 1 else 0 end) as                 flag_terminacao_pasto,
	   max(case when Terminacao ='Nao Informado'then 1 else 0 end) as         flag_terminacao_sem_informacao
FROM main_table
group by Cod_Empresa,
	   Num_Registro_Geral
),
combinar_main_transpose as (
select
a.*,
d.flag_acabemento_ausente,
d.flag_acabemento_escassa,
d.flag_acabemento_mediana,
d.flag_acabemento_uniforme,
d.flag_acabemento_excessiva,
d.flag_acabemento_falha,
e.flag_terminacao_confinamento_pasto,
e.flag_terminacao_confinamento,
e.flag_terminacao_semi_confinamento,
e.flag_terminacao_pasto,
e.flag_terminacao_sem_informacao,
j.flag_2_dentes,
j.flag_4_dentes,
j.flag_6_dentes,
j.flag_8_dentes,
j.flag_dt_leite,
b.flag_val_arroba_205,
b.flag_val_arroba_225,
b.flag_val_arroba_250,
b.flag_val_arroba_A250,
b.flag_val_arroba_vazio,
c.flag_habilitacao_chile,
c.flag_habilitacao_china,
c.flag_habilitacao_EUA,
c.flag_habilitacao_UE,
c.flag_habilitacao_grain_fed
from
main_table a
left join transpose_habilitaca c on a.Cod_Empresa = c.Cod_Empresa and a.Num_Registro_Geral = c.Num_Registro_Geral
left join transpose_faixa_preco b on a.Cod_Empresa = b.Cod_Empresa and a.Num_Registro_Geral = b.Num_Registro_Geral
left join transpose_acabamento d on a.Cod_Empresa = d.Cod_Empresa and a.Num_Registro_Geral = d.Num_Registro_Geral
left join transpose_terminacao e on a.Cod_Empresa = e.Cod_Empresa and a.Num_Registro_Geral = e.Num_Registro_Geral
left join transpose_Maturidade j on a.Cod_Empresa = j.Cod_Empresa and a.Num_Registro_Geral = j.Num_Registro_Geral
),
distinct_pecuarista as (
select
Cod_Pecuarista,
min(Data_Abate) as minimo_data_abate,
max(Data_Abate) as max_data_abate
from main_table
group by Cod_Pecuarista
),
grouped_pecuarista as (
select
Cod_Empresa,
a.Cod_Pecuarista,
Cod_Fazenda,
nome_pecuarista,
estado,
ano_abate,
periodo_abate,
max_data_abate,
minimo_data_abate,
dta_cadastramento,
sum(preco_compra_realizado)/sum(arrobas) as preco_medio_compra,
DATEDIFF(month , minimo_data_abate, periodo_abate) as meses_desde_primeira_compra,
count(*) as total_carcacas,
sum(Estancia92) as					soma_habilitacao_estancia,
sum(Organico) as					soma_habilitacao_organico,
sum(Hilton) as						soma_habilitacao_hilton,
sum(flag_habilitacao_chile) as		soma_habilitacao_chile,
sum(flag_habilitacao_china) as		soma_habilitacao_china,
sum(flag_habilitacao_EUA) as		soma_habilitacao_EUA,
sum(flag_habilitacao_UE) as         soma_habilitacao_UE,
sum(flag_habilitacao_grain_fed) as  soma_habilitacao_grain_fed,
cast(avg(qtd_area_propriedade) as float) as media_qtd_area_propriedade,
count(distinct Ordem_Compra) as n_ordem_compra,
cast(avg(Km) as float) as media_distancia,
cast(sum(Kgs) as float) as soma_kgs,
cast(sum(Arrobas) as float) as soma_arrobas,
sum(case when Sexo = 'MACHO' then 1 else 0 end) as soma_macho,
sum(flag_acabemento_ausente ) as soma_acabemento_ausente ,
sum(flag_acabemento_escassa) as soma_acabemento_escassa,
sum(flag_acabemento_mediana) as soma_acabemento_mediana,
sum(flag_acabemento_uniforme) as soma_acabemento_uniforme,
sum(flag_acabemento_excessiva) as soma_acabemento_excessiva,
sum(flag_acabemento_falha) as soma_acabemento_falha,
sum(flag_terminacao_confinamento_pasto) as soma_terminacao_confinamento_pasto,
sum(flag_terminacao_confinamento) as soma_terminacao_confinamento,
sum(flag_terminacao_semi_confinamento) as soma_terminacao_semi_confinamento,
sum(flag_terminacao_pasto) as soma_terminacao_pasto,
sum(flag_terminacao_sem_informacao) as soma_terminacao_sem_informacao,
sum(flag_raca_angus) as soma_raca_angus,
avg(MesInicialMaturidade) as media_mesinicialmat,
avg(MesFinalMaturidade) as   media_mesfinalmat,
sum(flag_2_dentes) as	 soma_2_dentes,
sum(flag_4_dentes) as	 soma_4_dentes,
sum(flag_6_dentes) as	 soma_6_dentes,
sum(flag_8_dentes) as	 soma_8_dentes,
sum(flag_dt_leite) as	 soma_dt_leite,
sum(flag_val_arroba_205 )  as  soma_val_arroba_205,
sum(flag_val_arroba_225)  as   soma_val_arroba_225,
sum(flag_val_arroba_250)  as   soma_val_arroba_250,
sum(flag_val_arroba_A250)  as  soma_val_arroba_A250,
sum(flag_val_arroba_vazio)  as soma_val_arroba_vazio

from combinar_main_transpose a
left join distinct_pecuarista b on a.Cod_Pecuarista = b.Cod_Pecuarista
group by
Cod_Empresa,
a.Cod_Pecuarista,
nome_pecuarista,
Cod_Fazenda,
dta_cadastramento,
estado,
ano_abate,
periodo_abate,
max_data_abate,
minimo_data_abate
),
final_pecurista_table as (
select
a.Cod_Empresa,
a.Cod_Pecuarista,
a.Cod_Fazenda,
a.estado,
a.nome_pecuarista,
a.periodo_abate,
a.meses_desde_primeira_compra,
a.total_carcacas,
a.soma_habilitacao_estancia,
a.soma_habilitacao_organico,
a.soma_habilitacao_hilton,
a.soma_acabemento_ausente ,
a.soma_acabemento_escassa,
a.soma_acabemento_mediana,
a.soma_acabemento_uniforme,
a.soma_acabemento_excessiva,
a.soma_acabemento_falha,
a.soma_terminacao_confinamento_pasto,
a.soma_terminacao_confinamento,
a.soma_terminacao_semi_confinamento,
a.soma_terminacao_pasto,
a.soma_terminacao_sem_informacao,
a.soma_raca_angus,
a.media_qtd_area_propriedade,
a.n_ordem_compra,
a.media_distancia,
a.soma_kgs,
a.soma_arrobas,
a.media_mesinicialmat,
a.media_mesfinalmat,
a.soma_2_dentes,
a.soma_4_dentes,
a.soma_6_dentes,
a.soma_8_dentes,
a.soma_dt_leite,
a.soma_val_arroba_205,
a.soma_val_arroba_225,
a.soma_val_arroba_250,
a.soma_val_arroba_A250,
a.soma_val_arroba_vazio,
a.dta_cadastramento,
count(distinct (case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.periodo_abate  end)) as n_compras_meses_8_m,
count(distinct (case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.n_ordem_compra  end)) as n_ordem_compras_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.total_carcacas else 0 end) as n_carcaca_8_m ,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_kgs else 0 end) as soma_kgs_8_m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_acabemento_ausente else 0 end) as soma_acabemento_ausente_8_m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_acabemento_escassa else 0 end) as soma_acabemento_escassa_8_m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_acabemento_mediana else 0 end) as soma_acabemento_mediana_8_m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_acabemento_uniforme else 0 end) as soma_acabemento_uniforme_8_m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_acabemento_excessiva else 0 end) as soma_acabemento_excessiva_8_m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_acabemento_falha else 0 end) as     soma_acabemento_falha_8_m,

sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_val_arroba_205  else 0 end) as  soma_val_arroba_205_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_val_arroba_225 else 0 end) as  soma_val_arroba_225_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_val_arroba_250 else 0 end) as  soma_val_arroba_250_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_val_arroba_A250  else 0 end) as soma_val_arroba_A250_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_val_arroba_vazio  else 0 end) as soma_val_arroba_vazio_8m,

sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_estancia else 0 end) as   soma_habilitacao_estancia_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_organico else 0 end) as   soma_habilitacao_organico_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_hilton else 0 end) as		soma_habilitacao_hilton_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_chile else 0 end) as		soma_habilitacao_chile_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_china else 0 end) as		soma_habilitacao_china_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_EUA else 0 end) as		soma_habilitacao_EUA_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_UE else 0 end) as		    soma_habilitacao_UE_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_habilitacao_grain_fed else 0 end) as  soma_habilitacao_grain_fed_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_raca_angus else 0 end) as				soma_raca_angus_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_macho else 0 end) as					soma_macho_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_2_dentes else 0 end) as				soma_2_dentes_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_4_dentes else 0 end) as				soma_4_dentes_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_6_dentes else 0 end) as				soma_6_dentes_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_8_dentes else 0 end) as				soma_8_dentes_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_dt_leite else 0 end) as				soma_dt_leite_8m,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_terminacao_confinamento_pasto else 0 end) as soma_terminacao_confinamento_pasto_8m ,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_terminacao_confinamento else 0 end) as	   soma_terminacao_confinamento_8m ,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_terminacao_semi_confinamento else 0 end) as  soma_terminacao_semi_confinamento_8m ,
sum(case when DATEDIFF(month , b.periodo_abate, a.periodo_abate) <= 8 then b.soma_terminacao_pasto else 0 end) as			   soma_terminacao_pasto_8m

/*FAZER HISTORICO DE TEMPO DE DENTI  O*/
from grouped_pecuarista a
left join grouped_pecuarista b
on a.Cod_Empresa=b.Cod_Empresa and a.Cod_Pecuarista = b.Cod_Pecuarista and a.Cod_Fazenda =b.Cod_Fazenda and a.periodo_abate > b.periodo_abate
group by
a.Cod_Empresa,
a.Cod_Pecuarista,
a.Cod_Fazenda,
a.nome_pecuarista,
a.estado,
a.periodo_abate,
a.meses_desde_primeira_compra,
a.total_carcacas,
a.soma_habilitacao_estancia,
a.soma_habilitacao_organico,
a.soma_habilitacao_hilton,
a.soma_acabemento_ausente ,
a.soma_acabemento_escassa,
a.soma_acabemento_mediana,
a.soma_acabemento_uniforme,
a.soma_acabemento_excessiva,
a.soma_acabemento_falha,
a.soma_terminacao_confinamento_pasto,
a.soma_terminacao_confinamento,
a.soma_terminacao_semi_confinamento,
a.soma_terminacao_pasto,
a.soma_terminacao_sem_informacao,
a.soma_raca_angus,
a.media_qtd_area_propriedade,
a.n_ordem_compra,
a.dta_cadastramento,
a.media_distancia,
a.soma_kgs,
a.soma_arrobas,
a.media_mesinicialmat,
a.media_mesfinalmat,
a.soma_2_dentes,
a.soma_4_dentes,
a.soma_6_dentes,
a.soma_8_dentes,
a.soma_dt_leite,
a.soma_val_arroba_205,
a.soma_val_arroba_225,
a.soma_val_arroba_250,
a.soma_val_arroba_A250,
a.soma_val_arroba_vazio
)
select
*,


case when total_carcacas >0 then   cast(soma_acabemento_escassa as float)/cast(total_carcacas as float) else 0 end as prop_soma_acabemento_escassa,
case when n_carcaca_8_m >0 then    cast(soma_acabemento_escassa_8_m as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_acabemento_escassa_8_m,


case when total_carcacas >0 then   cast(soma_acabemento_mediana as float)/cast(total_carcacas as float) else 0 end as prop_soma_acabemento_mediana,
case when n_carcaca_8_m >0 then    cast(soma_acabemento_mediana_8_m as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_acabemento_mediana_8_m,


case when total_carcacas >0 then   cast(soma_acabemento_uniforme as float)/cast(total_carcacas as float) else 0 end as prop_soma_acabemento_uniforme,
case when n_carcaca_8_m >0 then    cast(soma_acabemento_uniforme_8_m as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_acabemento_uniforme_8_m,

case when total_carcacas >0 then   cast(soma_acabemento_falha as float)/cast(total_carcacas as float) else 0 end as prop_soma_acabemento_falha,
case when n_carcaca_8_m >0 then    cast(soma_acabemento_falha_8_m as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_acabemento_falha_8_m,

case when total_carcacas >0 then   cast(soma_acabemento_excessiva as float)/cast(total_carcacas as float) else 0 end as prop_soma_acabemento_excessiva,
case when n_carcaca_8_m >0 then    cast(soma_acabemento_excessiva_8_m as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_acabemento_excessiva_8_m,

case when total_carcacas >0 then   cast(soma_kgs as float)/(cast(total_carcacas as float)/2) else 0 end as media_kgs_cabeca,
case when n_carcaca_8_m >0 then    cast(soma_kgs_8_m as float)/(cast(n_carcaca_8_m as float)/2) else 0 end as media_kgs_cabeca_8_m,

case when n_carcaca_8_m >0 then    cast(soma_val_arroba_205  as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_val_arroba_205_8m,
case when n_carcaca_8_m >0 then    cast(soma_val_arroba_225 as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_val_arroba_225_8m,
case when n_carcaca_8_m >0 then    cast(soma_val_arroba_250 as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_val_arroba_250_8m,
case when n_carcaca_8_m >0 then    cast(soma_val_arroba_A250 as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_val_arroba_A250_8m,
case when n_carcaca_8_m >0 then    cast(soma_val_arroba_vazio as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_val_arroba_vazio_8m,



case when total_carcacas >0 then cast(soma_raca_angus as float)/cast(total_carcacas as float) else 0 end as prop_soma_raca_angus,

case when total_carcacas >0 then cast(soma_terminacao_confinamento_pasto as float)/cast(total_carcacas as float) else 0 end as prop_confinamento_pasto,
case when total_carcacas >0 then cast(soma_terminacao_confinamento as float)/cast(total_carcacas as float) else 0 end as prop_confinamento,
case when total_carcacas >0 then cast(soma_terminacao_semi_confinamento as float)/cast(total_carcacas as float) else 0 end as prop_semi_confinamento,
case when total_carcacas >0 then cast(soma_terminacao_pasto as float)/cast(total_carcacas as float) else 0 end as prop_pasto,

case when n_ordem_compra >0 then   cast(total_carcacas as float)/cast(n_ordem_compra as float) else 0 end as media_n_carcaca_compra,
case when n_compras_meses_8_m >0 then    (cast(n_carcaca_8_m as float)/2)/cast(n_compras_meses_8_m  as float) else 0 end as media_n_cabeca_compra_8m,

case when n_carcaca_8_m >0 then    cast(soma_habilitacao_estancia_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as				prop_habilitacao_estancia_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_organico_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as				prop_habilitacao_organico_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_hilton_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as				prop_habilitacao_hilton_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_chile_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as					prop_habilitacao_chile_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_china_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as					prop_habilitacao_china_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_EUA_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as					prop_habilitacao_EUA_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_UE_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as					prop_habilitacao_UE_8m,
case when n_carcaca_8_m >0 then    cast(soma_habilitacao_grain_fed_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as				prop_habilitacao_grain_fed_8m,
case when n_carcaca_8_m >0 then    cast(soma_raca_angus_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as						prop_raca_angus_8m,
case when n_carcaca_8_m >0 then    cast(soma_macho_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as								prop_macho_8m,
case when n_carcaca_8_m >0 then    cast(soma_2_dentes_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 							prop_2_dentes_8m,
case when n_carcaca_8_m >0 then    cast(soma_4_dentes_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 							prop_4_dentes_8m,
case when n_carcaca_8_m >0 then    cast(soma_6_dentes_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 							prop_6_dentes_8m,
case when n_carcaca_8_m >0 then    cast(soma_8_dentes_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 							prop_8_dentes_8m,
case when n_carcaca_8_m >0 then    cast(soma_dt_leite_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 							prop_dt_leite_8m,
case when n_carcaca_8_m >0 then    cast(soma_terminacao_confinamento_pasto_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 	prop_terminacao_confinamento_pasto_8m ,
case when n_carcaca_8_m >0 then    cast(soma_terminacao_confinamento_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 			prop_terminacao_confinamento_8m ,
case when n_carcaca_8_m >0 then    cast(soma_terminacao_semi_confinamento_8m  as float)/cast(n_carcaca_8_m as float) else 0 end as 		prop_terminacao_semi_confinamento_8m ,
case when n_carcaca_8_m >0 then    cast(soma_terminacao_pasto_8m   as float)/cast(n_carcaca_8_m as float) else 0 end as					prop_terminacao_pasto_8m


into #BASE_UNICA_ABATE
from final_pecurista_table
;


with tab_maior_acabamento as (
SELECT
Cod_Empresa,
cod_pecuarista,
cod_fazenda,
periodo_abate,
MAX(x.CombinedDate) AS maior_acabamento
FROM (select*,
		case when n_carcaca_8_m >0 then    cast(soma_acabemento_ausente_8_m as float)/cast(n_carcaca_8_m as float) else 0 end as prop_soma_acabemento_ausente_8_m
		 from  #BASE_UNICA_ABATE) AS u
CROSS APPLY ( VALUES ( u.prop_soma_acabemento_ausente_8_m ),
					 ( u.prop_soma_acabemento_escassa_8_m ),
					 ( u.prop_soma_acabemento_mediana_8_m ),
					 ( u.prop_soma_acabemento_uniforme_8_m ),
					 ( u.prop_soma_acabemento_excessiva_8_m ),
					 ( u.prop_soma_acabemento_falha_8_m )
					) AS x ( CombinedDate )
group by
Cod_Empresa,
cod_pecuarista,
cod_fazenda,
periodo_abate
),
tab_maior_preco as (SELECT
Cod_Empresa,
cod_pecuarista,
cod_fazenda,
periodo_abate,
MAX(x.CombinedDate) AS maior_preco
FROM (select*
		 from  #BASE_UNICA_ABATE) AS u
CROSS APPLY ( VALUES ( u.prop_soma_val_arroba_205_8m ),
					 ( u.prop_soma_val_arroba_225_8m ),
					 ( u.prop_soma_val_arroba_250_8m ),
					 ( u.prop_soma_val_arroba_A250_8m ),
					 ( u.prop_soma_val_arroba_vazio_8m )
					) AS x ( CombinedDate )
group by
Cod_Empresa,
cod_pecuarista,
cod_fazenda,
periodo_abate)


SELECT
    a.periodo_abate,
    a.Cod_Empresa,
    a.cod_pecuarista,
    a.nome_pecuarista,
    tc.Cod_Comprador,
    tc.Nome_Comprador,
    e.nom_logradouro,
    a.cod_fazenda,
    CASE WHEN n_carcaca_8_m > 0 THEN a.n_carcaca_8_m / 2 ELSE 0 END AS total_gado_8m,
    CASE WHEN n_carcaca_8_m > 0 THEN soma_kgs_8_m / (n_carcaca_8_m / 2) ELSE 0 END AS peso_por_kg_8m,
    CASE WHEN total_carcacas > 0 THEN a.total_carcacas / 2 ELSE 0 END AS total_gado,
    CASE WHEN total_carcacas > 0 THEN soma_kgs / (total_carcacas / 2) ELSE 0 END AS peso_por_kg,
    CASE WHEN maior_acabamento = 0 THEN '0_sem_info'
         WHEN (CASE WHEN n_carcaca_8_m > 0 THEN cast(soma_acabemento_ausente_8_m AS float) / cast(n_carcaca_8_m AS float) ELSE 0 end) = maior_acabamento THEN '1_AUSENTE'
         WHEN prop_soma_acabemento_escassa_8_m = maior_acabamento THEN '2_ESCASSA'
         WHEN prop_soma_acabemento_mediana_8_m = maior_acabamento THEN '4_MEDIANA'
         WHEN prop_soma_acabemento_uniforme_8_m = maior_acabamento THEN '3_UNIFORME'
         WHEN prop_soma_acabemento_excessiva_8_m = maior_acabamento THEN '4_EXCESSIVA'
         WHEN prop_soma_acabemento_falha_8_m = maior_acabamento THEN '2_FALHA'
         ELSE 'ERRO'
    END AS Maior_classifica_acabamento,
    CASE WHEN maior_preco = 0 THEN '0_Sem_info'
         WHEN prop_soma_val_arroba_205_8m = maior_preco THEN '4_at  205'
         WHEN prop_soma_val_arroba_225_8m = maior_preco THEN '3_206 a 225'
         WHEN prop_soma_val_arroba_250_8m = maior_preco THEN '2_226 a 250'
         WHEN prop_soma_val_arroba_A250_8m = maior_preco THEN '1_Acima 250'
         WHEN prop_soma_val_arroba_vazio_8m = maior_preco THEN '0_vazio'
         ELSE 'ERRO'
    END AS faixa_preco_compra,
    CASE WHEN n_carcaca_8_m / 2 = 0 THEN '0_SEm comercializaco'
         WHEN n_carcaca_8_m / 2 <= 35 THEN '1_At  35'
         WHEN n_carcaca_8_m / 2 <= 55 THEN '2_36 a 55'
         WHEN n_carcaca_8_m / 2 <= 125 THEN '3_56 a 125'
         WHEN n_carcaca_8_m / 2 <= 325 THEN '4_126 a 325'
         ELSE '5_acima de 326'
    END AS faixa_qtd_cabeca,
    CASE WHEN n_carcaca_8_m = 0 THEN '0_SEm comercializaco'
         WHEN soma_kgs_8_m / (n_carcaca_8_m / 2) <= 225 THEN '1_At  225'
         WHEN soma_kgs_8_m / (n_carcaca_8_m / 2) <= 260 THEN '2_226 a 260'
         WHEN soma_kgs_8_m / (n_carcaca_8_m / 2) <= 285 THEN '3_261 a 285'
         WHEN soma_kgs_8_m / (n_carcaca_8_m / 2) <= 305 THEN '4_286 a 305'
         ELSE '5_acima de 305'
    END AS faixa_peso_cabeca,
    CASE WHEN prop_habilitacao_estancia_8m > 0.25 THEN 1 ELSE 0 END AS flag_estancia,
    CASE WHEN prop_habilitacao_organico_8m > 0.25 THEN 1 ELSE 0 END AS flag_organico,
    CASE WHEN prop_habilitacao_hilton_8m > 0.15 THEN 1 ELSE 0 END AS flag_hilton,
    CASE WHEN prop_habilitacao_chile_8m > 0.25 THEN 1 ELSE 0 END AS flag_chile,
    CASE WHEN prop_habilitacao_china_8m > 0.25 THEN 1 ELSE 0 END AS flag_china,
    CASE WHEN prop_habilitacao_EUA_8m > 0.25 THEN 1 ELSE 0 END AS flag_eua,
    CASE WHEN prop_habilitacao_UE_8m > 0.25 THEN 1 ELSE 0 END AS flag_ue,
    CASE WHEN prop_habilitacao_grain_fed_8m > 0.15 THEN 1 ELSE 0 END AS flag_grain_fed,
    CASE WHEN prop_raca_angus_8m > 0.1 THEN 1 ELSE 0 END AS flag_angus,
    prop_habilitacao_estancia_8m,
    prop_habilitacao_organico_8m,
    prop_habilitacao_hilton_8m,
    prop_habilitacao_chile_8m,
    prop_habilitacao_china_8m,
    prop_habilitacao_EUA_8m,
    prop_habilitacao_UE_8m,
    prop_habilitacao_grain_fed_8m,
    prop_raca_angus_8m,
    CASE WHEN prop_macho_8m > 0.5 THEN 1 ELSE 0 END AS flag_macho,
    CASE WHEN (prop_2_dentes_8m + prop_dt_leite_8m) > 0.5 THEN 1 ELSE 0 END AS flag_2_dente_dt_leite,
    CASE WHEN prop_4_dentes_8m > 0.5 THEN 1 ELSE 0 END AS flag_4_dente,
    CASE WHEN (prop_6_dentes_8m + prop_8_dentes_8m) > 0.5 THEN 1 ELSE 0 END AS flag_6_8_dente,
    CASE WHEN prop_terminacao_confinamento_pasto_8m > 0.5 THEN 1 ELSE 0 END AS flag_confinamento_pasto,
    CASE WHEN prop_terminacao_confinamento_8m > 0.5 THEN 1 ELSE 0 END AS flag_confinamento,
    CASE WHEN prop_terminacao_semi_confinamento_8m > 0.5 THEN 1 ELSE 0 END AS flag_semi_confinamento,
    CASE WHEN prop_terminacao_pasto_8m > 0.5 THEN 1 ELSE 0 END AS flag_pasto,
    prop_macho_8m,
    prop_2_dentes_8m,
    prop_4_dentes_8m,
    prop_6_dentes_8m,
    prop_8_dentes_8m,
    prop_dt_leite_8m,
    prop_terminacao_confinamento_pasto_8m,
    prop_terminacao_confinamento_8m,
    prop_terminacao_semi_confinamento_8m,
    prop_terminacao_pasto_8m,
    media_distancia AS distancia,
    a.dta_cadastramento AS data_cadastro
FROM #BASE_UNICA_ABATE a
LEFT JOIN tab_maior_acabamento b ON a.Cod_Empresa = b.Cod_Empresa AND a.Cod_Pecuarista = b.cod_pecuarista AND a.Cod_Fazenda = b.cod_fazenda AND a.periodo_abate = b.periodo_abate
LEFT JOIN tab_maior_preco c ON a.Cod_Empresa = c.Cod_Empresa AND a.Cod_Pecuarista = c.cod_pecuarista AND a.Cod_Fazenda = c.cod_fazenda AND a.periodo_abate = c.periodo_abate
LEFT JOIN #TAB_COMPRADOR_PECUARISTA tc ON a.Cod_Empresa = tc.Cod_Empresa AND a.Cod_Pecuarista = tc.Cod_Pecuarista AND a.Cod_Fazenda = tc.Cod_Fazenda AND a.periodo_abate = tc.periodo_abate
LEFT JOIN IKOL.dbpessoa.dbo.tab_logradouro e WITH (NOLOCK) ON e.cod_pessoa = a.Cod_Pecuarista AND e.seq_logradouro = a.Cod_Fazenda
WHERE a.periodo_abate > '2022-12-31'
ORDER BY a.periodo_abate;
--where a.periodo_abate >'2024-01-31'


/*
SELECT
    a.periodo_abate,
    a.Cod_Empresa,
    a.cod_pecuarista,
    a.nome_pecuarista,
    tc.Cod_Comprador,
    tc.Nome_Comprador
from #BASE_UNICA_ABATE a
LEFT JOIN #TAB_COMPRADOR_PECUARISTA tc ON a.Cod_Pecuarista = tc.Cod_Pecuarista AND a.periodo_abate = tc.periodo_abate
WHERE a.periodo_abate > '2025-04-01'
*/


--select top 10 * from #BASE_UNICA_ABATE



--select  * from ikol.dbintranet.dbo.tab_reuniao_custo_gado_ofertas_nao_compradas where upper(pecuarista) like '%SANDOVAL CARDOSO%'
