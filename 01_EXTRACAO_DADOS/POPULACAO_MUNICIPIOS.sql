SELECT
    ibge.nu_Populacao2010 AS populacao_municipio,
	CONCAT(dt_Ano, '-', cd_IBGE) AS ano_codigo_municipio
FROM SAGRES_MUNICIPAL.dbo.Censo_IBGE AS ibge
ORDER BY ibge.dt_Ano;