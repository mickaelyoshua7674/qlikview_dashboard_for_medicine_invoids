SELECT
	DISTINCT chave.chave
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave -- Relação com DIM_CHAVE para pegar a chave da NF
	ON nfe.id_chave = chave.id_chave
WHERE nfe.nota_cancelada = 1;