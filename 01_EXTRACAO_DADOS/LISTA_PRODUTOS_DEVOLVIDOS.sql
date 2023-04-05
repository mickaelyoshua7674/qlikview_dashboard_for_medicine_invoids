SELECT
	DISTINCT CONCAT(chave_referenciada.chave_nf, '-', produtos.codigo) AS chave_codigo
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que � o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer liga��o com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave -- Juntando com tabela DIM_CHAVE para pegar a chave da nota fiscal
	ON produtos.id_chave = chave.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_REFERENCIA AS referencia -- Rela��o para pegar refer�ncia
	ON nfe.id_chave = referencia.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE_REFERENCIADA AS chave_referenciada -- Rela��o para pegar a chave da NF que est� sendo referenciada
	ON referencia.id_chave_referenciada = chave_referenciada.id_chave_referenciada
--------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE
--FILTRO NOTA FISCAL
	nfe.nota_cancelada = 0 AND -- Notas que n�o foram canceladas
	nfe.id_modelo = 2 AND -- Notas 55
	nfe.id_finalidade = 4 AND -- Notas que possuem valor 4 (DEVOLUCAO DE MERCADORIA)
	chave_referenciada.chave_nf IS NOT NULL;