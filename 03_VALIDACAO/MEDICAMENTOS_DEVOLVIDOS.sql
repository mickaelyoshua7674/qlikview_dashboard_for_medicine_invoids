DROP TABLE IF EXISTS #CHAVES_DISTINTAS_DEVOLVIDAS;
SELECT
	DISTINCT chave.chave, -- Para eliminar notas fiscais carregadas duas vezes
	FIRST_VALUE(chave.id_chave) OVER( -- inserindo primeiro id para ser a primeira nota carregada e fazer rela��o com as outras tabelas
		PARTITION BY chave.chave
		ORDER BY chave.chave
	) AS id_chave
	INTO #CHAVES_DISTINTAS_DEVOLVIDAS
FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer liga��o com a tabela DIM_DESTINATARIO_EMITENTE
	ON chave.id_chave = nfe.id_chave
WHERE
	nfe.nota_cancelada = 0 AND -- Notas que n�o foram canceladas
	nfe.id_modelo = 2 AND -- Notas 55
	nfe.id_finalidade = 4; -- Notas que possuem valor 4 (DEVOLUCAO DE MERCADORIA)

DROP TABLE IF EXISTS #MEDICAMENTOS_DADOS_DEVOLVIDOS;
SELECT
	DISTINCT CONCAT(chave_referenciada.chave_nf, '-', produtos.codigo) AS chave_codigo,
	chaves_distintas.chave AS chave_devolucao,
	FIRST_VALUE(produtos.id_produto) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS id_produto_devolucao,
	FIRST_VALUE(produtos.descricao) OVER( -- Pegando valor da descri��o e botando como uma coluna
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo) -- Particionando com a mesma chave em DISTINCT acima para que os valores estejam corretos e relacionados
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo) -- Ordenando pela mesma chave
	) AS descricao_devolucao,
	FIRST_VALUE(produtos.unidade) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS unidade_devolucao,
	FIRST_VALUE(nfe.id_emitente) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS id_emitente_devolucao,
	FIRST_VALUE(nfe.id_destinatario) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS id_destinatario_devolucao,
	FIRST_VALUE(endereco.codigo_municipio) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS codigo_municipio_destinatario_devolucao,
	FIRST_VALUE(id_data_emissao.data) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS data_emissao_devolucao,
	FIRST_VALUE(id_data_fabricacao.data) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS data_fabricacao_devolucao,
	FIRST_VALUE(id_data_validade.data) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS data_validade_devolucao,
	FIRST_VALUE(produtos.lote) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS lote_devolucao,
	FIRST_VALUE(produtos.quantidade_lote) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS quantidade_lote_devolucao,
	0 AS nota_cancelada_devolucao
	INTO #MEDICAMENTOS_DADOS_DEVOLVIDOS
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que � o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer liga��o com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (SELECT id_chave, chave FROM #CHAVES_DISTINTAS_DEVOLVIDAS) AS chaves_distintas
	ON nfe.id_chave = chaves_distintas.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente -- Rela��o das notas fiscais com os dados de seus emitentes
	ON nfe.id_emitente = emitente.id_destinatario_emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS destinatario -- Rela��o das notas fiscais com os dados de seus destinatarios
	ON nfe.id_destinatario = destinatario.id_destinatario_emitente
LEFT JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_ENDERECO as endereco -- Rela��o com DIM_ENDERECO para pegar o municipio de cada destinatario
	ON destinatario.id_endereco = endereco.id_endereco
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Rela��o com tabela DIM_CPF_CNPJ para pegar o cnpj
	ON emitente.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Rela��o com banco de dados da Rceita Federal atrav�s do NPJ para filtras os c�digos CNAE das empresas
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_REFERENCIA AS referencia -- Rela��o para pegar refer�ncia
	ON nfe.id_chave = referencia.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE_REFERENCIADA AS chave_referenciada -- Rela��o para pegar a chave da NF que est� sendo referenciada
	ON referencia.id_chave_referenciada = chave_referenciada.id_chave_referenciada
--------------------------------------------------------------------------------------------------------------------------------------------------------------
-- JOINs com tabela DIM_DATA para pegar datas de emiss�o de nota, fabrica��o e validade de produtos
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DATA as id_data_emissao
	ON nfe.id_data_emissao = id_data_emissao.id_data
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DATA as id_data_fabricacao
	ON produtos.id_data_fabricacao = id_data_fabricacao.id_data
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DATA as id_data_validade
	ON produtos.id_data_validade = id_data_validade.id_data
--------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE
--FILTRO PRODUTOS
	(
		-- C�digo para bebidas, l�quidos alco�licos e vinagres (Cap�tulo 22)
		produtos.codigo_ncm LIKE '22071%' OR
		produtos.codigo_ncm LIKE '22072%' OR
		-- C�digos NCM para produtos farmac�uticos (Cap�tulo 30)
		produtos.codigo_ncm LIKE '30%' OR
		-- C�digo Extratos tanantes e tintoriais (Cap�tulo 32)
		produtos.codigo_ncm LIKE '320416%' OR -- corantes reagentes e prepara��es � base desses corantes
		-- C�digo para produtos para fotografia e cinematografia (Cap�tulo 37)
		produtos.codigo_ncm LIKE '370110%' OR -- filme para raio x
		-- C�digo produtos qu�micos diversos (Cap�tulo 38)
		produtos.codigo_ncm LIKE '3821%' OR -- Meios de cultura preparados para o desenvolvimento e a manuten��o de microrganismos 
		produtos.codigo_ncm LIKE '3822%' OR -- reagentes de diagn�stico ou de laborat�rio (e.g. analisador de PH, glicose liquida, f�sforo UV, hemoglobina glicada, acido urico)
		-- C�digo NCM para produtos de borracha (Cap�tulo 40)
		produtos.codigo_ncm LIKE '40151%' OR
		-- C�digo para tecidos especiais, tecidos tufados, rendas, tape�arias, passamanarias, bordados (Cap�tulo 58)
		produtos.codigo_ncm LIKE '5803%' OR -- Tecidos em ponto de gaze
		-- C�digo para Outros artefatos t�xteis confeccionados (Cap�tulo 60)
		produtos.codigo_ncm LIKE '6307901%' OR -- Outros artefatos confeccionados de falso tecido (e.g. m�scara)
		-- C�digo Reatores nucleares, caldeiras, m�quinas, aparelhos e instrumentos mec�nicos, e suas partes (Cap�tulo 84)
		produtos.codigo_ncm LIKE '842119%' OR -- Centrifugadores para laborat�rios de an�lises, ensaios ou pesquisas cient�ficas
		produtos.codigo_ncm LIKE '842191%' OR -- Partes de centrifugadores
		produtos.codigo_ncm LIKE '84798912' OR -- Distribuidores e doseadores de s�lidos ou de l�quidos  (e.g. micropipeta)
		-- C�digos NCM para instrumentos e aparelhos de �ptica, de fotografia, de cinematografia, de medida, de controle ou de precis�o.
		-- instrumentos e aparelhos m�dico-cir�rgicos suas partes e acess�rios (Cap�tulo 90)
		produtos.codigo_ncm LIKE '9011%' OR --  Microsc�pios �pticos, incluindo os microsc�pios para fotomicrografia, cinefotomicrografia ou microproje��o.
		produtos.codigo_ncm LIKE '9012%' OR --  Microsc�pios, exceto �pticos, difrat�grafos.
		produtos.codigo_ncm LIKE '9018%' OR -- instumentos e aparelhos para medicina, cirurgia, odontologia e veterin�ria
		produtos.codigo_ncm LIKE '9019%' OR -- aparelhos de mecanoterapia, aparelhos de massagem, de psicot�cnica, de ozonoterapia...
		produtos.codigo_ncm LIKE '9020%' OR -- outros aparelhos respirat�rios e m�scaras contra gases
		produtos.codigo_ncm LIKE '9021%' OR -- Artigos e aparelhos ortop�dicos
		produtos.codigo_ncm LIKE '9022%' OR -- Aparelhos de raios X e aparelhos que utilizem radia��es alfa
		produtos.codigo_ncm LIKE '9027%' OR -- Instrumentos e aparelhos para an�lises f�sicas ou qu�micas
		-- C�digos para Obras diversas (Cap�tulo 96)
		produtos.codigo_ncm LIKE '9619%' OR --  Absorventes e tamp�es higi�nicos, cueiros e fraldas para beb�s e artigos higi�nicos semelhantes, de qualquer mat�ria.
			-- FONTE: http://www5.sefaz.mt.gov.br/documents/6071037/6401784/Tabela+NCM+-+MDIC+atualizada.pdf/bc780e4b-fd2f-4312-879c-65d5fd1ff49d / P�g. 129
		produtos.id_medicamento != -1 -- possui id de medicamento v�lido
	) AND
--------------------------------------------------------------------------------------------------------------------------------------------------------------
--FILTRO NOTA FISCAL
	chave_referenciada.chave_nf IS NOT NULL AND
--------------------------------------------------------------------------------------------------------------------------------------------------------------
--FILTRO PESSOA JURIDICA
	pessoa_juridica.cnae_fiscal IN (2110600, 2121101, 2121102, 2121103, 2123800, 2312500, 3299099, 4618401,
									4644301, 4645101, 4664800, 4684299, 4771701, 4771702, 4771703, 4773300); -- Empresas farmoquimicas e farmaceuticas
-- Fonte: https://www.contabilizei.com.br/contabilidade-online/cnae/ - https://concla.ibge.gov.br/busca-online-cnae.html
--------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #MEDIDAMENTOS_VALORES_DEVOLVIDOS;
SELECT
	CONCAT(chave_referenciada.chave_nf, '-', produtos.codigo) AS chave_codigo,
	chaves_distintas.chave AS chave_devolucao,
	SUM(produtos.valor_produtos) AS valor_produtos_devolucao,
	SUM(produtos.quantidade) AS quantidade_devolucao
	INTO #MEDIDAMENTOS_VALORES_DEVOLVIDOS
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que � o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer liga��o com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (SELECT id_chave, chave FROM #CHAVES_DISTINTAS_DEVOLVIDAS) AS chaves_distintas
	ON nfe.id_chave = chaves_distintas.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente_destinatario -- Rela��o das notas fiscais com os dados de seus emitentes
	ON nfe.id_emitente = emitente_destinatario.id_destinatario_emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Rela��o com tabela DIM_CPF_CNPJ para pegar o cnpj
	ON emitente_destinatario.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Rela��o com banco de dados da Rceita Federal atrav�s do NPJ para filtras os c�digos CNAE das empresas
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_REFERENCIA AS referencia -- Rela��o para pegar refer�ncia
	ON nfe.id_chave = referencia.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE_REFERENCIADA AS chave_referenciada -- Rela��o para pegar a chave da NF que est� sendo referenciada
	ON referencia.id_chave_referenciada = chave_referenciada.id_chave_referenciada
--------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE
--FILTRO PRODUTOS
	(
		-- C�digo para bebidas, l�quidos alco�licos e vinagres (Cap�tulo 22)
		produtos.codigo_ncm LIKE '22071%' OR
		produtos.codigo_ncm LIKE '22072%' OR
		-- C�digos NCM para produtos farmac�uticos (Cap�tulo 30)
		produtos.codigo_ncm LIKE '30%' OR
		-- C�digo Extratos tanantes e tintoriais (Cap�tulo 32)
		produtos.codigo_ncm LIKE '320416%' OR -- corantes reagentes e prepara��es � base desses corantes
		-- C�digo para produtos para fotografia e cinematografia (Cap�tulo 37)
		produtos.codigo_ncm LIKE '370110%' OR -- filme para raio x
		-- C�digo produtos qu�micos diversos (Cap�tulo 38)
		produtos.codigo_ncm LIKE '3821%' OR -- Meios de cultura preparados para o desenvolvimento e a manuten��o de microrganismos 
		produtos.codigo_ncm LIKE '3822%' OR -- reagentes de diagn�stico ou de laborat�rio (e.g. analisador de PH, glicose liquida, f�sforo UV, hemoglobina glicada, acido urico)
		-- C�digo NCM para produtos de borracha (Cap�tulo 40)
		produtos.codigo_ncm LIKE '40151%' OR
		-- C�digo para tecidos especiais, tecidos tufados, rendas, tape�arias, passamanarias, bordados (Cap�tulo 58)
		produtos.codigo_ncm LIKE '5803%' OR -- Tecidos em ponto de gaze
		-- C�digo para Outros artefatos t�xteis confeccionados (Cap�tulo 60)
		produtos.codigo_ncm LIKE '6307901%' OR -- Outros artefatos confeccionados de falso tecido (e.g. m�scara)
		-- C�digo Reatores nucleares, caldeiras, m�quinas, aparelhos e instrumentos mec�nicos, e suas partes (Cap�tulo 84)
		produtos.codigo_ncm LIKE '842119%' OR -- Centrifugadores para laborat�rios de an�lises, ensaios ou pesquisas cient�ficas
		produtos.codigo_ncm LIKE '842191%' OR -- Partes de centrifugadores
		produtos.codigo_ncm LIKE '84798912' OR -- Distribuidores e doseadores de s�lidos ou de l�quidos  (e.g. micropipeta)
		-- C�digos NCM para instrumentos e aparelhos de �ptica, de fotografia, de cinematografia, de medida, de controle ou de precis�o.
		-- instrumentos e aparelhos m�dico-cir�rgicos suas partes e acess�rios (Cap�tulo 90)
		produtos.codigo_ncm LIKE '9011%' OR --  Microsc�pios �pticos, incluindo os microsc�pios para fotomicrografia, cinefotomicrografia ou microproje��o.
		produtos.codigo_ncm LIKE '9012%' OR --  Microsc�pios, exceto �pticos, difrat�grafos.
		produtos.codigo_ncm LIKE '9018%' OR -- instumentos e aparelhos para medicina, cirurgia, odontologia e veterin�ria
		produtos.codigo_ncm LIKE '9019%' OR -- aparelhos de mecanoterapia, aparelhos de massagem, de psicot�cnica, de ozonoterapia...
		produtos.codigo_ncm LIKE '9020%' OR -- outros aparelhos respirat�rios e m�scaras contra gases
		produtos.codigo_ncm LIKE '9021%' OR -- Artigos e aparelhos ortop�dicos
		produtos.codigo_ncm LIKE '9022%' OR -- Aparelhos de raios X e aparelhos que utilizem radia��es alfa
		produtos.codigo_ncm LIKE '9027%' OR -- Instrumentos e aparelhos para an�lises f�sicas ou qu�micas
		-- C�digos para Obras diversas (Cap�tulo 96)
		produtos.codigo_ncm LIKE '9619%' OR --  Absorventes e tamp�es higi�nicos, cueiros e fraldas para beb�s e artigos higi�nicos semelhantes, de qualquer mat�ria.
			-- FONTE: http://www5.sefaz.mt.gov.br/documents/6071037/6401784/Tabela+NCM+-+MDIC+atualizada.pdf/bc780e4b-fd2f-4312-879c-65d5fd1ff49d / P�g. 129
		produtos.id_medicamento != -1 -- possui id de medicamento v�lido
	) AND
--------------------------------------------------------------------------------------------------------------------------------------------------------------
--FILTRO NOTA FISCAL
	chave_referenciada.chave_nf IS NOT NULL AND
--------------------------------------------------------------------------------------------------------------------------------------------------------------
--FILTRO PESSOA JURIDICA
	pessoa_juridica.cnae_fiscal IN (2110600, 2121101, 2121102, 2121103, 2123800, 2312500, 3299099, 4618401,
									4644301, 4645101, 4664800, 4684299, 4771701, 4771702, 4771703, 4773300) -- Empresas farmoquimicas e farmaceuticas
-- Fonte: https://www.contabilizei.com.br/contabilidade-online/cnae/ - https://concla.ibge.gov.br/busca-online-cnae.html
--------------------------------------------------------------------------------------------------------------------------------------------------------------
GROUP BY CONCAT(chave_referenciada.chave_nf, '-', produtos.codigo), chaves_distintas.chave;

DROP TABLE IF EXISTS #MEDICAMENTOS_DEVOLVIDOS;
SELECT
	medicamentos_dados_devolvidos.chave_codigo,
	medicamentos_dados_devolvidos.chave_devolucao,
	medicamentos_dados_devolvidos.id_produto_devolucao,
	medicamentos_dados_devolvidos.descricao_devolucao,
	medicamentos_dados_devolvidos.unidade_devolucao,
	medicamentos_dados_devolvidos.id_emitente_devolucao,
	medicamentos_dados_devolvidos.id_destinatario_devolucao,
	medicamentos_dados_devolvidos.codigo_municipio_destinatario_devolucao,
	medicamentos_dados_devolvidos.data_emissao_devolucao,
	medicamentos_dados_devolvidos.data_fabricacao_devolucao,
	medicamentos_dados_devolvidos.data_validade_devolucao,
	medicamentos_dados_devolvidos.lote_devolucao,
	medicamentos_dados_devolvidos.quantidade_lote_devolucao,
	medicamentos_dados_devolvidos.nota_cancelada_devolucao,
	medicamentos_valores_devolvidos.valor_produtos_devolucao,
	medicamentos_valores_devolvidos.quantidade_devolucao
	INTO #MEDICAMENTOS_DEVOLVIDOS
FROM #MEDICAMENTOS_DADOS_DEVOLVIDOS AS medicamentos_dados_devolvidos
INNER JOIN #MEDIDAMENTOS_VALORES_DEVOLVIDOS AS medicamentos_valores_devolvidos
	ON medicamentos_dados_devolvidos.chave_codigo = medicamentos_valores_devolvidos.chave_codigo
	AND medicamentos_dados_devolvidos.chave_devolucao = medicamentos_valores_devolvidos.chave_devolucao;

SELECT
	LEFT(chave_codigo, 44) AS chave,
	chave_devolucao,
	quantidade_devolucao,
	valor_produtos_devolucao,
	descricao_devolucao
FROM #MEDICAMENTOS_DEVOLVIDOS
WHERE LEFT(chave_codigo, 44) = '35210701645409000390550010004248401015650065'