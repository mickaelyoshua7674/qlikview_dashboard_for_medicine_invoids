DROP TABLE IF EXISTS #CHAVES_DISTINTAS_DEVOLVIDAS;
SELECT
	DISTINCT chave.chave, -- Para eliminar notas fiscais carregadas duas vezes
	FIRST_VALUE(chave.id_chave) OVER( -- inserindo primeiro id para ser a primeira nota carregada e fazer relação com as outras tabelas
		PARTITION BY chave.chave
		ORDER BY chave.chave
	) AS id_chave
	INTO #CHAVES_DISTINTAS_DEVOLVIDAS
FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
	ON chave.id_chave = nfe.id_chave
WHERE
	nfe.nota_cancelada = 0 AND -- Notas que não foram canceladas
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
	FIRST_VALUE(produtos.descricao) OVER( -- Pegando valor da descrição e botando como uma coluna
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
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que é o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (SELECT id_chave, chave FROM #CHAVES_DISTINTAS_DEVOLVIDAS) AS chaves_distintas
	ON nfe.id_chave = chaves_distintas.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente -- Relação das notas fiscais com os dados de seus emitentes
	ON nfe.id_emitente = emitente.id_destinatario_emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS destinatario -- Relação das notas fiscais com os dados de seus destinatarios
	ON nfe.id_destinatario = destinatario.id_destinatario_emitente
LEFT JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_ENDERECO as endereco -- Relação com DIM_ENDERECO para pegar o municipio de cada destinatario
	ON destinatario.id_endereco = endereco.id_endereco
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Relação com tabela DIM_CPF_CNPJ para pegar o cnpj
	ON emitente.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Relação com banco de dados da Rceita Federal através do NPJ para filtras os códigos CNAE das empresas
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_REFERENCIA AS referencia -- Relação para pegar referência
	ON nfe.id_chave = referencia.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE_REFERENCIADA AS chave_referenciada -- Relação para pegar a chave da NF que está sendo referenciada
	ON referencia.id_chave_referenciada = chave_referenciada.id_chave_referenciada
--------------------------------------------------------------------------------------------------------------------------------------------------------------
-- JOINs com tabela DIM_DATA para pegar datas de emissão de nota, fabricação e validade de produtos
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
		-- Código para bebidas, líquidos alcoólicos e vinagres (Capítulo 22)
		produtos.codigo_ncm LIKE '22071%' OR
		produtos.codigo_ncm LIKE '22072%' OR
		-- Códigos NCM para produtos farmacêuticos (Capítulo 30)
		produtos.codigo_ncm LIKE '30%' OR
		-- Código Extratos tanantes e tintoriais (Capítulo 32)
		produtos.codigo_ncm LIKE '320416%' OR -- corantes reagentes e preparações à base desses corantes
		-- Código para produtos para fotografia e cinematografia (Capítulo 37)
		produtos.codigo_ncm LIKE '370110%' OR -- filme para raio x
		-- Código produtos químicos diversos (Capítulo 38)
		produtos.codigo_ncm LIKE '3821%' OR -- Meios de cultura preparados para o desenvolvimento e a manutenção de microrganismos 
		produtos.codigo_ncm LIKE '3822%' OR -- reagentes de diagnóstico ou de laboratório (e.g. analisador de PH, glicose liquida, fósforo UV, hemoglobina glicada, acido urico)
		-- Código NCM para produtos de borracha (Capítulo 40)
		produtos.codigo_ncm LIKE '40151%' OR
		-- Código para tecidos especiais, tecidos tufados, rendas, tapeçarias, passamanarias, bordados (Capítulo 58)
		produtos.codigo_ncm LIKE '5803%' OR -- Tecidos em ponto de gaze
		-- Código para Outros artefatos têxteis confeccionados (Capítulo 60)
		produtos.codigo_ncm LIKE '6307901%' OR -- Outros artefatos confeccionados de falso tecido (e.g. máscara)
		-- Código Reatores nucleares, caldeiras, máquinas, aparelhos e instrumentos mecânicos, e suas partes (Capítulo 84)
		produtos.codigo_ncm LIKE '842119%' OR -- Centrifugadores para laboratórios de análises, ensaios ou pesquisas científicas
		produtos.codigo_ncm LIKE '842191%' OR -- Partes de centrifugadores
		produtos.codigo_ncm LIKE '84798912' OR -- Distribuidores e doseadores de sólidos ou de líquidos  (e.g. micropipeta)
		-- Códigos NCM para instrumentos e aparelhos de óptica, de fotografia, de cinematografia, de medida, de controle ou de precisão.
		-- instrumentos e aparelhos médico-cirúrgicos suas partes e acessórios (Capítulo 90)
		produtos.codigo_ncm LIKE '9011%' OR --  Microscópios ópticos, incluindo os microscópios para fotomicrografia, cinefotomicrografia ou microprojeção.
		produtos.codigo_ncm LIKE '9012%' OR --  Microscópios, exceto ópticos, difratógrafos.
		produtos.codigo_ncm LIKE '9018%' OR -- instumentos e aparelhos para medicina, cirurgia, odontologia e veterinária
		produtos.codigo_ncm LIKE '9019%' OR -- aparelhos de mecanoterapia, aparelhos de massagem, de psicotécnica, de ozonoterapia...
		produtos.codigo_ncm LIKE '9020%' OR -- outros aparelhos respiratórios e máscaras contra gases
		produtos.codigo_ncm LIKE '9021%' OR -- Artigos e aparelhos ortopédicos
		produtos.codigo_ncm LIKE '9022%' OR -- Aparelhos de raios X e aparelhos que utilizem radiações alfa
		produtos.codigo_ncm LIKE '9027%' OR -- Instrumentos e aparelhos para análises físicas ou químicas
		-- Códigos para Obras diversas (Capítulo 96)
		produtos.codigo_ncm LIKE '9619%' OR --  Absorventes e tampões higiênicos, cueiros e fraldas para bebês e artigos higiênicos semelhantes, de qualquer matéria.
			-- FONTE: http://www5.sefaz.mt.gov.br/documents/6071037/6401784/Tabela+NCM+-+MDIC+atualizada.pdf/bc780e4b-fd2f-4312-879c-65d5fd1ff49d / Pág. 129
		produtos.id_medicamento != -1 -- possui id de medicamento válido
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
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que é o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (SELECT id_chave, chave FROM #CHAVES_DISTINTAS_DEVOLVIDAS) AS chaves_distintas
	ON nfe.id_chave = chaves_distintas.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente_destinatario -- Relação das notas fiscais com os dados de seus emitentes
	ON nfe.id_emitente = emitente_destinatario.id_destinatario_emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Relação com tabela DIM_CPF_CNPJ para pegar o cnpj
	ON emitente_destinatario.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Relação com banco de dados da Rceita Federal através do NPJ para filtras os códigos CNAE das empresas
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_REFERENCIA AS referencia -- Relação para pegar referência
	ON nfe.id_chave = referencia.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE_REFERENCIADA AS chave_referenciada -- Relação para pegar a chave da NF que está sendo referenciada
	ON referencia.id_chave_referenciada = chave_referenciada.id_chave_referenciada
--------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE
--FILTRO PRODUTOS
	(
		-- Código para bebidas, líquidos alcoólicos e vinagres (Capítulo 22)
		produtos.codigo_ncm LIKE '22071%' OR
		produtos.codigo_ncm LIKE '22072%' OR
		-- Códigos NCM para produtos farmacêuticos (Capítulo 30)
		produtos.codigo_ncm LIKE '30%' OR
		-- Código Extratos tanantes e tintoriais (Capítulo 32)
		produtos.codigo_ncm LIKE '320416%' OR -- corantes reagentes e preparações à base desses corantes
		-- Código para produtos para fotografia e cinematografia (Capítulo 37)
		produtos.codigo_ncm LIKE '370110%' OR -- filme para raio x
		-- Código produtos químicos diversos (Capítulo 38)
		produtos.codigo_ncm LIKE '3821%' OR -- Meios de cultura preparados para o desenvolvimento e a manutenção de microrganismos 
		produtos.codigo_ncm LIKE '3822%' OR -- reagentes de diagnóstico ou de laboratório (e.g. analisador de PH, glicose liquida, fósforo UV, hemoglobina glicada, acido urico)
		-- Código NCM para produtos de borracha (Capítulo 40)
		produtos.codigo_ncm LIKE '40151%' OR
		-- Código para tecidos especiais, tecidos tufados, rendas, tapeçarias, passamanarias, bordados (Capítulo 58)
		produtos.codigo_ncm LIKE '5803%' OR -- Tecidos em ponto de gaze
		-- Código para Outros artefatos têxteis confeccionados (Capítulo 60)
		produtos.codigo_ncm LIKE '6307901%' OR -- Outros artefatos confeccionados de falso tecido (e.g. máscara)
		-- Código Reatores nucleares, caldeiras, máquinas, aparelhos e instrumentos mecânicos, e suas partes (Capítulo 84)
		produtos.codigo_ncm LIKE '842119%' OR -- Centrifugadores para laboratórios de análises, ensaios ou pesquisas científicas
		produtos.codigo_ncm LIKE '842191%' OR -- Partes de centrifugadores
		produtos.codigo_ncm LIKE '84798912' OR -- Distribuidores e doseadores de sólidos ou de líquidos  (e.g. micropipeta)
		-- Códigos NCM para instrumentos e aparelhos de óptica, de fotografia, de cinematografia, de medida, de controle ou de precisão.
		-- instrumentos e aparelhos médico-cirúrgicos suas partes e acessórios (Capítulo 90)
		produtos.codigo_ncm LIKE '9011%' OR --  Microscópios ópticos, incluindo os microscópios para fotomicrografia, cinefotomicrografia ou microprojeção.
		produtos.codigo_ncm LIKE '9012%' OR --  Microscópios, exceto ópticos, difratógrafos.
		produtos.codigo_ncm LIKE '9018%' OR -- instumentos e aparelhos para medicina, cirurgia, odontologia e veterinária
		produtos.codigo_ncm LIKE '9019%' OR -- aparelhos de mecanoterapia, aparelhos de massagem, de psicotécnica, de ozonoterapia...
		produtos.codigo_ncm LIKE '9020%' OR -- outros aparelhos respiratórios e máscaras contra gases
		produtos.codigo_ncm LIKE '9021%' OR -- Artigos e aparelhos ortopédicos
		produtos.codigo_ncm LIKE '9022%' OR -- Aparelhos de raios X e aparelhos que utilizem radiações alfa
		produtos.codigo_ncm LIKE '9027%' OR -- Instrumentos e aparelhos para análises físicas ou químicas
		-- Códigos para Obras diversas (Capítulo 96)
		produtos.codigo_ncm LIKE '9619%' OR --  Absorventes e tampões higiênicos, cueiros e fraldas para bebês e artigos higiênicos semelhantes, de qualquer matéria.
			-- FONTE: http://www5.sefaz.mt.gov.br/documents/6071037/6401784/Tabela+NCM+-+MDIC+atualizada.pdf/bc780e4b-fd2f-4312-879c-65d5fd1ff49d / Pág. 129
		produtos.id_medicamento != -1 -- possui id de medicamento válido
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