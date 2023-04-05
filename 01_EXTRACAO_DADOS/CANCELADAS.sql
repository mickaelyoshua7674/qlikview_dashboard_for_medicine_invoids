SELECT
	MEDICAMENTOS_DADOS.chave_codigo,
	MEDICAMENTOS_DADOS.chave,
	MEDICAMENTOS_DADOS.id_produto,
	MEDICAMENTOS_DADOS.descricao,
	MEDICAMENTOS_DADOS.unidade,
	MEDICAMENTOS_DADOS.id_emitente,
	MEDICAMENTOS_DADOS.id_destinatario,
	MEDICAMENTOS_DADOS.codigo_municipio_destinatario,
	MEDICAMENTOS_DADOS.data_emissao,
	MEDICAMENTOS_DADOS.data_fabricacao,
	MEDICAMENTOS_DADOS.data_validade,
	MEDICAMENTOS_DADOS.lote,
	MEDICAMENTOS_DADOS.quantidade_lote,
	MEDICAMENTOS_DADOS.nota_cancelada,
	MEDICAMENTOS_VALORES.valor_unitario,
	MEDICAMENTOS_VALORES.quantidade
FROM (
SELECT
	DISTINCT CONCAT(chaves_distintas.chave, '-', produtos.codigo) AS chave_codigo,
	FIRST_VALUE(chaves_distintas.chave) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS chave,
	FIRST_VALUE(produtos.id_produto) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS id_produto,
	FIRST_VALUE(produtos.descricao) OVER( -- Pegando valor da descrição e botando como uma coluna
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo) -- Particionando com a mesma chave em DISTINCT acima para que os valores estejam corretos e relacionados
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo) -- Ordenando pela mesma chave
	) AS descricao,
	FIRST_VALUE(produtos.unidade) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS unidade,
	FIRST_VALUE(nfe.id_emitente) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS id_emitente,
	FIRST_VALUE(nfe.id_destinatario) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS id_destinatario,
	FIRST_VALUE(endereco.codigo_municipio) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS codigo_municipio_destinatario,
	FIRST_VALUE(id_data_emissao.data) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS data_emissao,
	FIRST_VALUE(id_data_fabricacao.data) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS data_fabricacao,
	FIRST_VALUE(id_data_validade.data) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS data_validade,
	FIRST_VALUE(produtos.lote) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS lote,
	FIRST_VALUE(produtos.quantidade_lote) OVER(
		PARTITION BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
		ORDER BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
	) AS quantidade_lote,
	1 AS nota_cancelada
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que é o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (
	SELECT
		DISTINCT chave.chave, -- Para eliminar notas fiscais carregadas duas vezes
		FIRST_VALUE(chave.id_chave) OVER( -- inserindo primeiro id para ser a primeira nota carregada e fazer relação com as outras tabelas
			PARTITION BY chave.chave
			ORDER BY chave.chave
		) AS id_chave
	FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave
	INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
		ON chave.id_chave = nfe.id_chave
	WHERE
		nfe.nota_cancelada = 1 AND -- Notas que foram canceladas
		nfe.id_modelo = 2 -- Notas 55
) AS chaves_distintas
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
        -- Código para Produtos químicos orgânicos (Capítulo 29)
        produtos.codigo_ncm LIKE '29037931%' OR produtos.codigo_ncm LIKE '29054400%' OR produtos.codigo_ncm LIKE '29054500%' OR produtos.codigo_ncm LIKE '29061100%' OR
        produtos.codigo_ncm LIKE '29072200%' OR produtos.codigo_ncm LIKE '29094910%' OR produtos.codigo_ncm LIKE '29095011%' OR produtos.codigo_ncm LIKE '29162014%' OR
        produtos.codigo_ncm LIKE '29163920%' OR produtos.codigo_ncm LIKE '29181610%' OR produtos.codigo_ncm LIKE '29182110%' OR produtos.codigo_ncm LIKE '29182211%' OR
        produtos.codigo_ncm LIKE '29189940%' OR produtos.codigo_ncm LIKE '292146%' OR produtos.codigo_ncm LIKE '2922192%' OR produtos.codigo_ncm LIKE '2922193%' OR
        produtos.codigo_ncm LIKE '2922194%' OR produtos.codigo_ncm LIKE '29221995%' OR produtos.codigo_ncm LIKE '29223120%' OR produtos.codigo_ncm LIKE '2922392%' OR
        produtos.codigo_ncm LIKE '29224110%' OR produtos.codigo_ncm LIKE '29224910%' OR produtos.codigo_ncm LIKE '2922496%' OR produtos.codigo_ncm LIKE '2922501%' OR
        produtos.codigo_ncm LIKE '2922502%' OR produtos.codigo_ncm LIKE '2922503%' OR produtos.codigo_ncm LIKE '2922504%' OR produtos.codigo_ncm LIKE '29231000%' OR
        produtos.codigo_ncm LIKE '29241992%' OR produtos.codigo_ncm LIKE '29242914%' OR produtos.codigo_ncm LIKE '29242943%' OR produtos.codigo_ncm LIKE '29242951%' OR
        produtos.codigo_ncm LIKE '29242952%' OR produtos.codigo_ncm LIKE '29251910%' OR produtos.codigo_ncm LIKE '2925291%' OR produtos.codigo_ncm LIKE '29252921%' OR
        produtos.codigo_ncm LIKE '29252923%' OR produtos.codigo_ncm LIKE '292630%' OR produtos.codigo_ncm LIKE '2926901%' OR produtos.codigo_ncm LIKE '29269024%' OR
        produtos.codigo_ncm LIKE '29280020%' OR produtos.codigo_ncm LIKE '29303012%' OR produtos.codigo_ncm LIKE '29309036%' OR produtos.codigo_ncm LIKE '29321910%' OR
        produtos.codigo_ncm LIKE '2932991%' OR produtos.codigo_ncm LIKE '29329991%' OR produtos.codigo_ncm LIKE '29331111%' OR produtos.codigo_ncm LIKE '29331112%' OR
        produtos.codigo_ncm LIKE '2933212%' OR produtos.codigo_ncm LIKE '29332912%' OR produtos.codigo_ncm LIKE '29332922%' OR produtos.codigo_ncm LIKE '29332923%' OR
        produtos.codigo_ncm LIKE '29332925%' OR produtos.codigo_ncm LIKE '29332930%' OR produtos.codigo_ncm LIKE '29332991%' OR produtos.codigo_ncm LIKE '29332993%' OR
        produtos.codigo_ncm LIKE '293331%' OR produtos.codigo_ncm LIKE '29333322%' OR produtos.codigo_ncm LIKE '29333381%' OR produtos.codigo_ncm LIKE '29333915%' OR
        produtos.codigo_ncm LIKE '29333924%' OR produtos.codigo_ncm LIKE '29333932%' OR produtos.codigo_ncm LIKE '29333943%' OR produtos.codigo_ncm LIKE '29333944%' OR
        produtos.codigo_ncm LIKE '29333946%' OR produtos.codigo_ncm LIKE '29333948%' OR produtos.codigo_ncm LIKE '29333983%' OR produtos.codigo_ncm LIKE '29333992%' OR
        produtos.codigo_ncm LIKE '29335321%' OR produtos.codigo_ncm LIKE '29335340%' OR produtos.codigo_ncm LIKE '29335510%' OR produtos.codigo_ncm LIKE '29335912%' OR
        produtos.codigo_ncm LIKE '29335913%' OR produtos.codigo_ncm LIKE '29335914%' OR produtos.codigo_ncm LIKE '29335942%' OR produtos.codigo_ncm LIKE '29335991%' OR
        produtos.codigo_ncm LIKE '29337210%' OR produtos.codigo_ncm LIKE '29337910%' OR produtos.codigo_ncm LIKE '29339111%' OR produtos.codigo_ncm LIKE '29339113%' OR
        produtos.codigo_ncm LIKE '29339122%' OR produtos.codigo_ncm LIKE '29339142%' OR produtos.codigo_ncm LIKE '29339153%' OR produtos.codigo_ncm LIKE '29339162%' OR
        produtos.codigo_ncm LIKE '29339164%' OR produtos.codigo_ncm LIKE '29339911%' OR produtos.codigo_ncm LIKE '29339932%' OR produtos.codigo_ncm LIKE '29339946%' OR
        produtos.codigo_ncm LIKE '29339953%' OR produtos.codigo_ncm LIKE '29339954%' OR produtos.codigo_ncm LIKE '29339992%' OR produtos.codigo_ncm LIKE '29341030%' OR
        produtos.codigo_ncm LIKE '29343030%' OR produtos.codigo_ncm LIKE '29349122%' OR produtos.codigo_ncm LIKE '29349931%' OR produtos.codigo_ncm LIKE '29349954%' OR
        produtos.codigo_ncm LIKE '29349991%' OR produtos.codigo_ncm LIKE '29349993%' OR produtos.codigo_ncm LIKE '29350012%' OR produtos.codigo_ncm LIKE '29350021%' OR
        produtos.codigo_ncm LIKE '29350023%' OR produtos.codigo_ncm LIKE '29350024%' OR produtos.codigo_ncm LIKE '29350025%' OR produtos.codigo_ncm LIKE '29350094%' OR
        produtos.codigo_ncm LIKE '29362%' OR produtos.codigo_ncm LIKE '29371%' OR produtos.codigo_ncm LIKE '29372%' OR produtos.codigo_ncm LIKE '29375000%' OR
        produtos.codigo_ncm LIKE '29379030%' OR produtos.codigo_ncm LIKE '29389010%' OR produtos.codigo_ncm LIKE '29391122%' OR produtos.codigo_ncm LIKE '29391152%' OR
        produtos.codigo_ncm LIKE '2939116%' OR produtos.codigo_ncm LIKE '29391181%' OR produtos.codigo_ncm LIKE '29393010%' OR produtos.codigo_ncm LIKE '29394%' OR
        produtos.codigo_ncm LIKE '29395%' OR produtos.codigo_ncm LIKE '29396921%' OR produtos.codigo_ncm LIKE '29396952%' OR produtos.codigo_ncm LIKE '2939991%' OR
        produtos.codigo_ncm LIKE '2939993%' OR produtos.codigo_ncm LIKE '2941%' OR
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
--FILTRO PESSOA JURIDICA
    (
        pessoa_juridica.cnae_fiscal IN (2019399, 2110600, 2121101, 2121102, 2121103, 2123800, 2312500, 3299099, 3250701, 3250702, 3250705, 4618401, 4618402,
                                        4644301, 4644302, 4645101, 4645103, 4664800, 4684299, 4771701, 4771702, 4771703, 4771704, 4773300, 7210000) OR -- Empresas farmoquimicas e farmaceuticas
        pessoa_juridica.cnae_secundaria LIKE '%2019399%' OR pessoa_juridica.cnae_secundaria LIKE '%2110600%' OR pessoa_juridica.cnae_secundaria LIKE '%2121101%' OR pessoa_juridica.cnae_secundaria LIKE '%2121102%' OR
        pessoa_juridica.cnae_secundaria LIKE '%2121103%' OR pessoa_juridica.cnae_secundaria LIKE '%2123800%' OR pessoa_juridica.cnae_secundaria LIKE '%2312500%' OR pessoa_juridica.cnae_secundaria LIKE '%3299099%' OR
        pessoa_juridica.cnae_secundaria LIKE '%3250701%' OR pessoa_juridica.cnae_secundaria LIKE '%3250702%' OR pessoa_juridica.cnae_secundaria LIKE '%3250705%' OR pessoa_juridica.cnae_secundaria LIKE '%4618401%' OR
        pessoa_juridica.cnae_secundaria LIKE '%4618402%' OR pessoa_juridica.cnae_secundaria LIKE '%4644301%' OR pessoa_juridica.cnae_secundaria LIKE '%4644302%' OR pessoa_juridica.cnae_secundaria LIKE '%4645101%' OR
        pessoa_juridica.cnae_secundaria LIKE '%4645103%' OR pessoa_juridica.cnae_secundaria LIKE '%4664800%' OR pessoa_juridica.cnae_secundaria LIKE '%4684299%' OR pessoa_juridica.cnae_secundaria LIKE '%4771701%' OR
        pessoa_juridica.cnae_secundaria LIKE '%4771702%' OR pessoa_juridica.cnae_secundaria LIKE '%4771703%' OR pessoa_juridica.cnae_secundaria LIKE '%4771704%' OR pessoa_juridica.cnae_secundaria LIKE '%4773300%' OR
        pessoa_juridica.cnae_secundaria LIKE '%7210000%'
    )
-- Fonte: https://www.contabilizei.com.br/contabilidade-online/cnae/ - https://concla.ibge.gov.br/busca-online-cnae.html
--------------------------------------------------------------------------------------------------------------------------------------------------------------
) AS MEDICAMENTOS_DADOS

INNER JOIN (
SELECT
	CONCAT(chaves_distintas.chave, '-', produtos.codigo) AS chave_codigo,
	SUM(produtos.valor_unitario) AS valor_unitario,
	SUM(produtos.quantidade) AS	quantidade
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que é o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (
	SELECT
		DISTINCT chave.chave, -- Para eliminar notas fiscais carregadas duas vezes
		FIRST_VALUE(chave.id_chave) OVER( -- inserindo primeiro id para ser a primeira nota carregada e fazer relação com as outras tabelas
			PARTITION BY chave.chave
			ORDER BY chave.chave
		) AS id_chave
	FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave
	INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer ligação com a tabela DIM_DESTINATARIO_EMITENTE
		ON chave.id_chave = nfe.id_chave
	WHERE
		nfe.nota_cancelada = 1 AND -- Notas que foram canceladas
		nfe.id_modelo = 2 -- Notas 55
) AS chaves_distintas
	ON nfe.id_chave = chaves_distintas.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente_destinatario -- Relação das notas fiscais com os dados de seus emitentes
	ON nfe.id_emitente = emitente_destinatario.id_destinatario_emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Relação com tabela DIM_CPF_CNPJ para pegar o cnpj
	ON emitente_destinatario.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Relação com banco de dados da Rceita Federal através do NPJ para filtras os códigos CNAE das empresas
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
--------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE
--FILTRO PRODUTOS
	(
		-- Código para bebidas, líquidos alcoólicos e vinagres (Capítulo 22)
		produtos.codigo_ncm LIKE '22071%' OR
		produtos.codigo_ncm LIKE '22072%' OR
        -- Código para Produtos químicos orgânicos (Capítulo 29)
        produtos.codigo_ncm LIKE '29037931%' OR produtos.codigo_ncm LIKE '29054400%' OR produtos.codigo_ncm LIKE '29054500%' OR produtos.codigo_ncm LIKE '29061100%' OR
        produtos.codigo_ncm LIKE '29072200%' OR produtos.codigo_ncm LIKE '29094910%' OR produtos.codigo_ncm LIKE '29095011%' OR produtos.codigo_ncm LIKE '29162014%' OR
        produtos.codigo_ncm LIKE '29163920%' OR produtos.codigo_ncm LIKE '29181610%' OR produtos.codigo_ncm LIKE '29182110%' OR produtos.codigo_ncm LIKE '29182211%' OR
        produtos.codigo_ncm LIKE '29189940%' OR produtos.codigo_ncm LIKE '292146%' OR produtos.codigo_ncm LIKE '2922192%' OR produtos.codigo_ncm LIKE '2922193%' OR
        produtos.codigo_ncm LIKE '2922194%' OR produtos.codigo_ncm LIKE '29221995%' OR produtos.codigo_ncm LIKE '29223120%' OR produtos.codigo_ncm LIKE '2922392%' OR
        produtos.codigo_ncm LIKE '29224110%' OR produtos.codigo_ncm LIKE '29224910%' OR produtos.codigo_ncm LIKE '2922496%' OR produtos.codigo_ncm LIKE '2922501%' OR
        produtos.codigo_ncm LIKE '2922502%' OR produtos.codigo_ncm LIKE '2922503%' OR produtos.codigo_ncm LIKE '2922504%' OR produtos.codigo_ncm LIKE '29231000%' OR
        produtos.codigo_ncm LIKE '29241992%' OR produtos.codigo_ncm LIKE '29242914%' OR produtos.codigo_ncm LIKE '29242943%' OR produtos.codigo_ncm LIKE '29242951%' OR
        produtos.codigo_ncm LIKE '29242952%' OR produtos.codigo_ncm LIKE '29251910%' OR produtos.codigo_ncm LIKE '2925291%' OR produtos.codigo_ncm LIKE '29252921%' OR
        produtos.codigo_ncm LIKE '29252923%' OR produtos.codigo_ncm LIKE '292630%' OR produtos.codigo_ncm LIKE '2926901%' OR produtos.codigo_ncm LIKE '29269024%' OR
        produtos.codigo_ncm LIKE '29280020%' OR produtos.codigo_ncm LIKE '29303012%' OR produtos.codigo_ncm LIKE '29309036%' OR produtos.codigo_ncm LIKE '29321910%' OR
        produtos.codigo_ncm LIKE '2932991%' OR produtos.codigo_ncm LIKE '29329991%' OR produtos.codigo_ncm LIKE '29331111%' OR produtos.codigo_ncm LIKE '29331112%' OR
        produtos.codigo_ncm LIKE '2933212%' OR produtos.codigo_ncm LIKE '29332912%' OR produtos.codigo_ncm LIKE '29332922%' OR produtos.codigo_ncm LIKE '29332923%' OR
        produtos.codigo_ncm LIKE '29332925%' OR produtos.codigo_ncm LIKE '29332930%' OR produtos.codigo_ncm LIKE '29332991%' OR produtos.codigo_ncm LIKE '29332993%' OR
        produtos.codigo_ncm LIKE '293331%' OR produtos.codigo_ncm LIKE '29333322%' OR produtos.codigo_ncm LIKE '29333381%' OR produtos.codigo_ncm LIKE '29333915%' OR
        produtos.codigo_ncm LIKE '29333924%' OR produtos.codigo_ncm LIKE '29333932%' OR produtos.codigo_ncm LIKE '29333943%' OR produtos.codigo_ncm LIKE '29333944%' OR
        produtos.codigo_ncm LIKE '29333946%' OR produtos.codigo_ncm LIKE '29333948%' OR produtos.codigo_ncm LIKE '29333983%' OR produtos.codigo_ncm LIKE '29333992%' OR
        produtos.codigo_ncm LIKE '29335321%' OR produtos.codigo_ncm LIKE '29335340%' OR produtos.codigo_ncm LIKE '29335510%' OR produtos.codigo_ncm LIKE '29335912%' OR
        produtos.codigo_ncm LIKE '29335913%' OR produtos.codigo_ncm LIKE '29335914%' OR produtos.codigo_ncm LIKE '29335942%' OR produtos.codigo_ncm LIKE '29335991%' OR
        produtos.codigo_ncm LIKE '29337210%' OR produtos.codigo_ncm LIKE '29337910%' OR produtos.codigo_ncm LIKE '29339111%' OR produtos.codigo_ncm LIKE '29339113%' OR
        produtos.codigo_ncm LIKE '29339122%' OR produtos.codigo_ncm LIKE '29339142%' OR produtos.codigo_ncm LIKE '29339153%' OR produtos.codigo_ncm LIKE '29339162%' OR
        produtos.codigo_ncm LIKE '29339164%' OR produtos.codigo_ncm LIKE '29339911%' OR produtos.codigo_ncm LIKE '29339932%' OR produtos.codigo_ncm LIKE '29339946%' OR
        produtos.codigo_ncm LIKE '29339953%' OR produtos.codigo_ncm LIKE '29339954%' OR produtos.codigo_ncm LIKE '29339992%' OR produtos.codigo_ncm LIKE '29341030%' OR
        produtos.codigo_ncm LIKE '29343030%' OR produtos.codigo_ncm LIKE '29349122%' OR produtos.codigo_ncm LIKE '29349931%' OR produtos.codigo_ncm LIKE '29349954%' OR
        produtos.codigo_ncm LIKE '29349991%' OR produtos.codigo_ncm LIKE '29349993%' OR produtos.codigo_ncm LIKE '29350012%' OR produtos.codigo_ncm LIKE '29350021%' OR
        produtos.codigo_ncm LIKE '29350023%' OR produtos.codigo_ncm LIKE '29350024%' OR produtos.codigo_ncm LIKE '29350025%' OR produtos.codigo_ncm LIKE '29350094%' OR
        produtos.codigo_ncm LIKE '29362%' OR produtos.codigo_ncm LIKE '29371%' OR produtos.codigo_ncm LIKE '29372%' OR produtos.codigo_ncm LIKE '29375000%' OR
        produtos.codigo_ncm LIKE '29379030%' OR produtos.codigo_ncm LIKE '29389010%' OR produtos.codigo_ncm LIKE '29391122%' OR produtos.codigo_ncm LIKE '29391152%' OR
        produtos.codigo_ncm LIKE '2939116%' OR produtos.codigo_ncm LIKE '29391181%' OR produtos.codigo_ncm LIKE '29393010%' OR produtos.codigo_ncm LIKE '29394%' OR
        produtos.codigo_ncm LIKE '29395%' OR produtos.codigo_ncm LIKE '29396921%' OR produtos.codigo_ncm LIKE '29396952%' OR produtos.codigo_ncm LIKE '2939991%' OR
        produtos.codigo_ncm LIKE '2939993%' OR produtos.codigo_ncm LIKE '2941%' OR
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
--FILTRO PESSOA JURIDICA
    (
        pessoa_juridica.cnae_fiscal IN (2019399, 2110600, 2121101, 2121102, 2121103, 2123800, 2312500, 3299099, 3250701, 3250702, 3250705, 4618401, 4618402,
                                        4644301, 4644302, 4645101, 4645103, 4664800, 4684299, 4771701, 4771702, 4771703, 4771704, 4773300, 7210000) OR -- Empresas farmoquimicas e farmaceuticas
        pessoa_juridica.cnae_secundaria LIKE '%2019399%' OR pessoa_juridica.cnae_secundaria LIKE '%2110600%' OR pessoa_juridica.cnae_secundaria LIKE '%2121101%' OR pessoa_juridica.cnae_secundaria LIKE '%2121102%' OR
        pessoa_juridica.cnae_secundaria LIKE '%2121103%' OR pessoa_juridica.cnae_secundaria LIKE '%2123800%' OR pessoa_juridica.cnae_secundaria LIKE '%2312500%' OR pessoa_juridica.cnae_secundaria LIKE '%3299099%' OR
        pessoa_juridica.cnae_secundaria LIKE '%3250701%' OR pessoa_juridica.cnae_secundaria LIKE '%3250702%' OR pessoa_juridica.cnae_secundaria LIKE '%3250705%' OR pessoa_juridica.cnae_secundaria LIKE '%4618401%' OR
        pessoa_juridica.cnae_secundaria LIKE '%4618402%' OR pessoa_juridica.cnae_secundaria LIKE '%4644301%' OR pessoa_juridica.cnae_secundaria LIKE '%4644302%' OR pessoa_juridica.cnae_secundaria LIKE '%4645101%' OR
        pessoa_juridica.cnae_secundaria LIKE '%4645103%' OR pessoa_juridica.cnae_secundaria LIKE '%4664800%' OR pessoa_juridica.cnae_secundaria LIKE '%4684299%' OR pessoa_juridica.cnae_secundaria LIKE '%4771701%' OR
        pessoa_juridica.cnae_secundaria LIKE '%4771702%' OR pessoa_juridica.cnae_secundaria LIKE '%4771703%' OR pessoa_juridica.cnae_secundaria LIKE '%4771704%' OR pessoa_juridica.cnae_secundaria LIKE '%4773300%' OR
        pessoa_juridica.cnae_secundaria LIKE '%7210000%'
    )
-- Fonte: https://www.contabilizei.com.br/contabilidade-online/cnae/ - https://concla.ibge.gov.br/busca-online-cnae.html
--------------------------------------------------------------------------------------------------------------------------------------------------------------
GROUP BY CONCAT(chaves_distintas.chave, '-', produtos.codigo)
) AS MEDICAMENTOS_VALORES
	ON MEDICAMENTOS_DADOS.chave_codigo = MEDICAMENTOS_VALORES.chave_codigo
ORDER BY MEDICAMENTOS_DADOS.chave_codigo;