SELECT
	chaves_distintas.chave AS chave_devolucao,
	COUNT(chaves_distintas.chave) AS qtde_itens_devolucao
FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_PRODUTO AS produtos -- Carregando a tabela produtos que � o alvo da query
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer liga��o com a tabela DIM_DESTINATARIO_EMITENTE
	ON produtos.id_chave = nfe.id_chave
INNER JOIN (
	SELECT
		DISTINCT chave.chave, -- Para eliminar notas fiscais carregadas duas vezes
		FIRST_VALUE(chave.id_chave) OVER( -- inserindo primeiro id para ser a primeira nota carregada e fazer rela��o com as outras tabelas
			PARTITION BY chave.chave
			ORDER BY chave.chave
		) AS id_chave
	FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_CHAVE AS chave
	INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe -- Juntando com a tabela NFE para, a partir dela, fazer liga��o com a tabela DIM_DESTINATARIO_EMITENTE
		ON chave.id_chave = nfe.id_chave
	WHERE
		nfe.nota_cancelada = 0 AND -- Notas que n�o foram canceladas
		nfe.id_modelo = 2 AND -- Notas 55
		nfe.id_finalidade = 4 -- Notas que possuem valor 4 (DEVOLUCAO DE MERCADORIA)
) AS chaves_distintas
	ON nfe.id_chave = chaves_distintas.id_chave
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente_destinatario -- Rela��o das notas fiscais com os dados de seus emitentes
	ON nfe.id_emitente = emitente_destinatario.id_destinatario_emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Rela��o com tabela DIM_CPF_CNPJ para pegar o cnpj
	ON emitente_destinatario.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Rela��o com banco de dados da Rceita Federal atrav�s do NPJ para filtras os c�digos CNAE das empresas
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
--------------------------------------------------------------------------------------------------------------------------------------------------------------
WHERE
--FILTRO PRODUTOS
	(
		-- C�digo para bebidas, l�quidos alco�licos e vinagres (Cap�tulo 22)
		produtos.codigo_ncm LIKE '22071%' OR
		produtos.codigo_ncm LIKE '22072%' OR
        -- C�digo para Produtos qu�micos org�nicos (Cap�tulo 29)
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
GROUP BY chaves_distintas.chave;