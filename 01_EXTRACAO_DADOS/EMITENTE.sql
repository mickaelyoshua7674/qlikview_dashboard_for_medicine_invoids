SELECT
	emitente.id_destinatario_emitente AS id_emitente,
	cpf_cnpj.cnpj AS cnpj_emitente,
	cpf_cnpj.cpf AS cpf_emitente,
    pessoa_juridica.razao_social_nome AS razao_social_emitente,
    emitente.nome_fantasia AS nome_fantasia_emitente,
	endereco.codigo_municipio AS codigo_municipio_emitente
FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS emitente
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Relação com tabela DIM_CPF_CNPJ para pegar o cnpj e cpf
	ON emitente.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_ENDERECO as endereco -- Relação com DIM_ENDERECO para pegar o municipio de cada emitente
	ON emitente.id_endereco = endereco.id_endereco
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Relação com banco de dados da Rceita Federal através do NPJ para pegar razão social por lá
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
INNER JOIN ( -- pegando apenas os IDs de emitentes
	SELECT DISTINCT	
		nfe.id_emitente
	FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe
) AS nfe
	ON emitente.id_destinatario_emitente = nfe.id_emitente;