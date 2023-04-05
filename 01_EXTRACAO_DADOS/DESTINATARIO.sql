SELECT
	destinatario.id_destinatario_emitente AS id_destinatario,
	cpf_cnpj.cnpj AS cnpj_destinatario,
	cpf_cnpj.cpf AS cpf_destinatario,
    pessoa_juridica.razao_social_nome AS razao_social_destinatario,
    destinatario.nome_fantasia AS nome_fantasia_destinatario,
	endereco.codigo_municipio AS codigo_municipio_destinatario
FROM NFE_PUBLICA_PRODUCAO.dbo.DIM_DESTINATARIO_EMITENTE AS destinatario
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_CPF_CNPJ AS cpf_cnpj -- Relação com tabela DIM_CPF_CNPJ para pegar o cnpj e cpf
	ON destinatario.id_cpf_cnpj = cpf_cnpj.id_cpf_cnpj
INNER JOIN NFE_PUBLICA_PRODUCAO.dbo.DIM_ENDERECO as endereco -- Relação com DIM_ENDERECO para pegar o municipio de cada destinatario
	ON destinatario.id_endereco = endereco.id_endereco
INNER JOIN RF.dbo.t_pessoa_juridica AS pessoa_juridica -- Relação com banco de dados da Rceita Federal através do CNPJ para pegar razão social por lá
	ON cpf_cnpj.cnpj = pessoa_juridica.cnpj
INNER JOIN ( -- pegando apenas os IDs de destinatários
	SELECT DISTINCT	
		nfe.id_destinatario
	FROM NFE_PUBLICA_PRODUCAO.dbo.FATO_NFE AS nfe
) AS nfe
	ON destinatario.id_destinatario_emitente = nfe.id_destinatario;