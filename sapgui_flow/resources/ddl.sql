CREATE TABLE sap.clientes_kna1 (
    id_cliente VARCHAR(100),
    chave_pais_regiao VARCHAR(100),
    nome_1 VARCHAR(255),
    nome_2 VARCHAR(255),
    cidade VARCHAR(100),
    codigo_postal VARCHAR(100),
    regiao VARCHAR(100),
    termo_pesquisa VARCHAR(255),
    rua VARCHAR(255),
    telefone_1 VARCHAR(100),
    endereco VARCHAR(255),
    data_criacao DATE,
    criado_por VARCHAR(100),
    bairro VARCHAR(100),
    idioma VARCHAR(100),
    cnpj VARCHAR(14), 
    cpf VARCHAR(11), 
    pessoa_fisica BIT,
    faturamento_anual VARCHAR(100),
    domicilio_fiscal VARCHAR(100),
    cnae_fiscal VARCHAR(100),
    natureza_juridica VARCHAR(4),
    tipo_declaracao_imposto VARCHAR(100),
    inserted_at DATETIME
);

CREATE TABLE sap.clientes_knvv (
    id_cliente INT,
    organizacao_vendas INT,
    canal_distribuicao INT,
    setor_atividade INT,
    esquema_cliente VARCHAR(10),
    regiao_vendas VARCHAR(10),
    grupo_precos_cliente VARCHAR(10),
    grupo_class_conta_cli VARCHAR(10),
    criado_por VARCHAR(100),
    data_criacao DATE,
    inserted_at DATETIME
);

ALTER TABLE sap.clientes_knvv
ADD unique_key AS CONCAT(id_cliente, organizacao_vendas, canal_distribuicao, setor_atividade)

CREATE TABLE sap.notas_fiscais_j1bnfdoc (
    numero_documento INT,
    categoria_nota_fiscal VARCHAR(10),
    tipo_documento INT,
    direcao_movimento INT,
    data_documento DATE,
    data_lancamento DATE,
    data_criacao DATE,
    criado_as TIME,
    criado_por VARCHAR(100),
    modificado_em DATE,
    modificado_as TIME,
    modificado_por VARCHAR(100),
    numero_nota_fiscal INT,
    moeda_documento VARCHAR(3),
    empresa INT,
    nf_funcao_parceiro VARCHAR(5),
    id_parceiro VARCHAR(100),
    tipo_parceiro VARCHAR(10),
    estornado BIT,
    data_estorno DATE,
    existe_documento_subsequente VARCHAR(1),
    numero_documento_original INT,
    data_base_prazo_pagamento DATE,
    valor_total_incluindo_imposto VARCHAR(100),
    numero_nfse_servicos VARCHAR(100),
    numero_nfe_servicos VARCHAR(100),
    status_documento VARCHAR(100),
    codigo_status VARCHAR(100),
    pessoa_fisica BIT,
    codigo_cgc VARCHAR(255),
    numero_cpf VARCHAR(100),
    domicilio_fiscal VARCHAR(100),
    endereco_email VARCHAR(255),
    data_processamento DATE,
    hora_processamento TIME,
    registro_hora_criacao VARCHAR(255),
    inserted_at DATETIME
);

CREATE TABLE sap.notas_fiscais_j1bnflin (
    numero_documento VARCHAR(100),
    numero_item_documento VARCHAR(100),
    material VARCHAR(10),
    area_avaliacao VARCHAR(10),
    texto_breve_material VARCHAR(255),
    referencia_documento_origem VARCHAR(100),
    valor_liquido VARCHAR(100),
    centro VARCHAR(10),
    pedido VARCHAR(255),
    codigo_conta_analitica_contabil_debito_credito VARCHAR(100),
    lc_116_codigo_tipo_servico VARCHAR(100),
    centro_lucro VARCHAR(100),
    inserted_at DATETIME
);

ALTER TABLE sap.notas_fiscais_j1bnflin
ADD unique_key AS CONCAT(numero_documento,numero_item_documento)

CREATE TABLE sap.faturamento_vbrk (
    documento_faturamento INT,
    tipo_documento_faturamento VARCHAR(10),
    organizacao_vendas INT,
    canal_distribuicao INT,
    numero_condicao_documento INT,
    data_faturamento DATE,
    numero_documento VARCHAR(100),
    exercicio VARCHAR(100),
    status_lancamento VARCHAR(100),
    forma_pagamento VARCHAR(100),
    valor_liquido VARCHAR(100),
    hora_registro TIME,
    data_criacao DATE,
    pagador INT,
    modificado_em DATE,
    referencia VARCHAR(50),
    atribuicao VARCHAR(50),
    montante_imposto VARCHAR(100),
    registro_hora VARCHAR(100),
    status_lancamento_1 VARCHAR(10),
    status_global VARCHAR(10),
    status_compensacao VARCHAR(10),
    status VARCHAR(10),
    tipo_documento VARCHAR(10),
    inserted_at DATETIME
);

CREATE TABLE sap.faturamento_vbrp (
    data_prestacao_servico DATE,
    valor_liquido VARCHAR(100),
    documento_vendas VARCHAR(10),
    item VARCHAR(10),
    denominacao_item VARCHAR(255),
    categoria_item VARCHAR(10),
    grupo_class_cont_mat VARCHAR(10),
    devolucao VARCHAR(10),
    centro_lucro VARCHAR(10),
    material_inserido VARCHAR(10),
    montante_imposto VARCHAR(100),
    documento_faturamento VARCHAR(10),
    tipo_documento_faturamento VARCHAR(10),
    organizacao_vendas VARCHAR(10),
    canal_distribuicao VARCHAR(10),
    status VARCHAR(10),
    pagador VARCHAR(10), 
    data_faturamento DATE,
    empresa VARCHAR(10),
    numero_condicao_documento VARCHAR(10),
    documento_faturamento_estornado VARCHAR(10),
    categoria_documento_sd VARCHAR(10),
    inserted_at DATETIME
);

CREATE TABLE sap.ordens_venda_vbak (
    documento_vendas INT,
    data_criacao DATE,
    criado_por VARCHAR(100),
    categoria_documento VARCHAR(10),
    tipo_documento_vendas VARCHAR(10),
    bloqueio_remessa VARCHAR(255),
    bloqueio_documento_faturamento VARCHAR(255),
    valor_liquido VARCHAR(100),
    organizacao_vendas INT,
    canal_distribuicao INT,
    setor_atividade INT,
    numero_condicao_documento VARCHAR(100),
    probabilidade INT,
    referencia_cliente VARCHAR(255),
    emissor_ordem INT,
    modificado_por VARCHAR(100),
    inserted_at DATETIME
)

CREATE TABLE sap.ordens_venda_vbap (
    documento_vendas VARCHAR(100),
    item VARCHAR(10),
    material VARCHAR(100),
    denominacao_item VARCHAR(100),
    categoria_item VARCHAR(100),
    motivo_recusa VARCHAR(255),
    bloqueio_documento_faturamento VARCHAR(255),
    setor_atividade INT,
    setor_atividade_1 INT,
    valor_liquido VARCHAR(100),
    centro INT,
    probabilidade INT,
    data_criacao DATE,
    criado_por VARCHAR(100),
    grupo_class_cont_mat INT,
    subtotal_1 VARCHAR(100),
    modificado_em DATE,
    centro_lucro INT,
    status_recusa VARCHAR(10),
    tipo_documento_vendas VARCHAR(10),
    montante_imposto VARCHAR(100),
    organizacao_vendas INT,
    emissor_ordem INT,
    numero_condicao_documento VARCHAR(255),
    referencia_cliente VARCHAR(255),
    data_faturamento VARCHAR(100),
    data_pagamento_estimado DATE,
    inserted_at DATETIME
);

ALTER TABLE sap.ordens_venda_vbap
ADD unique_key AS CONCAT(documento_vendas,item)

CREATE TABLE sap.documentos_contabeis_bkpf (
    empresa VARCHAR(100),
    numero_documento VARCHAR(100),
    exercicio VARCHAR(100),
    tipo_documento VARCHAR(10),
    data_documento VARCHAR(100),
    data_lancamento DATE,
    periodo INT,
    data_entrada DATE,
    nome_usuario VARCHAR(100),
    chave_referencia VARCHAR(255),
    taxa_cambio_3 VARCHAR(100),
    documento_estorno VARCHAR(100),
    estornado BIT,
    inserted_at DATETIME
);

ALTER TABLE sap.documentos_contabeis_bkpf
ADD unique_key AS CONCAT(empresa,numero_documento,exercicio)

CREATE TABLE sap.documentos_contabeis_bseg (
    data_lancamento DATE,
    data_base_prazo_pagamento DATE,
    empresa VARCHAR(100),
    numero_documento VARCHAR(100),
    exercicio VARCHAR(100),
    item VARCHAR(100),
    data_compensacao_1 DATE,
    data_compensacao_2 DATE,
    documento_compensacao VARCHAR(100),
    chave_lancamento VARCHAR(100),
    tipo_conta VARCHAR(1),
    debito_credito VARCHAR(1),
    montante_moeda_funcional VARCHAR(100),
    data_efetiva DATE,
    atribuicao VARCHAR(255),
    texto VARCHAR(255),
    montante_alocado VARCHAR(100),
    data_planejamento DATE,
    conta_razao_1  VARCHAR(100),
    conta_razao_2 VARCHAR(100),
    cliente VARCHAR(100),
    fornecedor VARCHAR(100),
    condicoes_pagamento VARCHAR(100),
    montante_mi_3 VARCHAR(100),
    anular_compensacao BIT,
    numero_conta_alternativa VARCHAR(255),
    area_controle_credito VARCHAR(255),
    referencia_pagamento VARCHAR(255),
    item_compensacao VARCHAR(255),
    chave_referencia VARCHAR(255),
    tipo_fluxo VARCHAR(100),
    inserted_at DATETIME
);

ALTER TABLE sap.documentos_contabeis_bseg
ADD unique_key AS CONCAT(empresa,numero_documento,exercicio,item)

CREATE TABLE sap.organizacoes_vendas_tvko (
    organizacao_vendas VARCHAR(10),
    moeda VARCHAR(10),
    empresa VARCHAR(10),
    denominacao VARCHAR(255),
    endereco VARCHAR(100),
    texto_endereco VARCHAR(255),
    organizacao_vendas_refencia_tipo_documento VARCHAR(10),
    categoria_documento_compras VARCHAR(10),
    inserted_at DATETIME
);



