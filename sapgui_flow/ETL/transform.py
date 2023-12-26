import pandas as pd
import os
import logging
from datetime import datetime, timedelta
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("my_logger")

five_days_ago = datetime.now() - timedelta(days=5)
five_days_ago = five_days_ago.strftime('%d/%m/%Y') # transformations that use this variable are those with incremental refresh enabled

def trim_dataframe(df):
    """
    Trims white spaces from all string columns of a DataFrame.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be trimmed.

    Returns:
    - pd.DataFrame: The trimmed DataFrame.
    """
    return df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# PREPARATIONS FOR V2 OF THIS ETL
# def commom_data_transformation(data, tablename, columns_to_rename):
#     logger.info(f'Started transformation {tablename}')
#     df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn')
#     df = df.iloc[1:]
#     df = df.loc[:, ~df.columns.str.contains('Unnamed')]
#     df.columns = df.columns.str.strip()
#     df = trim_dataframe(df)
#     df = df.replace('nan', pd.NA)
#     df.rename(columns=columns_to_rename, inplace=True)
#     df.to_csv('df.csv')
#     

#     logger.info(f'Finished transformation {tablename}')
#     return df


def transformVBAK(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    df = df.replace('nan', pd.NA)
    columns_to_rename = {
        'Criado em': 'data_criacao',
        'Criado por': 'criado_por',
        'Criado/a': 'criado_por',
        'Criado_a': 'criado_por',
        'Ctg.doc.': 'categoria_documento',
        'BlqR': 'bloqueio_remessa',
        'Doc.venda': 'documento_vendas',
        'TpDV': 'tipo_documento_vendas',
        'BF': 'bloqueio_documento_faturamento',
        'Val.líq.': 'valor_liquido',
        'OrgV': 'organizacao_vendas',
        'CDst': 'canal_distribuicao',
        'SA': 'setor_atividade',
        'Cond.doc.': 'numero_condicao_documento',
        'Prb': 'probabilidade',
        'Referência cliente': 'referencia_cliente',
        'Ref_cliente': 'referencia_cliente',
        'Ref.cliente': 'referencia_cliente',
        'Emis.ordem': 'emissor_ordem',
        'Modif.por': 'modificado_por'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['data_criacao'] = pd.to_datetime(df['data_criacao'].str.replace('.', '/'), format='%d/%m/%Y')
    df['documento_vendas'] = df['documento_vendas'].astype(str).str[:-2]
    df['organizacao_vendas'] = df['organizacao_vendas'].astype(str).str[:-2]
    df['canal_distribuicao'] = df['canal_distribuicao'].astype(str).str[:-2]
    df['setor_atividade'] = df['setor_atividade'].astype(str).str[:-2]
    df['probabilidade'] = df['probabilidade'].astype(str).str[:-2]
    df['numero_condicao_documento'] = df['numero_condicao_documento'].astype(str).str[:-2]
    df['emissor_ordem'] = df['emissor_ordem'].astype(str).str[:-2]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformKNA1(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Cliente': 'id_cliente',
        'P/R': 'chave_pais_regiao',
        'CódigoPost': 'codigo_postal',
        'CDst': 'canal_distribuicao',
        'Rg': 'regiao',
        'Crit.pesq.': 'termo_pesquisa',
        'ConcPesq': 'termo_pesquisa',
        'Telefone 1': 'telefone_1',
        'Nome 1': 'nome_1',
        'Nome 2': 'nome_2',
        'Endereço': 'endereco',
        'Cidade': 'cidade',
        'Bairro': 'bairro',
        'Idioma': 'idioma',
        'Rua': 'rua',
        'Dt.abert.': 'data_criacao',
        'Autor': 'criado_por',
        'Criado por': 'criado_por',
        'PessFísica': 'pessoa_fisica',
        'DomFiscal': 'domicilio_fiscal',
        'CNAE fisc.': 'cnae_fiscal',
        'TpDecImp.': 'tipo_declaracao_imposto',
        'Fatur.': 'faturamento_anual',
        'NºIdFisc-2': 'cpf',
        'Nº ID fiscal 1': 'cnpj',
        'NºIdFisc-1': 'cnpj',
        'CaracLegal': 'natureza_juridica'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['pessoa_fisica'] = df['pessoa_fisica'].replace({'X': True, '': False}).astype(bool)
    df['data_criacao'] = pd.to_datetime(df['data_criacao'].str.replace('.', '.'), format='%d.%m.%Y')
    df['telefone_1'] = df['telefone_1'].str.replace(r'\D', '', regex=True)
    df['endereco'] = df['endereco'].astype(str).str[:-2]

    # df = df.iloc[:, [0, 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]]

    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformKNVV(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Cliente': 'id_cliente',
        'OrgV': 'organizacao_vendas',
        'CDst': 'canal_distribuicao',
        'SA': 'setor_atividade',
        'Criado em': 'data_criacao',
        'Criado por': 'criado_por',
        'Criado/a': 'criado_por',
        'EsCl': 'esquema_cliente',
        'Região': 'regiao_vendas',
        'GPC': 'grupo_precos_cliente',
        'GCCC': 'grupo_class_conta_cli'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['data_criacao'] = pd.to_datetime(df['data_criacao'].str.replace('.', '/'), format='%d/%m/%Y')
    # df['data_criacao'] = df['data_criacao'].str[10:]
    df['organizacao_vendas'] = df['organizacao_vendas'].astype(str).str[:-2]
    df['id_cliente'] = df['id_cliente'].astype(str).str[:-2]
    df['esquema_cliente'] = df['esquema_cliente'].astype(str).str[:-2]
    df['grupo_precos_cliente'] = df['grupo_precos_cliente'].astype(str).str[:-2]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformJ_1BNFDOC(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Nº doc.': 'numero_documento',
        'CN': 'categoria_nota_fiscal',
        'D': 'tipo_documento',
        'M': 'direcao_movimento',
        'Data doc.': 'data_documento',
        'Dt.lçto.': 'data_lancamento',
        'Criado em': 'data_criacao',
        'Criado por': 'criado_por',
        'Criad.às': 'criado_as',
        'Usuário': 'criado_por',
        'Modif.em': 'modificado_em',
        'Modif.às': 'modificado_as',
        'Modif.por': 'modificado_por',
        'Usuário    .1': 'modificado_por',
        'Nº NF': 'numero_nota_fiscal',
        'Moeda': 'moeda_documento',
        'Empr': 'empresa',
        'FP': 'nf_funcao_parceiro',
        'ID parc.': 'id_parceiro',
        'P': 'tipo_parceiro',
        'Estornado': 'estornado',
        'Dt.estorno': 'data_estorno',
        'DocSubseq.': 'existe_documento_subsequente',
        'Doc.orig.': 'numero_documento_original',
        'Data base': 'data_base_prazo_pagamento',
        'Val.total': 'valor_total_incluindo_imposto',
	'Número NFS-e': 'numero_nfse_servicos',
        'Nº NFS-e': 'numero_nfse_servicos',
        'Nº NF-e': 'numero_nfe_servicos',
        'Stat.doc.': 'status_documento',
        'Cód.status': 'codigo_status',
        'PessFísica': 'pessoa_fisica',
        'Code CGC': 'codigo_cgc',
        'CPF': 'numero_cpf',
        'DomFiscal': 'domicilio_fiscal',
        'E-mail': 'endereco_email',
        'Data proc.': 'data_processamento',
        'H.procmto.': 'hora_processamento',
        'Reg.hora criação': 'registro_hora_criacao',
        'Reg.hora': 'registro_hora_criacao'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['data_processamento'] = pd.to_datetime(df['data_processamento'].str.replace('.', '/'), format='%d/%m/%Y')
    # df['data_base_prazo_pagamento'] = pd.to_datetime(df['data_base_prazo_pagamento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_estorno'] = pd.to_datetime(df['data_estorno'].str.replace('.', '/'), format='%d/%m/%Y')
    df['modificado_em'] = pd.to_datetime(df['modificado_em'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_criacao'] = pd.to_datetime(df['data_criacao'].str.replace('.', '/'), format='%d/%m/%Y')     
    df['data_lancamento'] = pd.to_datetime(df['data_lancamento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_documento'] = pd.to_datetime(df['data_documento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['codigo_cgc'] = df['codigo_cgc'].str[:18]
    df['numero_cpf'] = df['numero_cpf'].str[:14]
    df['pessoa_fisica'] = df['pessoa_fisica'].replace({'X': True, '': False}).astype(bool)
    df['existe_documento_subsequente'] = df['existe_documento_subsequente'].replace({'X': True, '': False}).astype(bool)
    df['estornado'] = df['estornado'].replace({'X': True, '': False}).astype(bool)
    df['valor_total_incluindo_imposto'] = df['valor_total_incluindo_imposto'].str.replace('.','')
    df['valor_total_incluindo_imposto'] = df['valor_total_incluindo_imposto'].str.replace(',','.')
    df['numero_nota_fiscal'] = df['numero_nota_fiscal'].replace('', pd.NA)
    df['id_parceiro'] = df['id_parceiro'].replace('', pd.NA)
    df['id_parceiro'] = df['id_parceiro'].astype(str).str[:-2]
    df['numero_nfe_servicos'] = df['numero_nfe_servicos'].replace('', pd.NA)
    df['status_documento'] = df['status_documento'].replace('', pd.NA)
    df['codigo_status'] = df['codigo_status'].replace('', pd.NA)

    df = df.replace('', pd.NA)
    # df = df[df['modificado_em'] >= five_days_ago]

    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformJ_1BNFLIN(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Nº doc.': 'numero_documento',
        'NºIt': 'numero_item_documento',
        'Material': 'material',
        'ÁrAv': 'area_avaliacao',
        'Texto breve material': 'texto_breve_material',
        'TxtBreveMaterial': 'texto_breve_material',
        'Ref_origem': 'referencia_documento_origem',
        'Ref.doc.origem': 'referencia_documento_origem',
        'Ref.origem': 'referencia_documento_origem',
        'Valor': 'valor_liquido',
        'Cen.': 'centro',
        'Pedido': 'pedido',
        'CtaAnalít.': 'codigo_conta_analitica_contabil_debito_credito',
        'LC116 cód.': 'lc_116_codigo_tipo_servico',
        'Cen.lucro': 'centro_lucro'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['centro'] = df['centro'].astype(str).str[:-2]
    df['area_avaliacao'] = df['area_avaliacao'].astype(str).str[:-2]
    df['numero_documento'] = df['numero_documento'].astype(str).str[:-2]
    df['numero_item_documento'] = df['numero_item_documento'].astype(str).str[:-2]
    df['referencia_documento_origem'] = df['referencia_documento_origem'].astype(str).str[:-2]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformVBRK(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Doc.fat': 'documento_faturamento',
        'Doc.fat.': 'documento_faturamento',
        'Doc_fat': 'documento_faturamento',
        'TipFt': 'tipo_documento_faturamento',
        'OrgV': 'organizacao_vendas',
        'CDst': 'canal_distribuicao',
        'SA': 'setor_atividade',
        'Criado em': 'data_criacao',
        'Cond.doc.': 'numero_condicao_documento',
        'Dt.fatur.': 'data_faturamento',
        'Nº doc.': 'numero_documento',
        'Ano': 'exercicio',
        'StCn': 'status_lancamento',
        'MP': 'forma_pagamento',
        'Val.líq.': 'valor_liquido',
        'Hor.rg.': 'hora_registro',
        'Pagador': 'pagador',
        'Modif.em': 'modificado_em',
        'Referência': 'referencia',
        'Atribuição': 'atribuicao',
        'Registro da hora': 'registro_hora',
        'SC': 'status_lancamento_1',
        'SG': 'status_global',
        'St.comp': 'status_compensacao',
        'St_comp': 'status_compensacao',
        'St.comp.': 'status_compensacao',
        'Status': 'status',
        'Mont.imp.': 'montante_imposto',
        'Tipo': 'tipo_documento'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df = df.replace('', pd.NA)
    df['data_faturamento'] = pd.to_datetime(df['data_faturamento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_criacao'] = pd.to_datetime(df['data_criacao'].str.replace('.', '/'), format='%d/%m/%Y')
    df['modificado_em'] = pd.to_datetime(df['modificado_em'].str.replace('.', '/'), format='%d/%m/%Y')
    df['documento_faturamento'] = df['documento_faturamento'].astype(str).str[:-2]
    df['organizacao_vendas'] = df['organizacao_vendas'].astype(str).str[:-2]
    df['canal_distribuicao'] = df['canal_distribuicao'].astype(str).str[:-2]
    df['numero_condicao_documento'] = df['numero_condicao_documento'].astype(str).str[:-2]
    df['pagador'] = df['pagador'].astype(str).str[:-2]
    df['atribuicao'] = df['atribuicao'].astype(str).str[:-2]

    # df = df[df['modificado_em'] >= five_days_ago]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformVBRP(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Doc.fat.': 'documento_faturamento',
        'TipFt': 'tipo_documento_faturamento',
        'CDst': 'canal_distribuicao',
        'DtPrestSer': 'data_prestacao_servico',
        'Doc.venda': 'documento_vendas',
        'Cond.doc.': 'numero_condicao_documento',
        'Dt.fatur.': 'data_faturamento',
        'Item': 'item',
        'Denominação de item': 'denominacao_item',
        'Denominacao_itens': 'denominacao_item',
        'Denominação it.': 'denominacao_item',
        'CtgI': 'categoria_item',
        'GCCM': 'grupo_class_cont_mat',
        'Val.líq.': 'valor_liquido',
        'Cen.lucro': 'centro_lucro',
        'Dev': 'devolucao',
        'Pagador': 'pagador',
        'Mat.inser.': 'material_inserido',
        'Mont.imp.': 'montante_imposto',
        'Ctg.doc.': 'categoria_documento_sd',
        'OrgV': 'organizacao_vendas',
        'Empr': 'empresa',
        'DF estorno': 'documento_faturamento_estornado',
        'Status': 'status'
    }

    df.rename(columns=columns_to_rename, inplace=True)
    df = df.replace('', pd.NA)
    df['data_faturamento'] = pd.to_datetime(df['data_faturamento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_prestacao_servico'] = pd.to_datetime(df['data_prestacao_servico'].str.replace('.', '/'), format='%d/%m/%Y')
    df['documento_vendas'] = df['documento_vendas'].astype(str)
    df['centro_lucro'] = df['centro_lucro'].astype(str).str[:-2]
    df['documento_faturamento'] = df['documento_faturamento'].astype(str).str[:-2]
    df['organizacao_vendas'] = df['organizacao_vendas'].astype(str).str[:-2]
    df['pagador'] = df['pagador'].astype(str).str[:-2]
    df['empresa'] = df['empresa'].astype(str).str[:-2]
    df['numero_condicao_documento'] = df['numero_condicao_documento'].astype(str).str[:-2]
    df['documento_faturamento_estornado'] = df['documento_faturamento_estornado'].astype(str).str[:-2]
    df['documento_vendas'] = df['documento_vendas'].astype(str).str[:-2]
    df['item'] = df['item'].astype(str).str[:-2]
    df['grupo_class_cont_mat'] = df['grupo_class_cont_mat'].astype(str).str[:-2]
    df['canal_distribuicao'] = df['canal_distribuicao'].astype(str).str[:-2]
    df['devolucao'] = df['devolucao'].replace({'X': True, '': False, pd.NA: False}).astype(bool)

    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformVBAP(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Criado em': 'data_criacao',
        'Criado por': 'criado_por',
        'Item': 'item',
        'Material': 'material',
        'Criado/a': 'criado_por',
        'GCCM': 'grupo_class_cont_mat',
        'Denominação it.': 'denominacao_item',
        'Denominação de item': 'denominacao_item',
        'CtgI': 'categoria_item',
        'Subtotal 1': 'subtotal_1',
        'Modif.em': 'modificado_em',
        'SA.1': 'setor_atividade',
        'St.recusa': 'status_recusa',
        'Rc': 'motivo_recusa',
        'BF': 'bloqueio_documento_faturamento',
        'Cen.': 'centro',
        'Dt.fatur.': 'data_faturamento',
        'Cen.lucro': 'centro_lucro',
        'Mont.imp.': 'montante_imposto',
        'Dt.estim.': 'data_pagamento_estimado',
        'Doc.venda': 'documento_vendas',
        'TpDV': 'tipo_documento_vendas',
        'BF': 'bloqueio_documento_faturamento',
        'Val.líq.': 'valor_liquido',
        'OrgV': 'organizacao_vendas',
        'SA': 'setor_atividade_1',
        'Cond.doc.': 'numero_condicao_documento',
        'Prb': 'probabilidade',
	'Referência do cliente': 'referencia_cliente',
        'Referência cliente': 'referencia_cliente',
        'Ref.cliente': 'referencia_cliente',
        'Emis.ordem': 'emissor_ordem',
        'Modif.por': 'modificado_por'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['modificado_em'] = pd.to_datetime(df['modificado_em'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_criacao'] = pd.to_datetime(df['data_criacao'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_pagamento_estimado'] = pd.to_datetime(df['data_pagamento_estimado'].str.replace('.', '/'), format='%d/%m/%Y')
    # df['data_faturamento'] = pd.to_datetime(df['data_faturamento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['documento_vendas'] = df['documento_vendas'].astype(str).str[:-2]
    df['item'] = df['item'].astype(str).str[:-2]
    df['numero_condicao_documento'] = df['numero_condicao_documento'].astype(str).str[:-2]
    df = df.replace('', pd.NA)
    # df = df[df['modificado_em'] >= five_days_ago]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df
    
def transformBKPF(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Empr': 'empresa',
        'Data doc.': 'data_documento',
        'Data lçto.': 'data_lancamento',
        'Dt.lçto.': 'data_lancamento',
        'Dt.entr.': 'data_entrada',
        'Período': 'periodo',
        'Entrado em': 'data_entrada',
        'Usuário': 'nome_usuario',
        'Chave referência': 'chave_referencia',
        'Chave refer.': 'chave_referencia',
        'Tx.câmbio3': 'taxa_cambio_3',
        'DEs': 'documento_estorno',
        'Est': 'estornado',
        'Tipo': 'tipo_documento',
        'Nº doc.': 'numero_documento',
        'Ano': 'exercicio'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    # df['data_documento'] = df['data_documento'].str.replace('0223', '2023')
    # df['data_documento'] = df['data_documento'].str.replace('0203', '2023')
    # df['data_documento'] = df['data_documento'].str.replace('022', '2023')
    # df['data_documento'] = df['data_documento'].str.replace('202', '2023') ARRUMAR DOCUMENTO ESTORNO PRA BOOL
    # df['data_documento'] = pd.to_datetime(df['data_documento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_lancamento'] = pd.to_datetime(df['data_lancamento'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_entrada'] = pd.to_datetime(df['data_entrada'].str.replace('.', '/'), format='%d/%m/%Y')
    df['estornado'] = df['estornado'].replace({'X': True, '': False}).astype(bool)
    df['empresa'] = df['empresa'].astype(str).str[:-2]
    df['numero_documento'] = df['numero_documento'].astype(str).str[:-2]
    df['exercicio'] = df['exercicio'].astype(str).str[:-2]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformBSEG(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python', dtype={'Chave refer.      ': str})
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'Dt.lçto.': 'data_lancamento',
        'Data lçto.': 'data_lancamento',
        'Data base': 'data_base_prazo_pagamento',
        'Empr': 'empresa',
        'Nº doc.': 'numero_documento',
        'Ano': 'exercicio',
        'Itm': 'item',
        'Compensaç.': 'data_compensacao_1',
        'DtCompens.': 'data_compensacao_2',
        'Dt.compen.': 'data_compensacao_2',
        'DocCompens': 'documento_compensacao',
        'CL': 'chave_lancamento',
        'TpCta': 'tipo_conta',
        'D/C': 'debito_credito',
        'MontMdFnc.': 'montante_moeda_funcional',
        'Dt.efetiva': 'data_efetiva',
        'Atribuição': 'atribuicao',
        'Texto': 'texto',
        'Montante': 'montante_alocado',
        'DtPlanej.': 'data_planejamento',
        'Dt.planej.': 'data_planejamento',
        'Cta.Razão': 'conta_razao_1',
        'CtaRz.': 'conta_razao_2',
        'Cliente': 'cliente',
        'Fornecedor': 'fornecedor',
        'CPgt': 'condicoes_pagamento',
        'Mont.MI 3': 'montante_mi_3',
        'Mont.MI3': 'montante_mi_3',
        'Anular compensação': 'anular_compensacao',
        'Cta.alt.': 'numero_conta_alternativa',
        'ACCr': 'area_controle_credito',
        'Ref.pgto.': 'referencia_pagamento',
        'ItCm': 'item_compensacao',
        'Chave referência': 'chave_referencia',
	'Chave ref.': 'chave_referencia',
        'Chave refer.': 'chave_referencia',
        'Tipo fluxo': 'tipo_fluxo'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    # df.drop(85069, inplace=True)
    df['data_compensacao_1'] = pd.to_datetime(df['data_compensacao_1'].str.replace('.', '/'), format='%d/%m/%Y')
    df['data_compensacao_2'] = pd.to_datetime(df['data_compensacao_2'].str.replace('.', '/'), format='%d/%m/%Y', errors='coerce')
    df['data_efetiva'] = pd.to_datetime(df['data_efetiva'].str.replace('.', '/'), format='%d/%m/%Y', errors='coerce')
    df['data_planejamento'] = pd.to_datetime(df['data_planejamento'].str.replace('.', '/'), format='%d/%m/%Y', errors='coerce')
    df['data_lancamento'] = pd.to_datetime(df['data_lancamento'].str.replace('.', '/'), format='%d/%m/%Y', errors='coerce')
    df['data_base_prazo_pagamento'] = pd.to_datetime(df['data_base_prazo_pagamento'].str.replace('.', '/'), format='%d/%m/%Y', errors='coerce')
    df['anular_compensacao'] = df['anular_compensacao'].replace({'X': True, '': False}).astype(bool)
    df['empresa'] = df['empresa'].astype(str).str[:-2]
    df['numero_documento'] = df['numero_documento'].astype(str).str[:-2]
    df['exercicio'] = df['exercicio'].astype(str).str[:-2]
    df['item'] = df['item'].astype(str).str[:-2]
    df['chave_lancamento'] = df['chave_lancamento'].astype(str).str[:-2]
    df['conta_razao_2'] = df['conta_razao_2'].astype(str).str[:-2]
    df['item_compensacao'] = df['item_compensacao'].astype(str).str[:-2]
    # arrumar notação cientifica
    # df['chave_referencia'] = df['chave_referencia'].astype(str).str[:-2]
    # df['montante_moeda_funcional'] = df['montante_moeda_funcional'].replace('.', '')
    # df['chave_referencia'] = df['chave_referencia'].astype(str)


    df.to_csv('df.csv')
    os.remove('data.txt')
    return df

def transformTVKO(data):
    df = pd.read_csv(data, delimiter="|", skiprows=3, skipfooter=1, encoding='iso-8859-1', encoding_errors='replace', on_bad_lines='warn', engine='python')
    df = df.iloc[1:]
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df.columns = df.columns.str.strip()
    df = trim_dataframe(df)
    columns_to_rename = {
        'OrgV': 'organizacao_vendas',
        'Moeda': 'moeda',
        'Empr': 'empresa',
        'Denominação': 'denominacao',
        'Endereço': 'endereco',
        'Texto endereço': 'texto_endereco',
        'OrgVndRef': 'organizacao_vendas_refencia_tipo_documento',
        'Ctg': 'categoria_documento_compras'
    }
    df.rename(columns=columns_to_rename, inplace=True)
    df['organizacao_vendas'] = df['organizacao_vendas'].astype(str).str[:-2]
    df['empresa'] = df['empresa'].astype(str).str[:-2]
    df['endereco'] = df['endereco'].astype(str).str[:-2]
    df['organizacao_vendas_refencia_tipo_documento'] = df['organizacao_vendas_refencia_tipo_documento'].astype(str).str[:-2]
    df.to_csv('df.csv')
    os.remove('data.txt')
    return df
