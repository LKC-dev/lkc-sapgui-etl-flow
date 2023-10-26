import time
import os
from sapgui_flow.ETL.extract import *
from sapgui_flow.ETL.transform import *
from sapgui_flow.ETL.load import *
from sapgui_flow.utils.aws_utils import pushToDataLake
from retry import retry
import psutil
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("my_logger")

# table list
# KNA1
# KNVV
# J_1BNFDOC
# J_1BNFLIN
# VBRK
# VBRP
# VBAK
# VBAP
# BKPF
# BSEG

def run():

    def check_delete_buffer():
        if os.path.exists('data.txt'):
            os.remove('data.txt')
            logger.info('Deleted old file data.txt')
        else:
            pass
    start_time = time.time()

    check_delete_buffer()

    session = open_sap()


    data = export_data('KNA1', session)
    logger.info('KNA1 - Started transformation')
    data = transformKNA1(data)
    logger.info('KNA1 - Finished transformation')
    load_data(data,'clientes_kna1', 'sap', 'id_cliente')
    pushToDataLake("sap/clientes-kna1", "clientes-kna1.csv", data)

    data = export_data('KNVV', session)
    logger.info('KNA1 - Started transformation')
    data = transformKNVV(data)
    logger.info('KNA1 - Finished transformation')
    load_data(data,'clientes_knvv', 'sap', 'id_cliente')
    pushToDataLake("sap/clientes-knvv", "clientes-knvv.csv", data)

    data = export_data('J_1BNFDOC', session)
    logger.info('J_1BNFDOC - Started transformation')
    data = transformJ_1BNFDOC(data)
    logger.info('J_1BNFDOC - Finished transformation')
    load_data(data,'notas_fiscais_j1bnfdoc', 'sap', 'numero_documento')
    pushToDataLake("sap/notas-fiscais-j1bnfdoc", "notas-fiscais-j1bnfdoc.csv", data)

    data = export_data('J_1BNFLIN', session)
    logger.info('J_1BNFLIN - Started transformation')
    data = transformJ_1BNFLIN(data)
    logger.info('J_1BNFLIN - Finished transformation')
    load_data(data,'notas_fiscais_j1bnflin', 'sap', 'numero_documento')
    pushToDataLake("sap/notas-fiscais-j1bnflin", "notas-fiscais-j1bnflin.csv", data)

    data = export_data('VBRK', session)
    logger.info('VBRK - Started transformation')
    data = transformVBRK(data)
    logger.info('VBRK - Finished transformation')
    load_data(data,'faturamento_vbrk', 'sap', 'documento_faturamento')
    pushToDataLake("sap/faturamento-vbrk", "faturamento-vbrk.csv", data)

    data = export_data('VBRP', session)
    logger.info('VBRP - Started transformation')
    data = transformVBRP(data)
    logger.info('VBRP - Finished transformation')
    load_data(data,'faturamento_vbrp', 'sap', 'documento_faturamento')
    pushToDataLake("sap/faturamento-vbrp", "faturamento-vbrp.csv", data)

    data = export_data('VBAK', session)
    logger.info('VBAK - Started transformation')
    data = transformVBAK(data)
    logger.info('VBAK - Finished transformation')
    load_data(data,'ordens_venda_vbak', 'sap', 'documento_vendas')
    pushToDataLake("sap/ordens-venda-vbak", "ordens-venda-vbak.csv", data)

    data = export_data('VBAP', session)
    logger.info('VBAP - Started transformation')
    data = transformVBAP(data)
    logger.info('VBAP - Finished transformation')
    load_data(data,'ordens_venda_vbap', 'sap', 'documento_vendas')
    pushToDataLake("sap/ordens-venda-vbap", "ordens-venda-vbap.csv", data)

    data = export_data('BKPF', session)
    logger.info('BKPF - Started transformation')
    data = transformBKPF(data)
    logger.info('BKPF - Finished transformation')
    load_data(data,'documentos_contabeis_bkpf', 'sap', 'numero_documento')
    pushToDataLake("sap/documentos-contabeis-bkpf", "documentos-contabeis-bkpf.csv", data)

    data = export_data('BSEG', session)
    logger.info('BSEG - Started transformation')
    data = transformBSEG(data)
    logger.info('BSEG - Finished transformation')
    load_data(data,'documentos_contabeis_bseg', 'sap', 'numero_documento')
    pushToDataLake("sap/documentos-contabeis-bseg", "documentos-contabeis-bseg.csv", data)

    check_delete_buffer()

    end_time = time.time()
    execution_time = end_time - start_time

    memory_usage_bytes = psutil.Process(os.getpid()).memory_info().rss
    memory_usage_megabytes = memory_usage_bytes / 1024 / 1024  # Convert bytes to megabytes
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in the last second

    print("Execution Time (in minutes):", execution_time / 60)
    print("Memory Usage (in megabytes):", memory_usage_megabytes)
    print("CPU Usage (in %):", cpu_usage)
    logger.info(f'Finished run - closed SAPGUI')


run()
