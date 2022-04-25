import sys
import glob
import os
import sys
import time
from typing import Tuple
import pymysql

"""
MANTER HISTORICO
"""

rotina = 'corporate_action'

colunas_tabela = [
    'RptDt',
    'CorpActnId',
    'CorpActnCtrlNb',
    'EnetPrtcol',
    'CrpnNm',
    'NtceDt',
    'NtceTitle',
    'ISIN',
    'TckrSymb',
    'SpcfctnCd',
    'CorpActnEvtTpCd',
    'CorpActnDesc',
    'RefDt',
    'SpclExDt',
    'PmtDt',
    'CcySymb',
    'EvtVal',
    'NetVal',
    'Note',
    'Lk',
    'EvtSts'
]

# lista todos os arquivos da pasta
path = '/Corporate_Action/Schedule/20201216/'
arquivos = os.listdir(path)


# print(arquivos)
# encontra os arquivos que contém EOD no nome
aux = [linha for linha in arquivos if 'EOD' in linha]

# abre o arquivo
teste = []
for arquivo in aux:
    leitor = open(path + arquivo, 'r')
    # print(arquivo)
    # ignora a primeira linha
    info = leitor.readlines()[1:]

    for dados in info:
        # adiciona quebra de linha ao encontrar ;
        valores = dados.split(';')
        str(valores)[1:-1]
        map(repr, valores)
        valores = tuple(valores)
        teste.append(valores)
        print(teste)

# aux.close()

try:
    con = pymysql.connect(host='teste',
                          database='teste',
                          user='teste',
                          password='teste')
except Exception as err:
    print('erro conexão')

with con.cursor() as cursor:
    querys = ''
    for i in range(len(colunas_tabela)):
        querys += '%s,'
    query = 'REPLACE INTO corporate_action_copy (' + (
        ','.join(colunas_tabela)) + ') VALUES (' + querys[:-1] + ")"
    try:

        cursor.executemany(query, teste)
        con.commit()
        print(cursor.rowcount, 'registro(s) inserido(s)')
        # print(query)
    except Exception as err:
        print('Falha ao inserir registros')
        # print(query)
        print(err)
