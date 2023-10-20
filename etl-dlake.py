import pytz
import datetime

# Dados simulados da fonte
vendas = [
    {"data": "2023-10-20 09:00:00", "produto": "Maçã", "cliente": "João", "quantidade": 5, "preco_total": 10.0},
    {"data": "2023-10-20 14:30:00", "produto": "Pão", "cliente": "Maria", "quantidade": 2, "preco_total": 2.0},
]

# Simulação de tabelas de dimensão no Data Lake
dim_produto = {}
dim_cliente = {}
dim_data = {}

# Simulação da tabela de fato no Data Lake
fato_vendas = []

# Função para gerar chaves surrogadas
def gerar_chave_surrogada(dic):
    if all(isinstance(key, int) for key in dic.keys()):
        return max(dic.keys(), default=0) + 1
    else:
        return len(dic) + 1

# Função para registrar logs
def registrar_log(mensagem):
    timestamp = datetime.datetime.now()
    with open("logs.txt", "a", encoding="utf-8") as log_file:
        log_file.write(f"[{str(timestamp)}] {mensagem}\n")

# Função para enviar notificações
def enviar_notificacao(mensagem):
    print(f"NOTIFICAÇÃO: {mensagem}")

# Função para converter data de UTC-4 para UTC
def converter_data_para_utc(data_string):
    timezone_utc_4 = pytz.timezone('Etc/GMT+4')
    data_utc_4 = datetime.datetime.strptime(data_string, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone_utc_4)
    data_utc = data_utc_4.astimezone(pytz.utc)
    return data_utc.strftime('%Y-%m-%d %H:%M:%S')

# ETL
try:
    for venda in vendas:
        # Converter data de UTC-4 para UTC
        venda["data"] = converter_data_para_utc(venda["data"])

        # Extração e Transformação
        chave_produto = dim_produto.get(venda["produto"], gerar_chave_surrogada(dim_produto))
        dim_produto[venda["produto"]] = chave_produto

        chave_cliente = dim_cliente.get(venda["cliente"], gerar_chave_surrogada(dim_cliente))
        dim_cliente[venda["cliente"]] = chave_cliente

        chave_data = dim_data.get(venda["data"], gerar_chave_surrogada(dim_data))
        dim_data[venda["data"]] = chave_data

        # Carga
        fato_vendas.append({
            "chave_produto": chave_produto,
            "chave_cliente": chave_cliente,
            "chave_data": chave_data,
            "quantidade": venda["quantidade"],
            "preco_total": venda["preco_total"]
        })

    registrar_log("ETL concluído com sucesso.")
    enviar_notificacao("ETL concluído com sucesso.")
except Exception as e:
    registrar_log(f"Erro no ETL: {e}")
    enviar_notificacao(f"Erro no ETL: {e}")

# Resultados
dimensao_produto = dim_produto
dimensao_cliente = dim_cliente
dimensao_data = dim_data
fato_vendas_result = fato_vendas

dimensao_produto, dimensao_cliente, dimensao_data, fato_vendas_result
