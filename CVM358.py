"""
Autor: Gustavo Satler Soares
Data de Criação: 14/01/2025
Descrição: Este script faz o processamento de dados da CVM, especificamente Valores Mobiliarios Negociados e Detidos de administradores e pessoas ligadas a cias abertas.
Contato: gustavosatler8@gmail.com
Licença: Todos os direitos reservados
"""

import os
import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot
import requests
import zipfile
from datetime import datetime

def download_and_cleanup():
    # Obter a data atual
    data_atual = datetime.now().date()

    # Lista de arquivos a serem baixados
    cvmzip_list = [
        f'vlmo_cia_aberta_{y}.zip' for y in range(2018, datetime.now().year + 1)
    ]

    # URL base
    base_url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/VLMO/DADOS/'

    # Criar o diretório "Arquivos" se não existir
    pasta_arquivos = "CVM358/Arquivos"
    os.makedirs(pasta_arquivos, exist_ok=True)

    for cvmzip in cvmzip_list:
        caminho_zip = os.path.join(pasta_arquivos, cvmzip)

        # Verificar se o arquivo já existe e foi modificado hoje
        if os.path.exists(caminho_zip):
            data_modificacao = datetime.fromtimestamp(os.path.getmtime(caminho_zip)).date()
            if data_modificacao == data_atual:
                print(f"Arquivo {cvmzip} já existe e foi atualizado hoje. Pulando download.")
                continue

        try:
            # Fazer o download do arquivo
            response = requests.get(base_url + cvmzip, stream=True)
            if response.status_code == 200:
                with open(caminho_zip, 'wb') as fp:
                    fp.write(response.content)

                # Extrair o arquivo .zip
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    zip_ref.extractall(pasta_arquivos)

                # Remover o arquivo .zip
                if os.path.exists(caminho_zip):
                    os.remove(caminho_zip)

                # Remover os arquivos extraídos que começam com "vlmo_cia_aberta_yyyy"
                arquivos_extraidos = [
                    f for f in os.listdir(pasta_arquivos) if f.startswith(cvmzip.replace('.zip', ''))
                ]
                for file in arquivos_extraidos:
                    os.remove(os.path.join(pasta_arquivos, file))
            else:
                print(f"Erro ao baixar {cvmzip}: Status {response.status_code}")
        except Exception as e:
            print(f"Erro ao processar {cvmzip}: {e}")

def carregar_dados():
    # Diretório do script
    arquivos_csv = [os.path.join("CVM358/Arquivos", f) for f in os.listdir("CVM358/Arquivos") if f.startswith('vlmo_cia_aberta_con_') and f.endswith('.csv')]
    
    if not arquivos_csv:
        print("Nenhum arquivo CSV encontrado.")
        return pd.DataFrame(), []
    
    df_consolidado = pd.DataFrame()
    for arquivo in arquivos_csv:
        try:
            df = pd.read_csv(arquivo, delimiter=';', encoding='latin1')
            df_consolidado = pd.concat([df_consolidado, df], ignore_index=True)
        except Exception as e:
            print(f"Erro ao carregar {arquivo}: {e}")
    
    companhias = sorted(df_consolidado['Nome_Companhia'].dropna().unique())
    return df_consolidado, companhias

def calcular_balanco(df, nome_companhia, empresa, tipo_cargo): 
    # Filtrar os dados com base na companhia, empresa e cargo
    filtro = (
        (df['Nome_Companhia'] == nome_companhia) &
        (df['Empresa'] == empresa) &
        (df['Tipo_Cargo'] == tipo_cargo)
    )
    df_filtrado = df[filtro]
    
    if df_filtrado.empty:
        print("Erro ao criar filtro de balanço. Nenhum dado encontrado para os critérios fornecidos.")
        return None

    # Identificar os valores únicos na coluna Caracteristica_Valor_Mobiliario
    valores_unicos = df_filtrado['Caracteristica_Valor_Mobiliario'].dropna().unique()
    
    if len(valores_unicos) == 1 and 'ON' in valores_unicos:
        # Se apenas "ON" está presente, usar "ON" para o cálculo
        caracteristica = 'ON'
        print("Somente valores 'ON' encontrados. O cálculo será baseado em acoes ordinarias.")
    elif set(valores_unicos) == {'ON', 'PN'}:
        # Se ambos "ON" e "PN" estão presentes, solicitar decisão do usuário
        while True:
            escolha = input("Acoes ordinarias e preferenciais estão presentes. Deseja calcular o balanço com base em 'ON' ou 'PN'? ").strip().upper()
            if escolha in {'ON', 'PN'}:
                caracteristica = escolha
                print(f"O cálculo será baseado em '{caracteristica}'.")
                break
            else:
                print("Entrada inválida. Por favor, escolha entre 'ON' ou 'PN'.")
    else:
        print(f"Valores inesperados encontrados na coluna Caracteristica_Valor_Mobiliario: {valores_unicos}")
        return None

    # Converter a coluna Data_Referencia para datetime e criar a coluna Mes_Ano
    df_filtrado['Data_Referencia'] = pd.to_datetime(df_filtrado['Data_Referencia'], errors='coerce')
    df_filtrado['Mes_Ano'] = df_filtrado['Data_Referencia'].dt.to_period('M')

    # Calcular o saldo inicial
    saldo = df_filtrado[
        (df_filtrado['Tipo_Movimentacao'] == 'Saldo Inicial') &
        (df_filtrado['Tipo_Operacao'] == 'Crédito') &
        (df_filtrado['Caracteristica_Valor_Mobiliario'] == caracteristica)
    ].groupby('Mes_Ano')['Quantidade'].sum()

    # Calcular as vendas
    vendas = df_filtrado[
        (df_filtrado['Tipo_Movimentacao'] != 'Saldo Inicial') &
        (df_filtrado['Tipo_Operacao'].isin(['Débito', 'Crédito'])) &#== 'Débito' or 'Crédito') & #mudanca
        (df_filtrado['Caracteristica_Valor_Mobiliario'] == caracteristica)
    ].groupby('Mes_Ano')['Quantidade'].sum()

    # Calcular o balanço
    balanco = saldo.subtract(vendas, fill_value=0)
    balanco.index = balanco.index.to_timestamp()

    return balanco

def gerar_grafico(balanco, nome_companhia, empresa, tipo_cargo):
    if balanco is None:
        print("Nenhum dado encontrado com os critérios fornecidos.")
        return
    
    # Criação do gráfico
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=balanco.index, y=balanco.values, mode='lines+markers', name='Balanço'))

    fig.update_layout(
        title=f'Balanço Mensal ({tipo_cargo}) - {nome_companhia} / {empresa}',
        xaxis_title='Mês de Referência',
        yaxis_title='Balanço (Quantidade)',
        template='ggplot2',
    )
    
    # Exibir o gráfico diretamente em uma janela interativa
    plot(fig, auto_open=True)

def main():
    # Executa a função
    download_and_cleanup()

    # Carregar dados
    df, companhias = carregar_dados()

    if df.empty:
        print("Erro ao carregar dados")
        return

    # Lista fixa de cargos possíveis
    CARGOS_POSIVEIS = [
        'Diretor ou Vinculado',
        'Conselho de Administração ou Vinculado',
        'Órgão Estatutário ou Vinculado',
        'Controlador ou Vinculado',
        'Conselho Fiscal ou Vinculado'
    ]

    # Escolher companhia, empresa e cargo
    print("Escolha a Companhia:")
    for idx, companhia in enumerate(companhias):
        print(f"{idx + 1}. {companhia}")
    nome_companhia = companhias[int(input("Digite o número da companhia: ")) - 1]

    empresas_disponiveis = sorted(df[df['Nome_Companhia'] == nome_companhia]['Empresa'].dropna().unique())
    if len(empresas_disponiveis) == 1:
        print(f"Dados para {empresas_disponiveis[0]}:")
        empresa_index = 0
    else:
        print(f"Escolha a Empresa para {nome_companhia}:")
        for idx, empresa in enumerate(empresas_disponiveis):
            print(f"{idx + 1}. {empresa}")
        empresa_index = int(input("Digite o número da empresa: ")) - 1
    empresa = empresas_disponiveis[empresa_index]

    print("Escolha o Cargo:")
    for idx, cargo in enumerate(CARGOS_POSIVEIS):
        print(f"{idx + 1}. {cargo}")
    tipo_cargo = CARGOS_POSIVEIS[int(input("Digite o número do cargo: ")) - 1]

    # Calcular e gerar gráfico
    gerar_grafico(calcular_balanco(df, nome_companhia, empresa, tipo_cargo),
                  nome_companhia, 
                  empresa, 
                  tipo_cargo)

if __name__ == "__main__":
    main()
