import requests
import pandas as pd

# ==============================================================================
# PASSO 1: Obter Metadados dos Países
# ==============================================================================
print("1. Buscando metadados dos países (Regiões e Grupos)...")
url_metadata = "https://api.worldbank.org/v2/country?format=json&per_page=1000"
response_meta = requests.get(url_metadata)

metadata_list = []
if response_meta.status_code == 200:
    dados_paises = response_meta.json()[1]
    for p in dados_paises:
        if p["region"]["id"] != "NA":  # Remove os agregados (Mundo Árabe, etc)
            metadata_list.append(
                {
                    "Country_Code": p["id"],
                    "Region": p["region"]["value"],
                    "Income_Group": p["incomeLevel"]["value"],
                }
            )

df_metadata = pd.DataFrame(metadata_list)
print(f" -> {len(df_metadata)} países reais encontrados.")

# ==============================================================================
# PASSO 2: Definir Indicadores
# ==============================================================================
indicadores = {
    "IT.NET.USER.ZS": "Internet_Usage_Pct",
    "NY.GDP.PCAP.PP.CD": "GDP_Per_Capita_PPP",
    "IT.NET.BBND.P2": "Broadband_Subscriptions",
    "NY.GDP.MKTP.CD": "GDP_USD",
    "IT.NET.SECR.P6": "Secure_Servers",  # <-- Ajustado para o código oficial do Banco Mundial
}

df_combinado = None

# ==============================================================================
# PASSO 3: Buscar Dados dos Indicadores
# ==============================================================================
print("\n2. Buscando dados dos indicadores...")
for cod, nome in indicadores.items():
    print(f" -> Baixando: {nome} ({cod})...")
    # Usando 'country' no singular
    url = f"https://api.worldbank.org/v2/country/all/indicator/{cod}?format=json&date=2010:2024&per_page=5000"

    res = requests.get(url)

    if res.status_code == 200:
        json_data = res.json()

        # Verifica se o Banco Mundial retornou erro na estrutura do JSON
        if (
            len(json_data) > 0
            and isinstance(json_data[0], dict)
            and "message" in json_data[0]
        ):
            print(
                f"    [ERRO DA API] O Banco Mundial rejeitou o indicador {cod}. Mensagem: {json_data[0]['message'][0]['value']}"
            )
            continue  # Pula para o próximo indicador

        elif len(json_data) > 1 and isinstance(json_data[1], list):
            df_temp = pd.DataFrame(
                [
                    {
                        "Country_Name": item["country"]["value"],
                        "Country_Code": item["countryiso3code"],
                        "Year": item["date"],
                        nome: item["value"],
                    }
                    for item in json_data[1]
                ]
            )

            # Se for o primeiro a dar certo, ele vira o df principal
            if df_combinado is None:
                df_combinado = df_temp
            else:
                # Une com os dados anteriores
                df_combinado = pd.merge(
                    df_combinado,
                    df_temp,
                    on=["Country_Name", "Country_Code", "Year"],
                    how="outer",
                )
            print(f"    [OK] Dados de {nome} adicionados com sucesso!")
        else:
            print(f"    [ERRO] Estrutura não reconhecida para {cod}.")
    else:
        print(f"    [ERRO HTTP] Falha ao conectar: {res.status_code}")

# ==============================================================================
# PASSO 4: Unir Metadados (Região) e Salvar
# ==============================================================================
print("\n3. Processando e salvando arquivo...")
if df_combinado is not None and not df_combinado.empty:
    df_final = pd.merge(df_metadata, df_combinado, on="Country_Code", how="inner")

    # Criar a lista de colunas apenas com os indicadores que realmente funcionaram
    colunas_presentes = [ind for ind in indicadores.values() if ind in df_final.columns]
    colunas_ordem = [
        "Country_Name",
        "Country_Code",
        "Region",
        "Income_Group",
        "Year",
    ] + colunas_presentes
    df_final = df_final[colunas_ordem]

    df_final = df_final.sort_values(by=["Region", "Country_Name", "Year"])

    nome_arquivo = "dados_paises_com_regiao.csv"
    df_final.to_csv(nome_arquivo, index=False, encoding="utf-8")
    print(f"\n✅ SUCESSO! Arquivo '{nome_arquivo}' gerado!")
else:
    print(
        "\n❌ FALHA: Nenhum dado válido foi retornado da API. Verifique sua conexão ou se a API está fora do ar."
    )
