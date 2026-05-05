"""
Extracao de Dados OCDE - SDMX 2.1
Indicadores estruturais do setor de software (J62+J63) para Market Twin / Predicao
"""

import requests
import pandas as pd
from io import StringIO
import logging
import time

# Configuracao de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Constantes
BASE_URL = "https://sdmx.oecd.org/public/rest/data/"
DATAFLOW = "OECD.SDD.TPS,DSD_SDBSBSC_ISIC4@DF_SDBS_ISIC4"

COLUNAS_SDMX = ["REF_AREA", "MEASURE", "ACTIVITY", "TIME_PERIOD", "OBS_VALUE"]
COLUNAS_RENAME = ["Pais", "Indicador", "Setor", "Ano", "Valor"]

# Indicadores com cobertura util para J62+J63 (verificado empiricamente):
#   ENTR  - numero de empresas ativas           (cobertura: ~97%)
#   TUTT  - turnover total (faturamento)        (cobertura: ~95%)
#   EMPE  - numero de empregados                (cobertura: ~91%)
#
# Indicadores testados e descartados por cobertura insuficiente em J62+J63:
#   VALU  - valor adicionado                    (cobertura:  ~8%)
#   EMPT, WAGE, GROS, INVE                      (cobertura:   0%, API nao retorna dados)
MEDIDAS_PADRAO = "ENTR+TUTT+EMPE"

TIMEOUT_SEGUNDOS = 180
MAX_TENTATIVAS = 3
PAUSA_TENTATIVA = 10


# Funcoes auxiliares


def _construir_url(
    paises: str = "",
    medidas: str = MEDIDAS_PADRAO,
    setores: str = "J62+J63",
    inicio: int = 2010,
    fim: int = 2024,
) -> str:
    """
    Monta a URL SDMX 2.1 parametrizada.
    Deixar `paises` vazio retorna todos os paises da OCDE + parceiros.
    """
    chave = f"A.{paises}.{medidas}.{setores}._T."
    params = (
        f"?startPeriod={inicio}&endPeriod={fim}&dimensionAtObservation=AllDimensions"
    )
    return f"{BASE_URL}{DATAFLOW}/{chave}{params}"


def _requisitar_com_retry(url: str, headers: dict) -> requests.Response | None:
    """
    Faz a requisicao HTTP com retry automatico em caso de falha de rede ou 5xx.
    Retorna o objeto Response em caso de sucesso, ou None apos esgotar tentativas.
    """
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        try:
            log.info(f"Tentativa {tentativa}/{MAX_TENTATIVAS} -- requisitando dados...")
            response = requests.get(url, headers=headers, timeout=TIMEOUT_SEGUNDOS)

            if response.status_code == 200:
                log.info(f"Resposta recebida ({len(response.content) / 1024:.1f} KB)")
                return response

            # Erros do cliente (4xx) nao adianta tentar de novo
            if 400 <= response.status_code < 500:
                log.error(
                    f"Erro do cliente {response.status_code}: {response.text[:500]}"
                )
                return None

            # Erros do servidor (5xx): tenta de novo
            log.warning(
                f"Erro do servidor {response.status_code}. Aguardando {PAUSA_TENTATIVA}s..."
            )
            time.sleep(PAUSA_TENTATIVA)

        except requests.exceptions.Timeout:
            log.warning(
                f"Timeout apos {TIMEOUT_SEGUNDOS}s. Aguardando {PAUSA_TENTATIVA}s..."
            )
            time.sleep(PAUSA_TENTATIVA)

        except requests.exceptions.RequestException as e:
            log.error(f"Erro de rede: {e}")
            return None

    log.error("Todas as tentativas falharam.")
    return None


def _validar_e_limpar(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Valida colunas esperadas e retorna DataFrame limpo com nomes legiveis.

    NaN em OBS_VALUE sao MANTIDOS intencionalmente -- representam anos sem
    reporte para um pais/indicador especifico. O tratamento (interpolacao,
    exclusao por cobertura, etc.) sera feito em etapa posterior, conforme
    o uso: snapshot para twins ou serie temporal para predicao.
    """
    colunas_ausentes = [c for c in COLUNAS_SDMX if c not in df.columns]
    if colunas_ausentes:
        log.error(f"Colunas ausentes na resposta da API: {colunas_ausentes}")
        log.info(f"Colunas disponiveis: {df.columns.tolist()}")
        return None

    df_clean = df[COLUNAS_SDMX].copy()
    df_clean.columns = COLUNAS_RENAME

    df_clean["Ano"] = pd.to_numeric(df_clean["Ano"], errors="coerce")
    df_clean["Valor"] = pd.to_numeric(df_clean["Valor"], errors="coerce")

    # Diagnostico de NaN -- informativo, sem remocao
    n_nulos = df_clean["Valor"].isna().sum()
    if n_nulos > 0:
        pct = n_nulos / len(df_clean) * 100
        log.info(
            f"Valores ausentes (OBS_VALUE): {n_nulos:,} linhas ({pct:.1f}%) "
            f"-- mantidos para tratamento posterior."
        )

    return df_clean


def _pivotar(df_clean: pd.DataFrame, aggfunc: str = "last") -> pd.DataFrame:
    """
    Transforma de formato longo (long) para largo (wide), um indicador por coluna.

    aggfunc='last' prioriza a revisao mais recente em caso de duplicatas,
    comportamento mais seguro para series da OCDE que sofrem revisoes.

    NaN no wide refletem ausencia real de reporte -- nao sao artefatos do pivot.
    """
    df_pivot = df_clean.pivot_table(
        index=["Pais", "Setor", "Ano"],
        columns="Indicador",
        values="Valor",
        aggfunc=aggfunc,
    ).reset_index()

    df_pivot.columns.name = None
    return df_pivot


def diagnostico_cobertura(df: pd.DataFrame) -> None:
    """
    Imprime dois relatorios de cobertura de dados:

    1. Por indicador: % de linhas (pais+ano) com valor nao-nulo.
       Use para decidir quais indicadores tem dados suficientes
       para o modelo (recomendado: > 60%).

    2. Por pais (indicador ENTR): % de anos com valor nao-nulo.
       Use para excluir paises com serie muito incompleta.
    """
    indicadores = [c for c in df.columns if c not in ["Pais", "Setor", "Ano"]]

    cobertura_ind = (
        df[indicadores]
        .notna()
        .mean()
        .sort_values(ascending=False)
        .mul(100)
        .round(1)
        .rename("Cobertura (%)")
    )
    print("\n--- Cobertura por indicador (% de linhas pais+ano com valor) ---")
    print(cobertura_ind.to_string())

    if "ENTR" in df.columns:
        cobertura_pais = (
            df.groupby("Pais")["ENTR"]
            .apply(lambda s: s.notna().mean())
            .sort_values()
            .mul(100)
            .round(1)
        )
        print("\n--- Cobertura por pais -- ENTR (% de anos com valor) ---")
        print(cobertura_pais.to_string())


# Funcao principal


def extrair_dados_ocde(
    paises: str = "",
    medidas: str = MEDIDAS_PADRAO,
    setores: str = "J62+J63",
    inicio: int = 2010,
    fim: int = 2024,
    formato_wide: bool = True,
) -> pd.DataFrame | None:
    """
    Extrai dados estruturais do setor de software da API SDMX 2.1 da OCDE.

    Parametros
    ----------
    paises       : Codigos ISO separados por '+'. Vazio = todos os paises.
    medidas      : Indicadores SDMX separados por '+'. Ver MEDIDAS_PADRAO.
    setores      : Atividades ISIC4. J62 = software, J63 = dados/hospedagem.
    inicio / fim : Intervalo temporal (ano).
    formato_wide : Se True, pivota indicadores em colunas (ideal para ML/twins).
                   Se False, retorna formato longo (ideal para visualizacao).

    Retorna
    -------
    pd.DataFrame ou None em caso de falha.
    NaN em Valor indicam ausencia de reporte -- nao foram removidos.
    """
    url = _construir_url(paises, medidas, setores, inicio, fim)
    headers = {"Accept": "text/csv"}

    log.info(f"URL construida:\n  {url}")

    response = _requisitar_com_retry(url, headers)
    if response is None:
        return None

    try:
        df_raw = pd.read_csv(StringIO(response.text))
    except Exception as e:
        log.error(f"Falha ao parsear CSV: {e}")
        log.debug(response.text[:1000])
        return None

    log.info(f"Linhas brutas lidas: {len(df_raw):,}")

    df_clean = _validar_e_limpar(df_raw)
    if df_clean is None:
        return None

    log.info(f"Linhas apos limpeza: {len(df_clean):,}")
    log.info(f"Paises encontrados: {df_clean['Pais'].nunique()}")
    log.info(f"Indicadores: {df_clean['Indicador'].unique().tolist()}")

    if not formato_wide:
        return df_clean

    df_final = _pivotar(df_clean)
    log.info(f"Shape final (wide): {df_final.shape}")
    return df_final


# Execucao

if __name__ == "__main__":
    log.info("=== Extracao: todos os paises, formato wide ===")
    df = extrair_dados_ocde(inicio=2010, fim=2022, formato_wide=True)

    if df is not None:
        print("\n--- Primeiras linhas ---")
        print(df.head(10).to_string(index=False))

        print(f"\n--- Shape: {df.shape} ---")
        print(f"Paises: {df['Pais'].nunique()}")
        print(f"Colunas: {df.columns.tolist()}")

        # Relatorio de cobertura -- base para decidir quais indicadores
        # e paises entram na analise de twins / predicao
        diagnostico_cobertura(df)

        df.to_csv("dados_ocde_software.csv", index=False)
        log.info("Arquivo salvo: dados_ocde_software.csv")

    # Exemplo: apenas 3 paises, formato longo (util para visualizacao)
    # df_long = extrair_dados_ocde(paises="FRA+DEU+ITA", formato_wide=False)
    # if df_long is not None:
    #     print(df_long.head(20))
