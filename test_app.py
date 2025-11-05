import pytest
import pandas as pd
import duckdb
from datetime import datetime
from pandas.testing import assert_frame_equal, assert_series_equal

from src.processing.calculations import map_and_calculate_valores
from src.processing.filters import filter_contestacoes
from src.processing.mdfe_checker import check_mdfe_status_duckdb


def test_map_and_calculate_valores():
    """
    Testa a função de cálculo de valores (unitário).
    Verifica se o Valor Correto e o Valor Estorno estão corretos
    e se o mapeamento de categoria (61 -> 7) funciona.
    """
    data = {
        'VALOR': ['100,00', '50,00'],
        'CATEG': [61, 10],
        'Quantidade Eixos Vazio': [7, 2]
    }
    input_df = pd.DataFrame(data)

    expected_data = {
        'VALOR': [100.0, 50.0],
        'CATEG': [7, 10],
        'Quantidade Eixos Vazio': [7, 2],
        'Valor por eixo': [100.0 / 7, 5.0],
        'Valor Correto': [100.00, 10.00],
        'Valor Estorno': [0.00, 40.00]
    }
    expected_df = pd.DataFrame(expected_data)[
        ['Valor Correto', 'Valor Estorno', 'CATEG']
    ]
    result_df = map_and_calculate_valores(input_df)

    pd.testing.assert_series_equal(
        result_df['Valor Correto'],
        expected_df['Valor Correto'],
        check_dtype=False
    )
    pd.testing.assert_series_equal(
        result_df['Valor Estorno'],
        expected_df['Valor Estorno'],
        check_dtype=False
    )
    pd.testing.assert_series_equal(
        result_df['CATEG'],
        expected_df['CATEG'],
        check_dtype=False
    )


def test_filter_contestacoes():
    """
    Testa a função de filtro (unitário).
    Verifica se apenas as linhas que atendem a TODOS os critérios
    são selecionadas.
    """
    data = {
        'mdfe_aberto_Atua': [False, True, False, False],
        'Quantidade Eixos Vazio': [2, 2, 0, 5],
        'CATEG': [5, 5, 5, 5],
    }
    input_df = pd.DataFrame(data)

    result_df = filter_contestacoes(input_df)
    assert len(result_df) == 1
    assert result_df.iloc[0]['Quantidade Eixos Vazio'] == 2


@pytest.fixture(scope="module")
def duckdb_conn():
    """Cria uma conexão DuckDB em memória para os testes."""
    return duckdb.connect(':memory:')


@pytest.fixture
def setup_duckdb_data(duckdb_conn):
    """Carrega dados de manifesto no DuckDB para o teste."""
    manifestos_data = {
        'placa': ['ABC1234', 'ABC1234'],
        'datahora': [datetime(2023, 1, 5), datetime(2023, 1, 15)],
        'updated_at': [datetime(2023, 1, 10), None]
    }
    manifestos_df = pd.DataFrame(manifestos_data)

    duckdb_conn.execute("CREATE OR REPLACE TABLE manifestos AS SELECT * FROM manifestos_df")
    yield
    duckdb_conn.execute("DROP TABLE manifestos")


def test_check_mdfe_status_duckdb(duckdb_conn, setup_duckdb_data):
    """
    Testa a lógica complexa de verificação de MDF-e no DuckDB (integração).
    """
    pedagios_data = {
        'PLACA': ['ABC1234', 'ABC1234', 'ABC1234', 'ABC1234', 'XYZ9999'],
        'Data Passagem': [
            datetime(2023, 1, 2),
            datetime(2023, 1, 7),
            datetime(2023, 1, 12),
            datetime(2023, 1, 17),
            datetime(2023, 1, 7),
        ]
    }
    pedagios_df = pd.DataFrame(pedagios_data)

    expected_mdfe_status = [
        False,  # 1. Antes
        True,   # 2. Durante (Fechado)
        False,  # 3. Entre
        True,   # 4. Durante (Aberto)
        False,  # 5. Outra placa
    ]

    result_df = check_mdfe_status_duckdb(pedagios_df, duckdb_conn)

    assert 'mdfe_aberto_Atua' in result_df.columns
    assert_series_equal(
        result_df['mdfe_aberto_Atua'],
        pd.Series(expected_mdfe_status, name='mdfe_aberto_Atua'),
        check_dtype=False
    )
