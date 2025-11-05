import pandas as pd


def check_mdfe_status_duckdb(pedagios_df, duckdb_conn):
    try:
        duckdb_conn.register('p', pedagios_df)

        query = """
        SELECT
            p.*,
            EXISTS (
                SELECT 1
                FROM manifestos m
                WHERE
                    m.placa = p.PLACA
                    AND p."Data Passagem" >= m.dataHora 
                    AND (p."Data Passagem" <= m.updated_at OR m.updated_at IS NULL)
            ) as mdfe_aberto_Atua
        FROM p
        """
        result_df = duckdb_conn.execute(query).df()
        return result_df

    finally:
        duckdb_conn.unregister('p')

