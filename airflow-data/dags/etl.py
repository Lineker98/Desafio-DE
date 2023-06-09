import pandas as pd
import csv
from typing import Optional, Tuple, List
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def extract_load(
    table: str, file_path: Optional[str] = None
) -> None | Tuple[List, List]:
    """
    Função para extração de uma tabela e carregamento se especificado.

    Args:
        table (str): tabela desejada para a extração.
        file_path (Optional[str], optional): Arquivo que deseja salvar a tabela, ex: output.csv. Defaults to None.

    Returns:
        None | Tuple[List, List]: None se realizar o carregamento da tabela uma tupla de listas
        contendo as linhas extraídas e o nomes das colunas, respectivamente.
    """
    hook = SqliteHook(sqlite_conn_id="sqlite_connection")
    conn = hook.get_conn()
    cursor = conn.cursor()
    select_query = f"SELECT * FROM '{table}'"
    cursor.execute(select_query)
    lines = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    if file_path:
        with open(file_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(columns)
            for line in lines:
                writer.writerow(line)
    else:
        return (lines, columns)


def process(
    join_table: str, left_key: str, right_key: str, how: str, file_path: str
) -> None:
    """
    Função para calcular a quantidade vendida para o Rio de janeiro.

    Args:
        join_table (str): Tabela desejada para fazer o join.
        left_key (str): Chave para o join da tabela à esquerda.
        right_key (str): Chave para o join da tabela à direita.
        how (str): De qual maneira o join será executado.
        file_path (str): Arquivo para savar o resultado da operação.
    """
    lines, columns = extract_load(table=join_table)
    df2 = pd.DataFrame(lines, columns=columns)
    df1 = pd.read_csv("output_orders.csv")

    df_join = pd.merge(df1, df2, left_on=left_key, right_on=right_key, how=how)
    result = df_join.loc[df_join["ShipCity"] == "Rio de Janeiro", "Quantity"].sum()

    with open(file_path, "w") as f:
        f.write(str(result))
