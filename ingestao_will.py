import pyodbc
import pandas as pd
from secrets.gcp import password_gcp, server_gcp, database_gcp, username_gcp, driver_gcp

server   = server_gcp
database = database_gcp
username = username_gcp
password = password_gcp
driver   = driver_gcp
table_name = 'core_pix_raw'

conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'


def table_exists(cursor: pyodbc.Cursor, table_name: str) -> bool:
    """
    Verifica se uma tabela existe no banco de dados.

    Args:
        cursor (pyodbc.Cursor): O cursor do banco de dados.
        table_name (str): O nome da tabela a verificar.

    Returns:
        bool: True se a tabela existir, False caso contrário.
    """
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", (table_name,))
    return cursor.fetchone()[0] > 0

@staticmethod
def create_table(conn: pyodbc.Connection, df: pd.DataFrame, table_name: str) -> None:
    """
    Cria uma tabela no banco de dados SQL Server e insere registros de um DataFrame.

    Se a tabela especificada não existir, ela será criada com colunas baseadas nos nomes das colunas do DataFrame. 
    Se a tabela já existir, seus dados serão apagados antes de novos registros serem inseridos.

    Args:
        conn (pyodbc.Connection): A conexão com o banco de dados.
        df (pd.DataFrame): O DataFrame contendo os dados a serem inseridos.
        table_name (str): O nome da tabela a ser criada ou limpa e onde os dados serão inseridos.

    Returns:
        None
    """
    cursor = conn.cursor()
    
    if not table_exists(cursor, table_name):
        columns = ', '.join(f'{col} VARCHAR(MAX)' for col in df.columns)
        create_table_sql = f'CREATE TABLE {table_name} ({columns})'
        cursor.execute(create_table_sql)
        conn.commit()
        print(f'Tabela {table_name} criada com sucesso.')
    else:
        print(f'Tabela {table_name} já existe.')

    cursor.execute(f'TRUNCATE TABLE {table_name}')
    conn.commit()
    print(f'Tabela {table_name} limpa com sucesso.')

    data = [tuple(row) for row in df.values.tolist()]

    cursor.fast_executemany = True
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?'] * len(df.columns))})"
    cursor.executemany(insert_query, data)
    conn.commit()

    print(f'Registros inseridos na tabela {table_name}.')

csv_file = '/Users/itamarnascimento/Downloads/case/core_pix.csv'
df = pd.read_csv(csv_file, dtype=str)  # Ler todos os dados como string para preservar o formato original

conn = pyodbc.connect(conn_str)

create_table(conn, df, table_name)

conn.close()
