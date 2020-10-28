import pandas as pd
import os

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
PATH_FILE = f'{AIRFLOW_HOME}/dags/files/googleplaystore.csv'


def clean_data(nomearquivo):
    # 1 - importar o csv
    df = pd.read_csv(nomearquivo)
    # 2 - criar coluna identificado tipo numerico ou string
    df['check_int'] = df['Reviews'].apply(lambda review: 'int' if review.isnumeric() else 'str')
    # 3 - realizar filtro somente numerico
    df = df[df['check_int'] != 'str']
    # 4 - Conversão da coluna reviews para numerico
    df['Reviews'] = df['Reviews'].astype(int)
    # 5 - excluir coluna utilizada para filtro
    df = df.drop(columns='check_int')
    df['Installs_Num'] = df['Installs'].apply(lambda x: int(x.replace('+', '').replace(',', ''))).astype(int)
    return df


def read_csv():
    df = pd.read_csv(PATH_FILE)
    dfgroup = df.groupby(['Category', 'Genres'])['App'].count().reset_index()
    dfgroup[dfgroup['Category'] == 'FAMILY'].sort_values('App', ascending=False)
    df[df['Category'] == 'ART_AND_DESIGN'].groupby('Genres')['App'].count()
    df['Genres'].apply(lambda genre: genre.split(';'))
    df['check_review_numeric'] = df['Reviews'].apply(lambda review: 'int' if review.isnumeric() else 'str')
    df.groupby('check_review_numeric')['App'].count()

    df = clean_data(PATH_FILE)
    print(df.sort_values('Rating', ascending=False)[['App', 'Rating']].head(30))
    print(df[df['Rating'] == 5.0].groupby('Category')['App'].count().reset_index().sort_values('App', ascending=False))
    return df
