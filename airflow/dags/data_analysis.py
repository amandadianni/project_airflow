import pandas as pd
import matplotlib.pyplot as plt
import os
import re

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
FILES_PATH = f'{AIRFLOW_HOME}/dags/files'
IMPORT_CSV_PATH = f'{FILES_PATH}/read/googleplaystore.csv'


def column_to_numeric(column):
    new_column = [re.sub("[^\d.]", "", row) for row in column]
    new_column = pd.to_numeric(new_column, errors="coerce")
    return new_column


def format_string(text):
    return text.capitalize()


def read_csv():
    df = pd.read_csv(IMPORT_CSV_PATH)
    return df


def clean_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='read_csv_task')
    df.Reviews = column_to_numeric(df.Reviews)
    df.Size = column_to_numeric(df.Size)
    df.Installs = column_to_numeric(df.Installs)
    df.Price = column_to_numeric(df.Price)
    [df.drop(index=idx, inplace=True) for idx in df.index[df.Rating > 5]]
    return df


def count_by_type(**context):
    df = context['task_instance'].xcom_pull(task_ids='clean_data_task')
    result = df.groupby("Type").App \
        .count() \
        .to_frame() \
        .reset_index() \
        .rename(columns={"App": "Quantity"})
    result.Type = ["Gratuitos" if x == "Free" else "Pagos" for x in result.Type]
    fig, ax = plt.subplots(figsize=[8, 8])
    ax.pie(x=result.Quantity, explode=[0.05, 0.05], autopct="%.2f%%")

    plt.title(format_string("aplicativos disponíveis"))
    plt.legend(labels=result.Type)
    fig.set_facecolor("#fff")
    fig.tight_layout()
    fig.savefig(fname=f"{FILES_PATH}/write/percent_paid_free.png", dpi=300)

    return result


def installs_by_category(**context):
    df = context['task_instance'].xcom_pull(task_ids='clean_data_task')
    result = df.groupby("Category").Installs \
        .sum() \
        .to_frame() \
        .sort_values("Installs") \
        .reset_index()

    fig, ax = plt.subplots(figsize=[16, 14])
    ax.barh(y=result.Category, width=result.Installs)

    plt.title(format_string("instalações por categoria"))
    plt.xlim(0, 4e10)
    plt.locator_params(axis="x", nbins=20)
    fig.set_facecolor("#fff")
    fig.tight_layout()

    for index, value in enumerate(result.Installs):
        plt.text(value + .05e10, index, str(value))

    fig.savefig(fname=f"{FILES_PATH}/write/installs_per_category.png", dpi=300)
    return result


def outro():
    # 1 - importar o csv
    # # 2 - criar coluna identificado tipo numerico ou string
    # df['check_int'] = df['Reviews'].apply(lambda review: 'int' if review.isnumeric() else 'str')
    # # 3 - realizar filtro somente numerico
    # df = df[df['check_int'] != 'str']
    # df['Reviews'] = df['Reviews'].astype(int)
    # # 5 - excluir coluna utilizada para filtro
    # df = df.drop(columns='check_int')
    # df['Installs_Num'] = df['Installs'].apply(lambda x: int(x.replace('+', '').replace(',', ''))).astype(int)

    # df = clean_data(PATH_FILE)
    # dfgroup = df.groupby(['Category', 'Genres'])['App'].count().reset_index()
    # dfgroup[dfgroup['Category'] == 'FAMILY'].sort_values('App', ascending=False)
    # df[df['Category'] == 'ART_AND_DESIGN'].groupby('Genres')['App'].count()
    # df['Genres'].apply(lambda genre: genre.split(';'))
    # df['check_review_numeric'] = df['Reviews'].apply(lambda review: 'int' if review.isnumeric() else 'str')
    # df.groupby('check_review_numeric')['App'].count()
    #
    # df = clean_data(PATH_FILE)
    # print(df.sort_values('Rating', ascending=False)[['App', 'Rating']].head(30))
    # print(df[df['Rating'] == 5.0].groupby('Category')['App'].count().reset_index().sort_values('App', ascending=False))
    return 'oi'

# 1. OK separar o tratamento em outro arquivo
# 2. OK PATH relativo do arquivo csv
# 3. mais análises
# 4. OK tentar extrair trechos em tasks separadas
# 5. exportar csv com os dados tratados
