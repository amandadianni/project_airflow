import pandas as pd
import matplotlib.pyplot as plt
import os
import re

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
FILES_PATH = f'{AIRFLOW_HOME}/dags/files'
IMPORT_CSV_PATH = f'{FILES_PATH}/read/googleplaystore.csv'
EXPORT_PATH = f'{FILES_PATH}/write'


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


def export_cleared_csv(**context):
    df = context['task_instance'].xcom_pull(task_ids='clean_data_task')
    df.to_csv(f"{EXPORT_PATH}/cleared_data.csv", index=False)
    text = f"Arquivo salvo em {EXPORT_PATH}/cleared_data.csv"
    print("-"*len(text))
    print(text)
    print("-"*len(text))
    return df


def count_per_type(**context):
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
    fig.savefig(fname=f"{EXPORT_PATH}/percent_paid_free.png", dpi=300)

    text = f"Gráfico salvo em {EXPORT_PATH}/percent_paid_free.png"
    print("-"*len(text))
    print(text)
    print("-"*len(text))
    return result


def installs_per_category(**context):
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

    fig.savefig(fname=f"{EXPORT_PATH}/installs_per_category.png", dpi=300)

    text = f"Gráfico salvo em {EXPORT_PATH}/installs_per_category.png"
    print("-"*len(text))
    print(text)
    print("-"*len(text))
    return result


def apps_per_android_version(**context):
    df = context['task_instance'].xcom_pull(task_ids='clean_data_task')
    result = df.groupby("Android Ver").App \
        .count() \
        .to_frame() \
        .reset_index() \
        .sort_values(by="App", ascending=True)

    fig, ax = plt.subplots(figsize=[16, 14])
    ax.barh(y=result["Android Ver"], width=result.App)

    plt.title(format_string("aplicativos por versão do android"))
    plt.xlim(0, 2600)
    plt.locator_params(axis="x", nbins=20)
    fig.set_facecolor("#fff")
    fig.tight_layout()
    plt.xlabel("\nQuantidade")
    plt.ylabel("Versão do Android")

    for index, value in enumerate(result.App):
        plt.text(value + 20, index, str(value))

    fig.savefig(fname=f"{EXPORT_PATH}/apps_per_android_version.png", dpi=300)

    text = f"Gráfico salvo em {EXPORT_PATH}/apps_per_android_version.png"
    print("-"*len(text))
    print(text)
    print("-"*len(text))
    return result


def review_5_count_per_category(**context):
    df = context['task_instance'].xcom_pull(task_ids='clean_data_task')
    result = df.loc[df.Rating == 5] \
        .groupby("Category").App \
        .count() \
        .to_frame() \
        .reset_index() \
        .sort_values(by="App", ascending=True)

    result.rename(columns={"App": "Quantity"}, inplace=True)

    fig, ax = plt.subplots(figsize=(16, 10))
    rects = ax.bar(x=result.Category, height=result.Quantity)

    plt.title(format_string("quantidade de avaliações 5 por categoria"))
    fig.autofmt_xdate(rotation=45)
    fig.set_facecolor("#fff")
    fig.tight_layout()

    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 1, height, ha="center")

    fig.savefig(fname=f"{EXPORT_PATH}/rating_equal_five.png", dpi=300)

    text = f"Gráfico salvo em {EXPORT_PATH}/rating_equal_five.png"
    print("-"*len(text))
    print(text)
    print("-"*len(text))
    return result


def review_1_count_per_category(**context):
    df = context['task_instance'].xcom_pull(task_ids='clean_data_task')
    result = df.loc[df.Rating == 1] \
        .groupby("Category")["App"] \
        .count() \
        .to_frame() \
        .reset_index() \
        .sort_values(by="App", ascending=True)

    result.rename(columns={"App": "Quantity"}, inplace=True)

    fig, ax = plt.subplots(figsize=(16, 10))
    rects = ax.bar(x=result["Category"], height=result["Quantity"])

    plt.title(format_string("quantidade de avaliações 1 por categoria"))
    fig.autofmt_xdate(rotation=45)
    fig.set_facecolor("#fff")
    fig.tight_layout()

    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 0.05, height, ha="center")

    fig.savefig(fname=f"{EXPORT_PATH}/rating_equal_one.png", dpi=300)

    text = f"Gráfico salvo em {EXPORT_PATH}/rating_equal_one.png"
    print("-"*len(text))
    print(text)
    print("-"*len(text))
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
