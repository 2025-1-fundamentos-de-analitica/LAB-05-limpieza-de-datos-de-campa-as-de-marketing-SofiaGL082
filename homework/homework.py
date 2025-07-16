"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
from pathlib import Path
from datetime import datetime




def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    input_dir = Path('files/input')
    output_dir = Path('files/output')
    output_dir.mkdir(parents=True, exist_ok=True)

    tablas = []
    for zip_path in input_dir.glob('*.csv.zip'):
        with zipfile.ZipFile(zip_path, 'r') as z:
            for nombre in z.namelist():
                if nombre.lower().endswith('.csv'):
                    with z.open(nombre) as f:
                        df = pd.read_csv(f)
                        tablas.append(df)

    if not tablas:
        print('No se encontró ningún CSV dentro de los ZIP en', input_dir)
        return
    
    datos = pd.concat(tablas, ignore_index=True)

    df_client = pd.DataFrame({
        'client_id': datos['client_id'],
        "age": datos['age'].astype(int),
        'job': datos['job'].str.replace(r"\.", "", regex=True).str.replace("-", "_"),
        'marital': datos['marital'],
        'education': datos['education'].replace("unknown", pd.NA).str.replace(r"\.", "_", regex=True),
        'credit_default': datos['credit_default'].apply(lambda x: 1 if x == 'yes' else 0),
        'mortgage': datos['mortgage'].apply(lambda x: 1 if x == 'yes' else 0),
    })

    df_client.to_csv(output_dir / "client.csv", index=False)

    months = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    meses = datos['month'].map(lambda x: months[x])
    dias = datos['day'].astype(int).astype(str).str.zfill(2)
    fechas = pd.to_datetime(dias + '-' + meses + '-2022', format='%d-%m-%Y')
    df_campaign = pd.DataFrame({
        'client_id': datos["client_id"],
        'number_contacts': datos['number_contacts'],
        'contact_duration': datos['contact_duration'],
        'previous_campaign_contacts': datos['previous_campaign_contacts'],
        'previous_outcome': datos['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0),
        'campaign_outcome': datos['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0),
        'last_contact_date': fechas.dt.strftime("%Y-%m-%d")
    })

    df_campaign.to_csv(output_dir / 'campaign.csv', index=False)

    df_econ = datos[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    df_econ.columns = ['client_id', 'cons_price_idx', 'euribor_three_months']
    df_econ.to_csv(output_dir / "economics.csv", index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
