# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 10:43:24 2024

@author: jcgarcia
"""

import requests
import pandas as pd
from datetime import date, timedelta
import time
import logging
import pytz
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, TEXT

tiempo_inicio = time.time()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def obtener_datos(api_url, headers, fecha_inicial, fecha_final):
    df_list = []
    current_date = fecha_inicial

    while current_date <= fecha_final:
        url = api_url + current_date.strftime("%Y/%m/%d")
        start, num = 0, 500
        params = {"start": start, "num": num}

        while True:
            try:
                response = requests.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    data = response.json()

                    if "response" in data and "rows" in data["response"]:
                        rows = data["response"]["rows"]

                        if rows:
                            df = pd.DataFrame(rows)
                            df["account"] = ""
                            df_list.append(df)

                            if len(df) == num:
                                start += num
                                params["start"] = start
                            else:
                                break
                        else:
                            break
                    else:
                        break
                else:
                    break
            except requests.exceptions.RequestException:
                break

        current_date += timedelta(days=1)

    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

base_urls = [
    "https://API_1:[token]@API_1_URL",
    "https://API_2:[token]@API_2_URL",
    "https://API_3:[token]@API_3_URL",
    "https://API_4:[token]@API_4_URL",
    "https://API_5:[token]@API_5_URL",
    "https://API_6:[token]@API_6_URL",
    "https://API_7:[token]@API_7_URL"
]

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.simulandorequerimientohumano.com"
}

projects = pd.read_excel(r'data/projects_with_team.xlsx')

fecha_inicial = date(2025, 6, 1)
fecha_final = date(2025, 6, 30)

cdr_list = []

for base_url in base_urls:
    try:
        cdr_data = obtener_datos(base_url, headers, fecha_inicial, fecha_final)
        if not cdr_data.empty:
            cdr_list.append(cdr_data)
    except Exception:
        continue

if cdr_list:
    cdr = pd.concat(cdr_list, ignore_index=True)

    if not cdr.empty:
        cdr["account"] = cdr["projectname"].str[-3:]

        account_mapping = {
            "W": "Wa",
            "A": "AI",
            "V": "Vi",
            "S": "So",
            "9": "91",
            "M": "Me",
            "A": "Ab",
            "D": "Da",
            "W": "Wa",
            "M": "Mi"
        }

        cdr["account"] = cdr["account"].map(account_mapping)

        columns_to_keep = [
            "uuid", "account", "source", "destination", "userid", "userfullname", "start_ts", "billing_ts",
            "ringtime", "billingtime", "talktime", "holdtime", "afterwork", "sum_work", "projectid",
            "projectname", "direction", "dispositionreach_label", "dispositionstatus_label", "dialermode_label",
            "disposition_label"
        ]

        existing_columns = [col for col in columns_to_keep if col in cdr.columns]
        cdr = cdr[existing_columns]

        projects = projects[['projectname', 'Service']]
        cdr = cdr.merge(projects, on="projectname", how="left")

        cdr['start_ts'] = pd.to_datetime(cdr['start_ts'], errors='coerce')
        cdr['billing_ts'] = pd.to_datetime(cdr['billing_ts'], errors='coerce')

        hungria_tz = pytz.timezone('Europe/Budapest')
        fecha_referencia = fecha_inicial
        is_dst = bool(hungria_tz.localize(pd.Timestamp(fecha_referencia)).dst())

        horas_a_restar = 7 if is_dst else 6

        cdr['start_ts'] = cdr['start_ts'] - timedelta(hours=horas_a_restar)
        cdr['billing_ts'] = cdr['billing_ts'] - timedelta(hours=horas_a_restar)

        #cdr = cdr[cdr['start_ts'].dt.date == fecha_inicial]

        nombre_archivo = f'CDR_Log_Daily_Latam_{fecha_inicial.strftime("%Y_%B_%d")}.xlsx'
        ruta = r'data/output/'
        os.makedirs(ruta, exist_ok=True)
        archivo_salida = f"{ruta}\\{nombre_archivo}"
        cdr.to_excel(archivo_salida, index=False)

        username = os.getenv('DB_USERNAME', '[DB_USERNAME]')
        password = os.getenv('DB_PASSWORD', '[DB_PASSWORD]')
        host = os.getenv('DB_HOST', "[DB_HOST]")
        database = os.getenv('DB_DATABASE', '[DB_NAME]')
        connection_string = f'mysql+pymysql://{username}:{password}@{host}/{database}'

        engine = create_engine(connection_string)
        metadata = MetaData()

        CDR_log_migration_table = Table('CDR_Log', metadata,
            Column('uuid', String(100)),
            Column('account_id', Integer),
            Column('project_id', Integer),
            Column('user_id', String(100)),
            Column('user_name', String(100)),
            Column('source_number', String(50)),
            Column('destination_number', String(50)),
            Column('start_ts', DateTime),
            Column('billing_ts', DateTime),
            Column('ring_time', Integer),
            Column('billing_time', Integer),
            Column('talk_time', Integer),
            Column('hold_time', Integer),
            Column('after_work', Integer),
            Column('sum_work', Integer),
            Column('direction', String(50)),
            Column('disposition_reach_label', String(50)),
            Column('disposition_status_label', String(50)),
            Column('dialer_mode_label', String(50)),
            Column('disposition_label', String(50))
        )

        metadata.create_all(engine)

        CDR_migration = cdr.copy()

        CDR_migration = CDR_migration.rename(columns={
            'uuid': 'uuid',
            'account': 'account_id',
            'projectid': 'project_id',
            'userid': 'user_id',
            'userfullname': 'user_name',
            'source': 'source_number',
            'destination': 'destination_number',
            'start_ts': 'start_ts',
            'billing_ts': 'billing_ts',
            'ringtime': 'ring_time',
            'billingtime': 'billing_time',
            'talktime': 'talk_time',
            'holdtime': 'hold_time',
            'afterwork': 'after_work',
            'sum_work': 'sum_work',
            'direction': 'direction',
            'dispositionreach_label': 'disposition_reach_label',
            'dispositionstatus_label': 'disposition_status_label',
            'dialermode_label': 'dialer_mode_label',
            'disposition_label': 'disposition_label'
        })

        CDR_migration = CDR_migration.dropna(subset=['uuid', 'start_ts', 'billing_ts'])

        from sqlalchemy.dialects.mysql import insert
        with engine.begin() as connection:
            for _, row in CDR_migration.iterrows():
                insert_stmt = insert(CDR_log_migration_table).values(
                    uuid=row['uuid'],
                    account_id=row.get('account_id'),
                    project_id=row.get('project_id'),
                    user_id=row.get('user_id'),
                    user_name=row.get('user_name'),
                    source_number=row.get('source_number'),
                    destination_number=row.get('destination_number'),
                    start_ts=row.get('start_ts'),
                    billing_ts=row.get('billing_ts'),
                    ring_time=row.get('ring_time'),
                    billing_time=row.get('billing_time'),
                    talk_time=row.get('talk_time'),
                    hold_time=row.get('hold_time'),
                    after_work=row.get('after_work'),
                    sum_work=row.get('sum_work'),
                    direction=row.get('direction'),
                    disposition_reach_label=row.get('disposition_reach_label'),
                    disposition_status_label=row.get('disposition_status_label'),
                    dialer_mode_label=row.get('dialer_mode_label'),
                    disposition_label=row.get('disposition_label')
                )
                connection.execute(insert_stmt)

        print("Migración a la base de datos completada correctamente.")
        
    else:
        print("No se encontraron datos CDR para procesar.")

tiempo_final = time.time()
tiempo_total = tiempo_final - tiempo_inicio
print(f"Tiempo total de ejecución: {tiempo_total:.2f} segundos")