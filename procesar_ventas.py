"""
=====================================================================
 PIPELINE: ESTADO DE VENTAS 2026  ->  KPIs de Almacén  ->  Google Sheets  ->  Looker Studio
=====================================================================

VERSIÓN: Preparada para correr automáticamente en GitHub Actions
(lee credenciales e IDs de hoja desde variables de entorno / Secrets,
no desde archivos ni valores escritos en el código)
"""

import json
import os
import re
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

CARPETA_SCRIPT = Path(__file__).resolve().parent


# =====================================================================
# 1. CONFIGURACIÓN — ahora se lee desde variables de entorno (Secrets)
# =====================================================================

SHEET_ID_ORIGINAL = os.environ["SHEET_ID_ORIGINAL"]
GID_ORIGINAL = os.environ["GID_ORIGINAL"]

SHEET_ID_DESTINO = os.environ["SHEET_ID_DESTINO"]
NOMBRE_PESTAÑA_DESTINO = os.environ.get("NOMBRE_PESTAÑA_DESTINO", "KPI_LIMPIO")

# El contenido completo del credentials.json va en el Secret GOOGLE_CREDENTIALS_JSON
CREDENCIALES_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

COLUMNAS_HORA = [
    "Hora Reg.", "Ini. Pick", "Fin. Pick",
    "Ini. Check", "Fin. Check", "Ini. Pack", "Fin. Pack", "Hora envio",
]

DIAS_ES = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
           4: "Viernes", 5: "Sábado", 6: "Domingo"}

OBJETIVO_SLA_INTERNO_MIN = 45


# =====================================================================
# 2. LECTURA DE LA HOJA ORIGINAL (vía CSV público)
# =====================================================================
def leer_google_sheets_csv(sheet_id: str, gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    print("Descargando datos de la hoja ORIGINAL...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    df = pd.read_csv(StringIO(resp.text), low_memory=False, dayfirst=True)
    print(f"Hoja original leída: {len(df)} filas, {len(df.columns)} columnas.")
    return df


# =====================================================================
# 3. LIMPIEZA DE HORAS CORRUPTAS
# =====================================================================
PATRON_HORA = re.compile(
    r"(\d{1,2}):(\d{2})(?::(\d{2}))?(?:[.,]\d+)?\s*(a\.?m\.?|p\.?m\.?)?\s*$",
    re.IGNORECASE,
)


def clean_time(valor):
    if pd.isna(valor):
        return pd.NA
    texto = str(valor).strip()
    if texto == "" or texto == "-":
        return pd.NA

    match = PATRON_HORA.search(texto)
    if match:
        h, m, s, meridiano = match.groups()
        h = int(h)
        s = s or "00"
        if meridiano:
            meridiano = meridiano.upper().replace(".", "")
            if meridiano == "PM" and h != 12:
                h += 12
            elif meridiano == "AM" and h == 12:
                h = 0
        return f"{h:02d}:{m}:{s}"

    try:
        fraccion = float(texto.replace(",", "."))
        if 0 <= fraccion < 1:
            segundos_totales = round(fraccion * 24 * 60 * 60)
            h, resto = divmod(segundos_totales, 3600)
            m, s = divmod(resto, 60)
            return f"{h:02d}:{m:02d}:{s:02d}"
    except ValueError:
        pass

    return pd.NA


# =====================================================================
# 4. CÁLCULO DE DIFERENCIAS DE TIEMPO (KPIs) CON SOPORTE MULTIDÍA
# =====================================================================
def time_diff_datetime(df, col_fecha_ini, col_hora_ini, col_fecha_fin=None, col_hora_fin=None):
    def calcular(fila):
        f_ini = fila[col_fecha_ini] if col_fecha_ini in fila.index else pd.NA
        h_ini = fila[col_hora_ini] if col_hora_ini in fila.index else pd.NA
        f_fin = fila[col_fecha_fin] if col_fecha_fin and col_fecha_fin in fila.index else pd.NA
        h_fin = fila[col_hora_fin] if col_hora_fin and col_hora_fin in fila.index else pd.NA

        if pd.isna(h_ini) or pd.isna(h_fin):
            return pd.NA

        str_f_ini = str(f_ini).strip() if pd.notna(f_ini) else None
        str_h_ini = str(h_ini).strip()
        str_f_fin = str(f_fin).strip() if pd.notna(f_fin) and str(f_fin).strip() != "-" else None
        str_h_fin = str(h_fin).strip()

        try:
            if str_f_ini and str_f_fin:
                dt_ini = pd.to_datetime(f"{str_f_ini} {str_h_ini}", errors='coerce')
                dt_fin = pd.to_datetime(f"{str_f_fin} {str_h_fin}", errors='coerce')
            elif str_f_ini:
                dt_ini = pd.to_datetime(f"{str_f_ini} {str_h_ini}", errors='coerce')
                dt_fin = pd.to_datetime(f"{str_f_ini} {str_h_fin}", errors='coerce')
                if dt_fin < dt_ini:
                    dt_fin += pd.Timedelta(days=1)
            else:
                dt_ini = pd.to_datetime(str_h_ini, format="%H:%M:%S", errors='coerce')
                dt_fin = pd.to_datetime(str_h_fin, format="%H:%M:%S", errors='coerce')
                if dt_fin < dt_ini:
                    dt_fin += pd.Timedelta(days=1)

            if pd.isna(dt_ini) or pd.isna(dt_fin):
                return pd.NA

            diferencia = (dt_fin - dt_ini).total_seconds() / 60
            return round(diferencia, 2)
        except Exception:
            return pd.NA

    return df.apply(calcular, axis=1)


# =====================================================================
# 5. COLUMNAS AUXILIARES Y BANDERAS
# =====================================================================
def agregar_columnas_auxiliares(df: pd.DataFrame) -> pd.DataFrame:
    if "FECHA" in df.columns:
        df["FECHA_DT"] = pd.to_datetime(df["FECHA"], errors="coerce")
        df["Dia_Semana"] = df["FECHA_DT"].dt.dayofweek.map(DIAS_ES)
        df["Semana_Anio"] = df["FECHA_DT"].dt.isocalendar().week.astype("Int64")
        df["Mes"] = df["FECHA_DT"].dt.month

    if "Hora Reg." in df.columns:
        def hora_bucket(valor):
            if pd.isna(valor):
                return pd.NA
            try:
                return int(str(valor).split(":")[0])
            except Exception:
                return pd.NA

        df["Hora_Del_Dia_Registro"] = df["Hora Reg."].apply(hora_bucket)

        def franja(hora):
            if pd.isna(hora):
                return pd.NA
            if 5 <= hora < 12:
                return "Mañana"
            if 12 <= hora < 18:
                return "Tarde"
            return "Noche"

        df["Franja_Horaria"] = df["Hora_Del_Dia_Registro"].apply(franja)

    if {"FECHA", "FECHA DE ENVIO"}.issubset(df.columns):
        def eval_cierre_mismo_dia(row):
            f_ini = str(row["FECHA"]).strip() if pd.notna(row["FECHA"]) else None
            f_fin = str(row["FECHA DE ENVIO"]).strip() if pd.notna(row["FECHA DE ENVIO"]) and str(row["FECHA DE ENVIO"]).strip() != "-" else None
            if not f_ini or not f_fin:
                return pd.NA
            return "Sí" if f_ini == f_fin else "No"

        df["Cerrado_Mismo_Dia"] = df.apply(eval_cierre_mismo_dia, axis=1)

    columnas_proceso = ['Tiempo_Picking_min', 'Tiempo_Checking_min', 'Tiempo_Packing_min']
    if all(col in df.columns for col in columnas_proceso):
        df['Tiempo_Proceso_Total_min'] = df[columnas_proceso].sum(axis=1, min_count=1)
        df['Tiempo_Proceso_Total_min'] = df['Tiempo_Proceso_Total_min'].where(
            df[columnas_proceso].notna().any(axis=1), pd.NA
        )

    columnas_espera = ['Espera_Reg_Pick_min', 'Espera_Pick_Check_min', 'Espera_Check_Pack_min']
    if all(col in df.columns for col in columnas_espera):
        df['Espera_Total_min'] = df[columnas_espera].sum(axis=1, min_count=1)
        df['Espera_Total_min'] = df['Espera_Total_min'].where(
            df[columnas_espera].notna().any(axis=1), pd.NA
        )

    if {"Hora Reg.", "Fin. Pack"}.issubset(df.columns):
        df["Tiempo_Interno_Total_min"] = time_diff_datetime(
            df, "FECHA", "Hora Reg.", None, "Fin. Pack"
        )

    if "Tiempo_Interno_Total_min" in df.columns:
        tiempo_interno_num = pd.to_numeric(df["Tiempo_Interno_Total_min"], errors='coerce')
        df["Tiempo_Interno_Horas"] = (tiempo_interno_num / 60).round(2)
        df["Tiempo_Interno_Dias"] = (tiempo_interno_num / 1440).round(2)

        df["Cumple_SLA_Interno"] = df["Tiempo_Interno_Total_min"].apply(
            lambda x: pd.NA if pd.isna(x) else ("Cumple" if x <= OBJETIVO_SLA_INTERNO_MIN else "No cumple")
        )

        p90_interno = df["Tiempo_Interno_Total_min"].quantile(0.90)
        df["Pico_Tiempo_Interno"] = df["Tiempo_Interno_Total_min"].apply(
            lambda x: pd.NA if pd.isna(x) else ("Sí" if x > p90_interno else "No")
        )

    if "Tiempo_Total_min" in df.columns:
        total_min_num = pd.to_numeric(df["Tiempo_Total_min"], errors='coerce')
        df["Tiempo_Total_Horas"] = (total_min_num / 60).round(2)
        df["Tiempo_Total_Dias"] = (total_min_num / 1440).round(2)

        df["Cumple_SLA"] = df["Tiempo_Total_min"].apply(
            lambda x: pd.NA if pd.isna(x) else ("Cumple" if x <= OBJETIVO_SLA_INTERNO_MIN else "No cumple")
        )

        p90_total = df["Tiempo_Total_min"].quantile(0.90)
        df["Pico_Tiempo_Total"] = df["Tiempo_Total_min"].apply(
            lambda x: pd.NA if pd.isna(x) else ("Sí" if x > p90_total else "No")
        )

    return df


# =====================================================================
# 6. SUBIDA A GOOGLE SHEETS (credenciales desde variable de entorno)
# =====================================================================
def subir_a_google_sheets(df: pd.DataFrame) -> None:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info_credenciales = json.loads(CREDENCIALES_JSON)
    creds = Credentials.from_service_account_info(info_credenciales, scopes=scopes)
    client = gspread.authorize(creds)

    print("Conectando a la hoja DESTINO...")
    try:
        spreadsheet = client.open_by_key(SHEET_ID_DESTINO)
    except gspread.exceptions.SpreadsheetNotFound:
        print("No pude abrir la hoja DESTINO. Revisa el SHEET_ID_DESTINO y que la hoja")
        print(f"esté compartida con: {creds.service_account_email}")
        raise SystemExit(1)

    try:
        hoja = spreadsheet.worksheet(NOMBRE_PESTAÑA_DESTINO)
    except gspread.exceptions.WorksheetNotFound:
        hoja = spreadsheet.add_worksheet(title=NOMBRE_PESTAÑA_DESTINO, rows=1, cols=1)

    print("Limpiando datos de la hoja destino...")
    hoja.clear()

    df_subida = df.copy()

    for col in df_subida.columns:
        if pd.api.types.is_datetime64_any_dtype(df_subida[col]):
            df_subida[col] = df_subida[col].dt.strftime("%Y-%m-%d")

    for col in df_subida.columns:
        df_subida[col] = df_subida[col].apply(
            lambda x: str(x) if pd.notna(x) else ""
        )

    datos = [df_subida.columns.tolist()] + df_subida.values.tolist()

    print(f"Subiendo {len(datos)-1} filas a Google Sheets...")
    hoja.update(values=datos, range_name="A1", value_input_option="USER_ENTERED")
    print("Datos subidos correctamente.")


# =====================================================================
# 7. FLUJO PRINCIPAL
# =====================================================================
def main():
    df = leer_google_sheets_csv(SHEET_ID_ORIGINAL, GID_ORIGINAL)

    if "FECHA" in df.columns:
        df["FECHA"] = pd.to_datetime(df["FECHA"], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")
    if "FECHA DE ENVIO" in df.columns:
        df["FECHA DE ENVIO"] = pd.to_datetime(df["FECHA DE ENVIO"], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")

    print("Limpiando columnas de hora...")
    for col in COLUMNAS_HORA:
        if col in df.columns:
            df[col] = df[col].apply(clean_time)

    print("Calculando KPIs Internos...")

    if {"Ini. Pick", "Fin. Pick"}.issubset(df.columns):
        df["Tiempo_Picking_min"] = time_diff_datetime(df, "FECHA", "Ini. Pick", None, "Fin. Pick")

    if {"Ini. Check", "Fin. Check"}.issubset(df.columns):
        df["Tiempo_Checking_min"] = time_diff_datetime(df, "FECHA", "Ini. Check", None, "Fin. Check")

    if {"Ini. Pack", "Fin. Pack"}.issubset(df.columns):
        df["Tiempo_Packing_min"] = time_diff_datetime(df, "FECHA", "Ini. Pack", None, "Fin. Pack")

    if {"Fin. Pick", "Ini. Check"}.issubset(df.columns):
        df["Espera_Pick_Check_min"] = time_diff_datetime(df, "FECHA", "Fin. Pick", None, "Ini. Check")
    if {"Fin. Check", "Ini. Pack"}.issubset(df.columns):
        df["Espera_Check_Pack_min"] = time_diff_datetime(df, "FECHA", "Fin. Check", None, "Ini. Pack")
    if {"Hora Reg.", "Ini. Pick"}.issubset(df.columns):
        df["Espera_Reg_Pick_min"] = time_diff_datetime(df, "FECHA", "Hora Reg.", None, "Ini. Pick")
    if {"Fin. Pack", "Hora envio"}.issubset(df.columns):
        df["Espera_Pack_Envio_min"] = time_diff_datetime(df, "FECHA", "Fin. Pack", None, "Hora envio")

    if {"Hora Reg.", "Hora envio"}.issubset(df.columns):
        df["Tiempo_Total_min"] = time_diff_datetime(df, "FECHA", "Hora Reg.", "FECHA DE ENVIO", "Hora envio")

    df = agregar_columnas_auxiliares(df)

    subir_a_google_sheets(df)

    print("Proceso completado.")


if __name__ == "__main__":
    main()
