"""
=====================================================================
 PIPELINE: ESTADO DE VENTAS 2026  ->  KPIs de Almacén  ->  Google Sheets  ->  Looker Studio
=====================================================================

VERSIÓN: Preparada para correr automáticamente en GitHub Actions
(lee credenciales e IDs de hoja desde variables de entorno / Secrets,
no desde archivos ni valores escritos en el código)

CAMBIOS EN ESTA VERSIÓN (acordados en revisión de datos):
  - Validación de columnas requeridas al inicio (advertencia si faltan).
  - Verificación de DataFrame vacío antes de procesar.
  - Subida a Google Sheets por lotes de 1000 filas.
  - Pico_Tiempo_Total ahora usa percentil 0.95 (antes 0.90), igual que Pico_Tiempo_Interno.
  - Logging en vez de print.
  - Reintentos ligeros SOLO en la descarga del CSV (no en la subida).
  - Tiempo_Interno_Total_min (y derivados) ya NO se calcula como Fin.Pack - Hora Reg. directo
    con tope de 1 día. Ahora tiene 3 ramas:
      1) Si los 6 tramos (Picking, Checking, Packing, Espera_Reg_Pick, Espera_Pick_Check,
         Espera_Check_Pack) están completos -> suma real de los 6 (medida más precisa,
         inmune al bug de medianoche porque cada tramo ya se corrige en el punto de
         digitación con el formato condicional de la hoja fuente).
      2) Si no, pero Fin. Pack sí tiene dato (por ejemplo, pedidos antiguos cerrados
         manualmente al redondear a la hora de cierre de turno) -> Fin. Pack - Hora Reg.
         directo.
      3) Si el pedido sigue abierto -> tiempo transcurrido = ahora (momento de ejecución
         del script) - Hora Reg. Nunca queda en 0 ni en blanco mientras está en proceso.
  - Cumple_SLA_Interno pasa de 2 a 3 estados: 'Cumple' / 'No cumple' / 'En proceso'.
  - Pico_Tiempo_Interno y Pico_Tiempo_Total se calculan SOLO sobre pedidos cerrados
    (Cumple/No cumple), para que los pedidos "En proceso" (incluidos los antiguos
    abandonados) no distorsionen el percentil.
  - NO se cambia el parseo de fechas (se mantiene formato fijo %d/%m/%Y).
  - NO se vectoriza el código por ahora.
  - NO se toca la lógica de time_diff_datetime para los 6 tramos individuales: el bug de
    medianoche en esos tramos se corrige en la fuente (alerta visual al digitador), no
    en el script.
"""

import json
import logging
import os
import re
import time
from datetime import timedelta, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

CARPETA_SCRIPT = Path(__file__).resolve().parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("procesar_ventas")


# =====================================================================
# 1. CONFIGURACIÓN — se lee desde variables de entorno (Secrets)
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

# Columnas mínimas que el pipeline necesita para calcular los KPIs.
# Si falta alguna, se sigue procesando pero se avisa con una advertencia
# (no se detiene el script: preferimos un reporte incompleto a uno que no corre).
COLUMNAS_REQUERIDAS = [
    "FECHA", "ESTATUS DEL PEDIDO", "Hora Reg.",
    "Ini. Pick", "Fin. Pick", "Ini. Check", "Fin. Check",
    "Ini. Pack", "Fin. Pack", "Hora envio", "FECHA DE ENVIO",
]

DIAS_ES = {0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
           4: "Viernes", 5: "Sábado", 6: "Domingo"}

OBJETIVO_SLA_INTERNO_MIN = 90

# Estatus de origen (columna "ESTATUS DEL PEDIDO") que consideramos "en proceso",
# usados solo como referencia informativa en logs; el cálculo real de "cerrado" se
# basa en si hay datos suficientes, no en el texto de este campo.
ESTATUS_EN_PROCESO = {
    "PEDIDO SIN ASIGNAR", "CHECKING EN PROCESO",
    "PICKING EN PROCESO", "PACKING EN PROCESO", "PACKING PENDIENTE",
}

MAX_REINTENTOS_DESCARGA = 3
ESPERA_ENTRE_REINTENTOS_SEG = 5
TAMANO_LOTE_SUBIDA = 1000

# Perú no usa horario de verano, así que un offset fijo UTC-5 es exacto y, a diferencia de
# usar tz="America/Lima", no depende de que el runner de GitHub Actions tenga la base de
# datos tzdata instalada.
ZONA_PERU = timezone(timedelta(hours=-5))


# =====================================================================
# 2. LECTURA DE LA HOJA ORIGINAL (vía CSV público, con reintentos ligeros)
# =====================================================================
def leer_google_sheets_csv(sheet_id: str, gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

    ultimo_error = None
    for intento in range(1, MAX_REINTENTOS_DESCARGA + 1):
        try:
            log.info("Descargando datos de la hoja ORIGINAL (intento %d/%d)...",
                      intento, MAX_REINTENTOS_DESCARGA)
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text), low_memory=False, dayfirst=True)
            log.info("Hoja original leída: %d filas, %d columnas.", len(df), len(df.columns))
            return df
        except (requests.exceptions.RequestException, pd.errors.ParserError) as exc:
            ultimo_error = exc
            log.warning("Fallo al descargar/parsear el CSV (intento %d/%d): %s",
                        intento, MAX_REINTENTOS_DESCARGA, exc)
            if intento < MAX_REINTENTOS_DESCARGA:
                time.sleep(ESPERA_ENTRE_REINTENTOS_SEG)

    log.error("No se pudo descargar la hoja original tras %d intentos.", MAX_REINTENTOS_DESCARGA)
    raise SystemExit(1) from ultimo_error


# =====================================================================
# 3. VALIDACIÓN DE COLUMNAS Y DE DATAFRAME VACÍO
# =====================================================================
def validar_columnas(df: pd.DataFrame) -> None:
    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
    if faltantes:
        log.warning("Faltan columnas esperadas en la hoja origen: %s. "
                    "Algunos KPIs relacionados no se podrán calcular.", faltantes)


def verificar_no_vacio(df: pd.DataFrame) -> None:
    if df.empty:
        log.error("La hoja origen no devolvió filas. Abortando para no subir un reporte vacío.")
        raise SystemExit(1)


# =====================================================================
# 4. LIMPIEZA DE HORAS CORRUPTAS
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
# 5. CÁLCULO DE DIFERENCIAS DE TIEMPO (KPIs) CON SOPORTE MULTIDÍA
#    (sin cambios: el bug de medianoche en estos 6 tramos se corrige en la
#     fuente, no aquí — ver nota al inicio del archivo)
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


def minutos_desde_hasta_ahora(df, col_fecha, col_hora, ahora: pd.Timestamp):
    """Minutos transcurridos entre (FECHA + col_hora) y 'ahora'. NA si no hay hora inicial."""
    def calcular(fila):
        f_ini = fila[col_fecha] if col_fecha in fila.index else pd.NA
        h_ini = fila[col_hora] if col_hora in fila.index else pd.NA
        if pd.isna(h_ini):
            return pd.NA
        str_f_ini = str(f_ini).strip() if pd.notna(f_ini) else None
        str_h_ini = str(h_ini).strip()
        try:
            if str_f_ini:
                dt_ini = pd.to_datetime(f"{str_f_ini} {str_h_ini}", errors="coerce")
            else:
                dt_ini = pd.to_datetime(str_h_ini, format="%H:%M:%S", errors="coerce")
            if pd.isna(dt_ini):
                return pd.NA
            diferencia = (ahora - dt_ini).total_seconds() / 60
            return round(diferencia, 2) if diferencia >= 0 else pd.NA
        except Exception:
            return pd.NA

    return df.apply(calcular, axis=1)


# =====================================================================
# 6. COLUMNAS AUXILIARES Y BANDERAS
# =====================================================================
def agregar_columnas_auxiliares(df: pd.DataFrame, ahora: pd.Timestamp) -> pd.DataFrame:
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

    # --- Tramos de proceso y espera (sin cambios en su definición) ---
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

    # =================================================================
    # Tiempo_Interno_Total_min: ver notas al inicio del archivo.
    #   - Rama 1: 6 tramos completos -> suma real.
    #   - Rama 2: sin los 6 tramos pero Fin. Pack sí tiene dato -> Fin.Pack - Hora Reg. directo.
    #   - Override de estatus: ESTATUS DEL PEDIDO = 'PROC. RMS' siempre cuenta como Cerrado,
    #     aunque no haya timestamps suficientes para calcular un número (queda NA en ese caso,
    #     pero no se sigue "cronometrando" como si estuviera abierto).
    #   - Filas sin 'Hora Reg.' (filas de relleno sin ningún dato real): no se les asigna
    #     estado ni se cronometran — quedan en blanco, porque la hora de registro es
    #     nuestra base de tiempo y sin ella no hay nada que medir.
    #   - El resto -> tiempo transcurrido hasta el momento de ejecución del script.
    # =================================================================
    columnas_los_6_tramos = columnas_proceso + columnas_espera
    tiene_6_tramos = all(col in df.columns for col in columnas_los_6_tramos)
    tiene_fin_pack = "Fin. Pack" in df.columns
    tiene_hora_reg = "Hora Reg." in df.columns

    if tiene_hora_reg and (tiene_6_tramos or tiene_fin_pack):
        n = len(df)
        tiempo_interno = pd.Series([pd.NA] * n, index=df.index, dtype="object")

        tiene_hora_reg_fila = df["Hora Reg."].notna()

        # Rama 1: los 6 tramos completos -> suma real (más precisa)
        if tiene_6_tramos:
            todos_presentes = df[columnas_los_6_tramos].notna().all(axis=1)
            suma_tramos = df[columnas_los_6_tramos].sum(axis=1, min_count=len(columnas_los_6_tramos))
            tiempo_interno = tiempo_interno.where(~todos_presentes, suma_tramos)
        else:
            todos_presentes = pd.Series([False] * n, index=df.index)

        # Rama 2: sin los 6 tramos, pero Fin. Pack sí tiene dato -> Fin.Pack - Hora Reg. directo
        # (cubre, por ejemplo, pedidos antiguos cerrados manualmente al redondear al cierre de
        # turno; rellenar Fin. Pack días después no afecta el cálculo, porque solo se usa la
        # FECHA del pedido + la hora que se ingrese, no la fecha real de digitación)
        if tiene_fin_pack:
            fin_pack_directo = time_diff_datetime(df, "FECHA", "Hora Reg.", None, "Fin. Pack")
            usar_rama_2 = (~todos_presentes) & df["Fin. Pack"].notna() & fin_pack_directo.notna()
            tiempo_interno = tiempo_interno.where(~usar_rama_2, fin_pack_directo)
        else:
            usar_rama_2 = pd.Series([False] * n, index=df.index)

        # Override: ESTATUS DEL PEDIDO = 'PROC. RMS' siempre es Cerrado para el negocio,
        # tenga o no tenga tiempos calculables.
        if "ESTATUS DEL PEDIDO" in df.columns:
            estatus_proc_rms = df["ESTATUS DEL PEDIDO"] == "PROC. RMS"
        else:
            estatus_proc_rms = pd.Series([False] * n, index=df.index)

        cerrado_mask = todos_presentes | usar_rama_2 | estatus_proc_rms

        # Pedido sigue abierto -> tiempo transcurrido hasta el momento de ejecución.
        # Solo aplica si hay Hora Reg. real (si no, no hay base de tiempo, queda en blanco).
        aun_abierto = tiene_hora_reg_fila & ~cerrado_mask
        transcurrido = minutos_desde_hasta_ahora(df, "FECHA", "Hora Reg.", ahora)
        tiempo_interno = tiempo_interno.where(~aun_abierto, transcurrido)

        # 'Cerrado' cubre tanto los casos con dato calculado (ramas 1/2) como el override de
        # ESTATUS DEL PEDIDO = 'PROC. RMS' sin timestamps; 'En proceso' solo si hay Hora Reg.
        # real y no está cerrado por ninguna vía; el resto (filas de relleno sin Hora Reg.
        # y sin ser PROC. RMS) queda en blanco (pd.NA), sin estado ni cronómetro.
        estado_pedido = pd.Series([pd.NA] * n, index=df.index, dtype="object")
        estado_pedido = estado_pedido.where(~cerrado_mask, "Cerrado")
        estado_pedido = estado_pedido.where(~aun_abierto, "En proceso")

        df["Tiempo_Interno_Total_min"] = pd.to_numeric(tiempo_interno, errors="coerce")
        df["Pedido_Cerrado"] = estado_pedido  # "Cerrado" / "En proceso" / NA (sin dato base) — filtro para Looker

    if "Tiempo_Interno_Total_min" in df.columns:
        tiempo_interno_num = pd.to_numeric(df["Tiempo_Interno_Total_min"], errors='coerce')
        df["Tiempo_Interno_Horas"] = (tiempo_interno_num / 60).round(2)
        df["Tiempo_Interno_Dias"] = (tiempo_interno_num / 1440).round(2)

        # Cumple_SLA_Interno ya no distingue Cerrado/En proceso: es un umbral puro sobre el
        # valor. Un pedido en proceso que ya lleva 91 min es "No cumple" igual que uno cerrado
        # con 91 min — así no hace falta un filtro extra en el informe para dejar de contar el
        # "En proceso" como una categoría aparte. Sin valor calculable (filas de relleno, o
        # PROC. RMS sin timestamps) sigue en blanco, porque no hay nada que comparar.
        def clasificar_sla(valor):
            if pd.isna(valor):
                return pd.NA
            return "Cumple" if valor <= OBJETIVO_SLA_INTERNO_MIN else "No cumple"

        df["Cumple_SLA_Interno"] = tiempo_interno_num.apply(clasificar_sla)

        # El umbral del percentil "pico" se calcula sobre el general de pedidos con tiempo
        # calculable (cerrados + en proceso), no solo cerrados. Con la clasificación actual
        # (PROC. RMS siempre Cerrado, filas de relleno excluidas) el grupo "En proceso" ya es
        # chico y no distorsiona el percentil de forma relevante — se validó contra datos
        # reales: el umbral prácticamente no cambia entre calcularlo solo con cerrados o con
        # el general (1536.0 vs 1536.05 en la última corrida).
        p95_interno = tiempo_interno_num.dropna().quantile(0.95)

        def clasificar_pico(valor):
            if pd.isna(valor):
                return pd.NA
            return "Sí" if valor > p95_interno else "No"

        df["Pico_Tiempo_Interno"] = tiempo_interno_num.apply(clasificar_pico)

    # --- KPI heredado (Tiempo_Total_min, de punta a punta hasta el envío) ---
    if "Tiempo_Total_min" in df.columns:
        total_min_num = pd.to_numeric(df["Tiempo_Total_min"], errors='coerce')
        df["Tiempo_Total_Horas"] = (total_min_num / 60).round(2)
        df["Tiempo_Total_Dias"] = (total_min_num / 1440).round(2)

        df["Cumple_SLA"] = df["Tiempo_Total_min"].apply(
            lambda x: pd.NA if pd.isna(x) else ("Cumple" if x <= OBJETIVO_SLA_INTERNO_MIN else "No cumple")
        )

        # Percentil corregido de 0.90 a 0.95 (antes usaba p90 pese a llamarse p95_total)
        p95_total = total_min_num.quantile(0.95)
        df["Pico_Tiempo_Total"] = df["Tiempo_Total_min"].apply(
            lambda x: pd.NA if pd.isna(x) else ("Sí" if x > p95_total else "No")
        )

    return df


# =====================================================================
# 7. SUBIDA A GOOGLE SHEETS (credenciales desde variable de entorno, por lotes)
# =====================================================================
def subir_a_google_sheets(df: pd.DataFrame) -> None:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info_credenciales = json.loads(CREDENCIALES_JSON)
    creds = Credentials.from_service_account_info(info_credenciales, scopes=scopes)
    client = gspread.authorize(creds)

    log.info("Conectando a la hoja DESTINO...")
    try:
        spreadsheet = client.open_by_key(SHEET_ID_DESTINO)
    except gspread.exceptions.SpreadsheetNotFound:
        log.error("No pude abrir la hoja DESTINO. Revisa el SHEET_ID_DESTINO y que la hoja "
                   "esté compartida con: %s", creds.service_account_email)
        raise SystemExit(1)

    try:
        hoja = spreadsheet.worksheet(NOMBRE_PESTAÑA_DESTINO)
    except gspread.exceptions.WorksheetNotFound:
        hoja = spreadsheet.add_worksheet(title=NOMBRE_PESTAÑA_DESTINO, rows=1, cols=1)

    log.info("Limpiando datos de la hoja destino...")
    hoja.clear()

    df_subida = df.copy()

    for col in df_subida.columns:
        if pd.api.types.is_datetime64_any_dtype(df_subida[col]):
            df_subida[col] = df_subida[col].dt.strftime("%Y-%m-%d")

    for col in df_subida.columns:
        df_subida[col] = df_subida[col].apply(
            lambda x: str(x) if pd.notna(x) else ""
        )

    encabezados = df_subida.columns.tolist()
    filas = df_subida.values.tolist()

    log.info("Subiendo encabezado y %d filas en lotes de %d...", len(filas), TAMANO_LOTE_SUBIDA)

    # Encabezado primero
    hoja.update(values=[encabezados], range_name="A1", value_input_option="USER_ENTERED")

    fila_inicio = 2  # la fila 1 ya tiene el encabezado
    for i in range(0, len(filas), TAMANO_LOTE_SUBIDA):
        lote = filas[i:i + TAMANO_LOTE_SUBIDA]
        rango = f"A{fila_inicio}"
        hoja.update(values=lote, range_name=rango, value_input_option="USER_ENTERED")
        log.info("Lote subido: filas %d a %d.", fila_inicio, fila_inicio + len(lote) - 1)
        fila_inicio += len(lote)

    log.info("Datos subidos correctamente.")


# =====================================================================
# 8. FLUJO PRINCIPAL
# =====================================================================
def main():
    # Ojo: GitHub Actions corre en UTC, pero Hora Reg. está en hora de Perú (UTC-5, sin
    # horario de verano). Si no se ajusta, "ahora" queda ~5h adelantado y el tiempo
    # transcurrido de los pedidos en proceso sale inflado en ~300 minutos.
    ahora = pd.Timestamp.now(tz=ZONA_PERU).tz_localize(None)

    df = leer_google_sheets_csv(SHEET_ID_ORIGINAL, GID_ORIGINAL)
    verificar_no_vacio(df)
    validar_columnas(df)

    if "FECHA" in df.columns:
        df["FECHA"] = pd.to_datetime(df["FECHA"], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")
    if "FECHA DE ENVIO" in df.columns:
        df["FECHA DE ENVIO"] = pd.to_datetime(df["FECHA DE ENVIO"], format="%d/%m/%Y", errors="coerce").dt.strftime("%Y-%m-%d")

    log.info("Limpiando columnas de hora...")
    for col in COLUMNAS_HORA:
        if col in df.columns:
            df[col] = df[col].apply(clean_time)

    log.info("Calculando KPIs internos...")

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

    df = agregar_columnas_auxiliares(df, ahora)

    subir_a_google_sheets(df)

    log.info("Proceso completado.")


if __name__ == "__main__":
    main()
