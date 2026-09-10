"""
=====================================================================
 PIPELINE: KARDEX → Talla Top → Google Sheets → Looker Studio
=====================================================================

Lee la hoja KARDEX (privada, vía API con Service Account), filtra
movimientos de SALIDA con Nº de pedido que empiece con "T0", descompone
el código de ITEM en CODIGO, MODELO y TALLA, y calcula la talla con
mayor cantidad de salidas por CODIGO+MODELO+FECHA.

El resultado se sube a la pestaña "Talla_Top" de la hoja destino.
"""

import json
import logging
import os
import sys
from datetime import timedelta, timezone

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# =====================================================================
# Logging
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("talla_top")

# =====================================================================
# 1. CONFIGURACIÓN — desde variables de entorno (Secrets de GitHub)
# =====================================================================
SHEET_ID_KARDEX = os.environ["SHEET_ID_KARDEX"]
GID_KARDEX = os.environ["GID_KARDEX"]
SHEET_ID_DESTINO = os.environ["SHEET_ID_DESTINO"]
CREDENCIALES_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

NOMBRE_PESTANA_DESTINO = "Talla_Top"

ZONA_PERU = timezone(timedelta(hours=-5))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

TAMANO_LOTE_SUBIDA = 1000


# =====================================================================
# 2. AUTENTICACIÓN
# =====================================================================
def autenticar() -> gspread.Client:
    """Devuelve un cliente gspread autenticado con la Service Account."""
    creds_dict = json.loads(CREDENCIALES_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    cliente = gspread.authorize(creds)
    log.info("Autenticación exitosa con Service Account: %s", creds.service_account_email)
    return cliente


# =====================================================================
# 3. LECTURA DE LA HOJA KARDEX (vía API, porque es privada)
# =====================================================================
def leer_kardex(cliente: gspread.Client) -> pd.DataFrame:
    """Lee todos los datos de la hoja KARDEX usando la API de Google Sheets."""
    log.info("Abriendo hoja KARDEX...")
    libro = cliente.open_by_key(SHEET_ID_KARDEX)

    # Buscar la hoja por gid
    hoja = None
    for ws in libro.worksheets():
        if str(ws.id) == GID_KARDEX:
            hoja = ws
            break

    if hoja is None:
        # Intentar por nombre como fallback
        try:
            hoja = libro.worksheet("KARDEX")
        except gspread.exceptions.WorksheetNotFound:
            log.error("No se encontró la hoja con gid=%s ni con nombre 'KARDEX'.", GID_KARDEX)
            sys.exit(1)

    log.info("Leyendo hoja '%s' (gid=%s)...", hoja.title, hoja.id)
    datos = hoja.get_all_records()

    if not datos:
        log.error("La hoja KARDEX está vacía. Abortando.")
        sys.exit(1)

    df = pd.DataFrame(datos)
    log.info("KARDEX leída: %d filas, %d columnas.", len(df), len(df.columns))
    return df


# =====================================================================
# 4. PROCESAMIENTO
# =====================================================================
def descomponer_item(item: str) -> dict:
    """
    Descompone un código de ITEM en CODIGO, MODELO y TALLA.

    Ejemplo: '454M442' → CODIGO='454', MODELO='M4', TALLA='42'
    Regla: últimos 2 = TALLA, 2 anteriores = MODELO, resto = CODIGO.
    El largo del ítem puede variar, pero la lógica es la misma.
    """
    item = str(item).strip()
    if len(item) < 4:
        return {"CODIGO": item, "MODELO": "", "TALLA": ""}
    return {
        "CODIGO": item[:-4],
        "MODELO": item[-4:-2],
        "TALLA": item[-2:],
    }


def procesar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra, descompone ítems, agrupa y selecciona la talla top
    por CODIGO+MODELO+FECHA.
    """
    # --- Normalizar nombres de columnas (quitar espacios extra) ---
    df.columns = df.columns.str.strip()

    # --- Identificar columnas por posición (como en la hoja KARDEX) ---
    # Columna A = FECHA_HORA, B = TIPO_MOVIMIENTO, E = ITEM,
    # F = CANTIDAD, H = Nº PEDIDO
    col_fecha_hora = df.columns[0]   # A
    col_movimiento = df.columns[1]   # B
    col_item = df.columns[4]         # E
    col_cantidad = df.columns[5]     # F
    col_pedido = df.columns[7]       # H

    log.info("Columnas detectadas → FechaHora: '%s', Movimiento: '%s', "
             "Item: '%s', Cantidad: '%s', Pedido: '%s'",
             col_fecha_hora, col_movimiento, col_item, col_cantidad, col_pedido)

    # --- Filtro 1: Solo SALIDA ---
    mascara_salida = df[col_movimiento].astype(str).str.strip().str.upper() == "SALIDA"
    log.info("Filas con SALIDA: %d de %d", mascara_salida.sum(), len(df))

    # --- Filtro 2: Nº PEDIDO empieza con T0 ---
    mascara_t0 = df[col_pedido].astype(str).str.strip().str.upper().str.startswith("T0")
    log.info("Filas con pedido T0: %d de %d", mascara_t0.sum(), len(df))

    df_filtrado = df[mascara_salida & mascara_t0].copy()
    log.info("Filas tras ambos filtros: %d", len(df_filtrado))

    if df_filtrado.empty:
        log.warning("No hay registros que cumplan los filtros. Se subirá tabla vacía.")
        return pd.DataFrame(columns=["FECHA", "CODIGO", "MODELO", "TALLA_TOP", "CANTIDAD_SALIDA"])

    # --- Extraer solo la fecha (sin hora) ---
    df_filtrado["FECHA"] = (
        pd.to_datetime(df_filtrado[col_fecha_hora], dayfirst=True, errors="coerce")
        .dt.strftime("%d/%m/%Y")
    )

    # --- Cantidad numérica ---
    df_filtrado["CANTIDAD"] = pd.to_numeric(
        df_filtrado[col_cantidad], errors="coerce"
    ).fillna(0).astype(int)

    # --- Descomponer ITEM en CODIGO, MODELO, TALLA ---
    descompuesto = df_filtrado[col_item].apply(descomponer_item).apply(pd.Series)
    df_filtrado = pd.concat([df_filtrado, descompuesto], axis=1)

    # --- Agrupar por FECHA + CODIGO + MODELO + TALLA → sumar CANTIDAD ---
    agrupado = (
        df_filtrado
        .groupby(["FECHA", "CODIGO", "MODELO", "TALLA"], as_index=False)["CANTIDAD"]
        .sum()
    )

    # --- Por cada FECHA + CODIGO + MODELO, quedarse con la talla top ---
    agrupado = agrupado.sort_values("CANTIDAD", ascending=False)
    talla_top = (
        agrupado
        .groupby(["FECHA", "CODIGO", "MODELO"], as_index=False)
        .first()  # ya está ordenado desc → el primero es el top
    )

    # --- Renombrar y ordenar ---
    talla_top = talla_top.rename(columns={
        "TALLA": "TALLA_TOP",
        "CANTIDAD": "CANTIDAD_SALIDA",
    })
    talla_top = talla_top.sort_values(
        "CANTIDAD_SALIDA", ascending=False
    ).reset_index(drop=True)

    log.info("Resultado final: %d filas (combinaciones únicas FECHA+CÓDIGO+MODELO).",
             len(talla_top))
    return talla_top[["FECHA", "CODIGO", "MODELO", "TALLA_TOP", "CANTIDAD_SALIDA"]]


# =====================================================================
# 5. SUBIDA A GOOGLE SHEETS
# =====================================================================
def subir_a_sheets(cliente: gspread.Client, df: pd.DataFrame) -> None:
    """Sube el DataFrame a la pestaña Talla_Top de la hoja destino."""
    log.info("Abriendo hoja destino...")
    libro = cliente.open_by_key(SHEET_ID_DESTINO)

    # Crear o limpiar la pestaña
    try:
        hoja = libro.worksheet(NOMBRE_PESTANA_DESTINO)
        log.info("Pestaña '%s' ya existe. Limpiando contenido anterior...",
                 NOMBRE_PESTANA_DESTINO)
        hoja.clear()
    except gspread.exceptions.WorksheetNotFound:
        log.info("Creando pestaña '%s'...", NOMBRE_PESTANA_DESTINO)
        hoja = libro.add_worksheet(
            title=NOMBRE_PESTANA_DESTINO, rows=1000, cols=10
        )

    # Preparar datos: encabezado + filas
    encabezado = df.columns.tolist()
    filas = df.values.tolist()
    todas_las_filas = [encabezado] + filas

    # Subir por lotes
    total = len(todas_las_filas)
    for inicio in range(0, total, TAMANO_LOTE_SUBIDA):
        lote = todas_las_filas[inicio : inicio + TAMANO_LOTE_SUBIDA]
        rango = f"A{inicio + 1}"
        hoja.update(values=lote, range_name=rango)
        log.info("Subidas filas %d a %d de %d.",
                 inicio + 1, min(inicio + TAMANO_LOTE_SUBIDA, total), total)

    log.info("✅ Subida completa: %d filas en pestaña '%s'.",
             len(filas), NOMBRE_PESTANA_DESTINO)


# =====================================================================
# 6. MAIN
# =====================================================================
def main():
    log.info("=" * 60)
    log.info("INICIO: Pipeline Talla Top desde KARDEX")
    log.info("=" * 60)

    cliente = autenticar()
    df_kardex = leer_kardex(cliente)
    df_resultado = procesar(df_kardex)
    subir_a_sheets(cliente, df_resultado)

    log.info("=" * 60)
    log.info("FIN: Pipeline Talla Top completado exitosamente")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
