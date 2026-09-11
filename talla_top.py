"""
=====================================================================
 PIPELINE: KARDEX filtrado → Google Sheets → Looker Studio
=====================================================================

Lee la hoja KARDEX (privada, vía API con Service Account), aplica dos
filtros sobre los datos originales y sube el resultado tal cual a la
pestaña "Kardex_Filtrado" de la hoja destino (mismos encabezados y
misma disposición de columnas que el KARDEX original).

Filtros:
  1) TIPO_MOVIMIENTO == "SALIDA"        (columna B)
  2) Nº PEDIDO empieza con "T0"          (columna H)
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
log = logging.getLogger("kardex_filtrado")

# =====================================================================
# 1. CONFIGURACIÓN — desde variables de entorno (Secrets de GitHub)
# =====================================================================
SHEET_ID_KARDEX = os.environ["SHEET_ID_KARDEX"]
GID_KARDEX = os.environ["GID_KARDEX"]
SHEET_ID_DESTINO = os.environ["SHEET_ID_DESTINO"]
CREDENCIALES_JSON = os.environ["GOOGLE_CREDENTIALS_JSON"]

NOMBRE_PESTANA_DESTINO = "Kardex_Filtrado"

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
# 3. LECTURA DE LA HOJA KARDEX
# =====================================================================
def leer_kardex(cliente: gspread.Client):
    """
    Lee TODOS los valores de la hoja KARDEX (incluyendo encabezados)
    usando la API de Google Sheets.

    Devuelve (encabezado: list, filas: list[list]) con el layout
    idéntico al de la hoja original.
    """
    log.info("Abriendo hoja KARDEX...")
    libro = cliente.open_by_key(SHEET_ID_KARDEX)

    # Buscar la hoja por gid
    hoja = None
    for ws in libro.worksheets():
        if str(ws.id) == GID_KARDEX:
            hoja = ws
            break

    if hoja is None:
        try:
            hoja = libro.worksheet("KARDEX")
        except gspread.exceptions.WorksheetNotFound:
            log.error("No se encontró la hoja con gid=%s ni con nombre 'KARDEX'.", GID_KARDEX)
            sys.exit(1)

    log.info("Leyendo hoja '%s' (gid=%s)...", hoja.title, hoja.id)
    valores = hoja.get_all_values()  # incluye encabezado como primera fila

    if not valores:
        log.error("La hoja KARDEX está vacía. Abortando.")
        sys.exit(1)

    encabezado = valores[0]
    filas = valores[1:]
    log.info("KARDEX leída: %d filas de datos, %d columnas.",
             len(filas), len(encabezado))
    return encabezado, filas


# =====================================================================
# 4. PROCESAMIENTO — solo filtros, sin transformar layout
# =====================================================================
def filtrar(filas: list) -> list:
    """
    Aplica los dos filtros sobre las filas crudas del KARDEX
    (listas de strings) SIN alterar el layout de columnas.

    Filtros:
      - columna B (índice 1): TIPO_MOVIMIENTO == "SALIDA"
      - columna H (índice 7): Nº PEDIDO empieza con "T0"

    Devuelve la lista de filas que cumplen ambas condiciones.
    """
    idx_movimiento = 1   # B
    idx_pedido = 7       # H

    resultado = []
    for fila in filas:
        # Asegurar longitud mínima por si alguna fila viene incompleta
        if len(fila) <= max(idx_movimiento, idx_pedido):
            continue

        movimiento = str(fila[idx_movimiento]).strip().upper()
        pedido = str(fila[idx_pedido]).strip().upper()

        if movimiento == "SALIDA" and pedido.startswith("T0"):
            resultado.append(fila)

    log.info("Filas tras filtros (SALIDA + T0): %d de %d",
             len(resultado), len(filas))
    return resultado


# =====================================================================
# 5. SUBIDA A GOOGLE SHEETS
# =====================================================================
def subir_a_sheets(cliente: gspread.Client,
                   encabezado: list,
                   filas: list) -> None:
    """Sube encabezado + filas filtradas a la pestaña destino."""
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
            title=NOMBRE_PESTANA_DESTINO, rows=1000, cols=max(10, len(encabezado))
        )

    # Estructura final: [encabezado] + filas
    todas_las_filas = [encabezado] + filas

    # Redimensionar si hace falta
    filas_necesarias = len(todas_las_filas) + 10
    filas_actuales = hoja.row_count
    if filas_necesarias > filas_actuales:
        log.info("Redimensionando pestaña de %d a %d filas...",
                 filas_actuales, filas_necesarias)
        hoja.resize(rows=filas_necesarias)

    # Subir por lotes
    total = len(todas_las_filas)
    for inicio in range(0, total, TAMANO_LOTE_SUBIDA):
        lote = todas_las_filas[inicio: inicio + TAMANO_LOTE_SUBIDA]
        rango = f"A{inicio + 1}"
        hoja.update(values=lote, range_name=rango, value_input_option="USER_ENTERED")
        log.info("Subidas filas %d a %d de %d.",
                 inicio + 1, min(inicio + TAMANO_LOTE_SUBIDA, total), total)

    log.info("✅ Subida completa: %d filas en pestaña '%s'.",
             len(filas), NOMBRE_PESTANA_DESTINO)


# =====================================================================
# 6. MAIN
# =====================================================================
def main():
    log.info("=" * 60)
    log.info("INICIO: Pipeline KARDEX filtrado")
    log.info("=" * 60)

    cliente = autenticar()
    encabezado, filas = leer_kardex(cliente)
    filas_filtradas = filtrar(filas)
    subir_a_sheets(cliente, encabezado, filas_filtradas)

    log.info("=" * 60)
    log.info("FIN: Pipeline completado exitosamente")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
