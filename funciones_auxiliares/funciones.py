import pandas as pd
import numpy as np
import re
from datetime import datetime

def asignar_marca(desc, marca_actual):
    """
    Asigna la marca correspondiente según un prefijo en la descripción, solo si la marca actual está vacía.

    Parámetros:
        desc (str): Descripción del producto.
        marca_actual (str): Valor actual de la marca.

    Returns:
        str: Marca asignada según el prefijo, o la marca original si ya estaba presente.
    """
    marca_mapping = {
        "NAT": "001 - NATURESSE",
        "JDE": "002 - JARDIN DE EVA",
        "CED": "003 - CEDRELA",
        "AMA": "004 - AMATISTA",
        "INM": "006 - INMARCESIBLE",
        "MED": "007 - MEDELA",
        "AWA": "008 - AWARA",
        "PER": "010 - PERSONALIZADO"
    } 
  
    if pd.isna(marca_actual): 
        for key, value in marca_mapping.items():
            if isinstance(desc, str) and desc.startswith(key):
                return value
    return marca_actual


def tiene_x(desc):
    """
    Verifica si la descripción contiene una cantidad precedida por 'X' (mayúscula o minúscula).

    Parámetros:
        desc (str): Descripción del producto.

    Returns:
        bool: True si se encuentra una coincidencia tipo "X número", False si no.
    """
    return bool(re.search(r"(?i)\s+X\s*\d", str(desc)))

def extraer_cantidad(desc):
    """
    Extrae la cantidad del producto que aparece después de una 'X', como "X12UND" o "x 400ml".

    Parámetros:
        desc (str): Descripción del producto.

    Returns:
        str: La cantidad encontrada, o "NA" si no se encuentra.
    """
    if pd.isna(desc) or not isinstance(desc, str): 
        return "NA"
    
    match = re.search(r"X\s*(\d+\S*)", desc, re.IGNORECASE)
    return match.group(1) if match else "NA"

def extraer_producto(desc):
    """
    Extrae el nombre del producto antes de una indicación de cantidad como "X 12UND".
    
    Parámetros:
        desc (str): Descripción del producto.

    Returns:
        str: Parte de la descripción correspondiente al producto, sin la parte de la cantidad.
    """
    if pd.isna(desc) or not isinstance(desc, str): 
        return desc
    
    partes = re.split(r"\s+X\s*\d", desc, maxsplit=1)
    return partes[0].strip() if partes else desc.strip()
  

def normalizar_cantidad(cantidad):
    """
    Normaliza cadenas que representan cantidades para asegurar uniformidad en el formato.

    Parámetros:
        cantidad (str): Cantidad original (ej. "500 ML").

    Returns:
        str: Cantidad normalizada (ej. "500ML"), o "NA" si está vacía o nula.
    """
    cantidad_replacements = {
        "2 UND": "2UND",
        "500 ML": "500ML",
        "400 ML": "400ML"
    }
    if pd.isna(cantidad) or cantidad.strip() == "":
        return "NA"
    cantidad = cantidad.upper().strip()
    for old, new in cantidad_replacements.items():
        cantidad = re.sub(rf"\b{re.escape(old)}\b", new, cantidad)
    cantidad = re.sub(r"\s+", " ", cantidad)
    return cantidad

def calcular_cantidad_sell_int(row):
    """
    Calcula la cantidad interna de producto considerando multiplicadores especiales para Makro.

    Parámetros:
        row (pd.Series): Fila de un DataFrame que contiene las claves 'Cantidad inv.', 'Razon social', 'Marca' y 'Cantidades'.

    Returns:
        int: Cantidad ajustada según el multiplicador correspondiente.
    """

    makro_multiplicadores = {
        "15GR X 12UND X 32PQ": 32,
        "48UND": 48,
        "6UND": 6
    }

    cantidad = int(row["Cantidad inv."]) 
    
    if row["Razon social"] == "MAKRO SUPERMAYORISTA SAS" and row["Marca"] == "001 - NATURESSE":
        return cantidad * makro_multiplicadores.get(row["Cantidades"], 24)
    return cantidad


def normalizar_producto(producto):
    """
    Normaliza la descripción del producto unificando sinónimos, errores tipográficos y abreviaciones.

    Parámetros:
        producto (str): Nombre o descripción del producto.

    Returns:
        str: Producto normalizado, sin errores comunes ni variaciones innecesarias.
    """

    replacements = {
        "CREM": "CREMA", "CREMAA": "CREMA",
        "HUME": "HUMEC", "HUMECC": "HUMEC",
        "LIQ": "LIQU", "LIQUU": "LIQU",
        "FLORY": "FLOR Y", "ANTIOX": "ANTI",
        "ANTIMOSQ": "ANTI MOSQ", "ANTMOSQ": "ANTI MOSQ",
        "JDE COMB CONC": "JDE COMB CONE",
        "RESPIRA": "RESP", "RELAJATE": "RELA",
        "SALES": "SALE", "EFER ACTI": "EFER ACT",
        "EFERV": "EFER", "PER JAB VEG SPA": "PER JAB VEG",
        "NAT ACOND": "NAT ACON", "NAT GEL DUCH": "NAT GEL DUCHA",
        "JABO": "JABON", "JABONN": "JABON",
        "ANTIBAC": "ANTI", "SHAM KER": "SHAM KERA",
        "SHAM KERAA": "SHAM KERA", "NAT SPLA SLEE NIGH": "NAT SPLASH SLEEPFUL NIGHT"
    }
    
    if pd.isna(producto):
        return np.nan
    producto = producto.upper().strip()
    for key, value in replacements.items():
        producto = re.sub(rf"\b{re.escape(key)}\b", value, producto)
    producto = re.sub(r"\s+", " ", producto)
    return producto
