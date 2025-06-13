import pandas as pd
import numpy as np
import re
from datetime import datetime
import calendar
import os
import sys

# Función para asignar la marca basada en el prefijo
def asignar_marca(desc, marca_actual):
    # Diccionario de marcas basado en el prefijo del texto
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
  
  
  
    if pd.isna(marca_actual):  # Solo cambiar si la marca es NaN
        for key, value in marca_mapping.items():
            if isinstance(desc, str) and desc.startswith(key):
                return value
    return marca_actual  # Mantener el valor original si no hay coincidencias

# Función para detectar si la descripción contiene "X" seguido de un número
def tiene_x(desc):
    return bool(re.search(r"(?i)\s+X\s*\d", str(desc)))

# Función para extraer la cantidad
def extraer_cantidad(desc):
    """
    Extrae la cantidad de un producto si está precedida por "X" (mayúscula o minúscula).
    Si no hay coincidencias, devuelve "NA".
    """
    if pd.isna(desc) or not isinstance(desc, str):  # Manejar valores NaN o no string
        return "NA"
    
    match = re.search(r"X\s*(\d+\S*)", desc, re.IGNORECASE)  # Buscar "X" seguido de un número
    return match.group(1) if match else "NA"


# Función para extraer el nombre del producto sin la cantidad
def extraer_producto(desc):
    """
    Extrae el nombre del producto eliminando la parte después de 'X' (si existe).
    Si no hay 'X' en la cadena, devuelve la descripción original sin espacios extra.
    """
    if pd.isna(desc) or not isinstance(desc, str):  # Manejar valores NaN o no string
        return desc
    
    partes = re.split(r"\s+X\s*\d", desc, maxsplit=1)  # Dividir por " X " seguido de un número
    return partes[0].strip() if partes else desc.strip()
  
# Función para estandarizar las cantidades
def normalizar_cantidad(cantidad):
  
    # Diccionario de reemplazos para estandarizar cantidades
    cantidad_replacements = {
        "2 UND": "2UND",
        "500 ML": "500ML",
        "400 ML": "400ML"
    }
    if pd.isna(cantidad) or cantidad.strip() == "":
        return "NA"
    cantidad = cantidad.upper().strip()  # Convertir a mayúsculas y eliminar espacios extra
    for old, new in cantidad_replacements.items():
        cantidad = re.sub(rf"\b{re.escape(old)}\b", new, cantidad)  # Reemplazo exacto
    cantidad = re.sub(r"\s+", " ", cantidad)  # Eliminar múltiples espacios
    return cantidad

# Función para calcular la cantidad de inventario vendido
def calcular_cantidad_sell_int(row):
    
    # Diccionario de multiplicadores especificos para MAKRO
    makro_multiplicadores = {
        "15GR X 12UND X 32PQ": 32,
        "48UND": 48,
        "6UND": 6
    }
  
    if row["Razon social"] == "MAKRO SUPERMAYORISTA SAS":
        return row["Cantidad inv."] * makro_multiplicadores.get(row["Cantidades"], 24)
    return row["Cantidad inv."]

# Función para normalizar nombres de productos
def normalizar_producto(producto):
  
    # Diccionario de reemplazos para estandarizar nombres
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
    producto = producto.upper().strip()  # Convertir a mayúsculas y eliminar espacios iniciales/finales
    for key, value in replacements.items():
        producto = re.sub(rf"\b{re.escape(key)}\b", value, producto)  # Reemplazo exacto de palabras
    producto = re.sub(r"\s+", " ", producto)  # Eliminar múltiples espacios
    return producto

