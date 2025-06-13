import pandas as pd
import numpy as np
import re
from datetime import datetime
import calendar
import os
import funciones
import sys
import logging

from config import ( ARCHIVOS_SALIDA )
from funciones_principales import (configurar_rutas, cargar_datos, limpiar_formato_numerico_excel, limpiar_dataframe_excel)


## PROCESAMIENTO DE VENTAS
def procesar_ventas(tabla_ventas, ultima_fecha, path):
    """
    Procesa la tabla de ventas aplicando todas las transformaciones necesarias.
    
    Args:
        tabla_ventas (pd.DataFrame): Tabla de ventas sin procesar
        ultima_fecha (pd.Timestamp): Fecha límite para filtrar ventas
        path (str): Ruta base para guardar archivos
        
    Returns:
        pd.DataFrame: Tabla de ventas procesada
    """
    logging.info("Iniciando procesamiento de ventas...")
    
    # Limpiar y convertir fechas
    tabla_ventas["Fecha"] = tabla_ventas["Fecha"].apply(lambda x: x.strip() if isinstance(x, str) else x)
    tabla_ventas["Fecha"] = pd.to_datetime(tabla_ventas["Fecha"], errors="coerce")

    
    # Filtrar fechas
    registros_antes = len(tabla_ventas)
    tabla_ventas = tabla_ventas[tabla_ventas["Fecha"] > ultima_fecha]
    registros_despues = len(tabla_ventas)
    logging.info(f"Filtrado por fecha: {registros_antes} -> {registros_despues} registros")
    
    # Renombrar columnas usando configuración
    tabla_ventas.rename(columns={"Desc. item": "Desc.item"}, inplace=True)
    
    # Agregar columna de tipo de identificación
    tabla_ventas["Tipo de identificacion"] = np.nan
    
    # Reordenar columnas
    cols = list(tabla_ventas.columns)
    cols.insert(3, cols.pop(cols.index("Tipo de identificacion")))
    tabla_ventas = tabla_ventas[cols]
    
    # Seleccionar solo las primeras 12 columnas
    tabla_ventas = tabla_ventas.iloc[:, :12]
    
    # Aplicar funciones de procesamiento
    tabla_ventas["Marca"] = tabla_ventas.apply(
        lambda row: funciones.asignar_marca(row["Desc.item"], row.get("Marca", np.nan)), 
        axis=1
    )
    
    tabla_ventas["TieneX"] = tabla_ventas["Desc.item"].apply(funciones.tiene_x)
    tabla_ventas["Cantidades"] = tabla_ventas["Desc.item"].apply(funciones.extraer_cantidad)
    tabla_ventas["Producto"] = tabla_ventas["Desc.item"].apply(funciones.extraer_producto)
    
    # Eliminar columna auxiliar
    tabla_ventas.drop(columns=["TieneX"], inplace=True)
    
    # Normalizar productos
    tabla_ventas["Producto_normalizado"] = tabla_ventas["Producto"].apply(funciones.normalizar_producto)
    tabla_ventas.drop(columns=["Producto"], inplace=True)
    tabla_ventas.rename(columns={"Producto_normalizado": "Producto"}, inplace=True)
    
    # Normalizar cantidades
    tabla_ventas["Cantidades"] = tabla_ventas["Cantidades"].apply(funciones.normalizar_cantidad)
    tabla_ventas["Cantidades"] = tabla_ventas.apply(
        lambda row: "1UND" if row["Cantidades"] == "NA" and not pd.isna(row["Desc.item"]) else row["Cantidades"], 
        axis=1
    )
    
    # Calcular cantidad sell int
    tabla_ventas["Cantidad inv.sell_Int"] = tabla_ventas.apply(funciones.calcular_cantidad_sell_int, axis=1)
    
    return tabla_ventas
    
def filtrar_registros_nuevos_ventas(df_entrada, df_consolidado):
    """
    Filtra los registros nuevos del archivo de entrada comparando con el consolidado histórico.
    """
    print("Verificando y estandarizando formatos de fecha...")

    campos_clave = [
        'Marca', 
        'Linea de mercado', 
        'Fecha', 
        'Referencia', 
        'Cantidad inv.',
        'Valor bruto local'
    ]

    # Convertir fechas a datetime para comparación
    df_entrada['Fecha'] = pd.to_datetime(df_entrada['Fecha'])
    df_consolidado['Fecha'] = pd.to_datetime(df_consolidado['Fecha'])
    
    # Obtener el último registro normalizado del consolidado
    ultimo_registro_normalizado = df_consolidado[campos_clave].astype(str)
    ultimo_registro_normalizado = limpiar_dataframe_excel(ultimo_registro_normalizado).iloc[-1]

    # 1. Encontrar fecha máxima del consolidado
    fecha_max_consolidado = df_consolidado['Fecha'].max()
    print(f"Fecha máxima en consolidado: {fecha_max_consolidado}")

    # 2. Filtrar entrada por fecha >= fecha máxima
    df_filtrado_fecha = df_entrada[df_entrada['Fecha'] >= fecha_max_consolidado].copy()
    print(f"Registros después del filtro por fecha: {len(df_filtrado_fecha)}")

    # Resetear índice para asegurar alineación
    df_filtrado_fecha = df_filtrado_fecha.reset_index(drop=True)

    # 3. Normalizar y limpiar datos
    entrada_normalizada = df_filtrado_fecha[campos_clave].astype(str)
    entrada_normalizada = limpiar_dataframe_excel(entrada_normalizada).reset_index(drop=True)

    print(f"Rango de fechas en entrada: {df_entrada['Fecha'].min()} a {df_entrada['Fecha'].max()}")

    # 4. Buscar coincidencias exactas
    coincidencias = entrada_normalizada.eq(ultimo_registro_normalizado).all(axis=1)
    print("Coincidencias exactas encontradas:", coincidencias.sum())

    """ # 5. Comparar columnas diferentes (opcional, útil para debug)
    if not coincidencias.any():
        diferencias = entrada_normalizada != ultimo_registro_normalizado
        columnas_diferentes = diferencias.any(axis=0)
        print("Columnas con diferencias respecto al último registro:")
        print(columnas_diferentes[columnas_diferentes].index.tolist())"""

    # 6. Tomar los registros nuevos correctamente después del match
    if coincidencias.any():
        idx = coincidencias[coincidencias].index[0]
        df_nuevos = df_filtrado_fecha.loc[idx+1:].copy()
        print(f"Se encontró el último registro del consolidado en la entrada. Nuevos desde el índice {idx + 1}.")
    else:
        df_nuevos = df_filtrado_fecha.copy()
        print("El último registro del consolidado NO se encontró en la entrada. Se toman todos como nuevos.")

    df_nuevos = df_nuevos.rename(columns={"EAN 13": "EAN"})
    return df_nuevos


## PROCESAMIENTO DE CLIENTES
def procesar_clientes(tabla_clientes):
    """
    Procesa la tabla de clientes eliminando duplicados.
    
    Args:
        tabla_clientes (pd.DataFrame): Tabla de clientes sin procesar
        
    Returns:
        pd.DataFrame: Tabla de clientes procesada
    """
    # Eliminar duplicados por número de identificación
    tabla_clientes = tabla_clientes.drop_duplicates(subset=["nit"])
    
    return tabla_clientes

## PROCESAMIENTO LINEA MERCADO
def crear_conteo_linea_mercado(tabla_ventas):
    """
    Crea el conteo de compras por línea de mercado agrupado por semana, año y mes.
    
    Args:
        tabla_ventas (pd.DataFrame): Tabla de ventas procesada
        
    Returns:
        pd.DataFrame: Conteo de compras por línea de mercado
    """
    # Diccionario de conversión de meses
    month_mapping = {
    # Inglés
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
    
    # Español
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
  
    # Asegurar que la columna Fecha es tipo datetime
    tabla_ventas["Fecha"] = pd.to_datetime(tabla_ventas["Fecha"], errors='coerce')
    
    # Crear columna de semana
    tabla_ventas["Semana"] = tabla_ventas["Fecha"].dt.to_period("W").apply(lambda r: r.start_time)
    
    # Extraer mes y año
    tabla_ventas["Mes"] = tabla_ventas["Semana"].dt.strftime("%B").str.lower()
    tabla_ventas["Año"] = tabla_ventas["Semana"].dt.year
    tabla_ventas["Mes_num"] = tabla_ventas["Mes"].map(month_mapping)
    
    # Agrupar y contar compras
    ComprasSemanaAñoMes = tabla_ventas.groupby([
        "Año", "Mes", "Semana", "Linea de mercado", "Mes_num", "Razon social"
    ]).size().reset_index(name="numCompras")

    
    return ComprasSemanaAñoMes
    
## PROCESAMIENTO A TABLA MARCA, REFERENCIA 

def crear_tabla_marca_referencia(Tabla_marca_referencia_y_producto, tabla_ventas):
    """
    Crea la tabla de marca, referencia y producto.
    
    Returns:
        pd.DataFrame: Tabla de marca, referencia y producto
    """

    # Fusionar con la tabla de ventas en la columna "Referencia"
    Tabla_marca_referencia_y_producto = tabla_ventas.merge(
        Tabla_marca_referencia_y_producto, on="Referencia", how="outer", suffixes=('_ventas', '_ref')
    )
    
    # Unificar las dos columnas EAN si existen
    if 'EAN_ventas' in Tabla_marca_referencia_y_producto.columns and 'EAN_ref' in Tabla_marca_referencia_y_producto.columns:
        Tabla_marca_referencia_y_producto['EAN'] = Tabla_marca_referencia_y_producto['EAN_ventas'].combine_first(
            Tabla_marca_referencia_y_producto['EAN_ref']
        )
    elif 'EAN' not in Tabla_marca_referencia_y_producto.columns and 'EAN 13' in Tabla_marca_referencia_y_producto.columns:
        Tabla_marca_referencia_y_producto = Tabla_marca_referencia_y_producto.rename(columns={"EAN 13": "EAN"})
    
    # Renombrar columna de descripción si aplica
    Tabla_marca_referencia_y_producto = Tabla_marca_referencia_y_producto.rename(
        columns={"Desc.item": "Desc._item.Sell.Out"}
    )
    
    # Seleccionar columnas necesarias
    Tabla_marca_referencia_y_producto = Tabla_marca_referencia_y_producto[
        ["MARCA", "Referencia", "Desc._item", "EAN", "Desc._item.Sell.Out"]
    ]
    
    # Mapeo de marcas
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
    
    def asignar_marca(desc):
        if pd.isna(desc):
            return np.nan
        for key, value in marca_mapping.items():
            if desc.startswith(key):
                return value
        return np.nan
    
    # Aplicar asignación de marca
    Tabla_marca_referencia_y_producto["MARCA"] = Tabla_marca_referencia_y_producto["Desc._item"].apply(asignar_marca)
    
    # Completar valores faltantes
    Tabla_marca_referencia_y_producto["Desc._item.Sell.Out"] = Tabla_marca_referencia_y_producto.apply(
        lambda row: row["Desc._item"] if pd.isna(row["Desc._item.Sell.Out"]) else row["Desc._item.Sell.Out"], 
        axis=1
    )

    Tabla_marca_referencia_y_producto = Tabla_marca_referencia_y_producto.dropna(subset=["MARCA"])
    Tabla_marca_referencia_y_producto = Tabla_marca_referencia_y_producto.drop_duplicates(subset=["MARCA", "EAN"])
    
    return Tabla_marca_referencia_y_producto


def consolidar_costos_historicos(datos, tabla_marca_referencia):
    
    tabla_costos_consolidado = datos['tabla_consolidado_costos']

    if datos.get('tabla_de_costos') is None:
        return tabla_costos_consolidado
    
    tabla_costo_entrada = datos['tabla_de_costos']

    # ✅ Renombrar columnas en la tabla de entrada (año actual)
    tabla_costo_entrada = tabla_costo_entrada.rename(
        columns={"Costo prom. uni.": "Costo prom.", "Desc. item": "Desc.item"}
    )[["Marca", "Referencia", "Desc.item", "Instalación", "U.M.", "Costo prom.", "Mes", "Año"]]

    # ✅ Asegurar columna "Incluir referencia(Si/No)" en tabla de entrada
    if "Incluir referencia(Si/No)" not in tabla_costo_entrada.columns:
        tabla_costo_entrada["Incluir referencia(Si/No)"] = None

    # ✅ Unir tabla histórica con marcas
    consolidado_costo = tabla_costos_consolidado.merge(tabla_marca_referencia, on="Referencia", how="left")

    # ✅ Resolver nombres conflictivos de columnas
    if "Incluir referencia(Si/No).y" in consolidado_costo.columns:
        consolidado_costo = consolidado_costo.rename(columns={"Incluir referencia(Si/No).y": "Incluir referencia(Si/No)"})
    elif "Incluir referencia(Si/No)" not in consolidado_costo.columns:
        consolidado_costo["Incluir referencia(Si/No)"] = None

    # ✅ Filtrar columnas necesarias
    consolidado_costo_filtrado = consolidado_costo[
        ["Referencia", "Desc.item", "Instalación", "U.M.", "Costo prom.", "Mes", "Año", "Marca", "Incluir referencia(Si/No)"]
    ]

    # ✅ Filtrar registros de años anteriores al actual
    max_year = tabla_costo_entrada["Año"].max()
    consolidado_costo_filtrado = consolidado_costo_filtrado[consolidado_costo_filtrado["Año"] < max_year]

    # ✅ Unir históricos + año actual
    consolidado_de_costo = pd.concat([consolidado_costo_filtrado, tabla_costo_entrada], ignore_index=True)

    return consolidado_de_costo
