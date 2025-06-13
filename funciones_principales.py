import pandas as pd
import numpy as np
import re
from datetime import datetime
import calendar
import os
import funciones
import sys
import logging

# Importar configuración
from config import (
    RUTAS, ARCHIVOS_ENTRADA, ARCHIVOS_CONSOLIDADOS, ARCHIVOS_SALIDA, FECHAS, obtener_ruta_completa, aplicar_configuracion_pandas
)

def configurar_rutas():
    """
    Configura las rutas y nombres de archivos necesarios para el procesamiento.
    
    Returns:
        dict: Diccionario con rutas y nombres de archivos
    """
    config = {
        'path': RUTAS['BASE'],
        'salida': RUTAS['SALIDA'],
        'archivo_ventas': ARCHIVOS_ENTRADA.get('VENTAS'),
        'archivo_clientes': ARCHIVOS_ENTRADA.get('CLIENTES'),
        'archivo_costos': ARCHIVOS_ENTRADA.get('COSTOS'),
        'archivo_marca_referencia': ARCHIVOS_ENTRADA.get('MARCA_REFERENCIA'),
        'archivo_metas': ARCHIVOS_ENTRADA.get('METAS'),

        # Estos pueden o no existir
        'archivo_promotora': ARCHIVOS_ENTRADA.get('CLIENTE_PROMOTORA'),
        'archivo_makro': ARCHIVOS_ENTRADA.get('CLIENTE_MAKRO'),
        'archivo_farmatodo': ARCHIVOS_ENTRADA.get('CLIENTE_FARMATODO'),
        
        'archivo_tabla_sell_out': ARCHIVOS_CONSOLIDADOS.get('CONSOLIDADO_SELL_OUT'),
        'archivo_tabla_sell_out_ciudades': ARCHIVOS_CONSOLIDADOS.get('CONSOLIDADO_SELL_OUT_CIUDADES'),
        'archivo_ventas_consolidado': ARCHIVOS_CONSOLIDADOS.get('VENTAS_CONSOLIDADOS'),
        'archivo_costos_consolidado': ARCHIVOS_CONSOLIDADOS.get('COSTOS_CONSOLIDADOS'),

        'ultima_fecha': FECHAS.get('ULTIMA_FECHA_CONSOLIDADO')
    }
    
    aplicar_configuracion_pandas()
    return config


def cargar_datos(config):
    """
    Carga todas las tablas necesarias desde los archivos Excel.
    
    Args:
        config (dict): Configuración con rutas y nombres de archivos
        
    Returns:
        dict: Diccionario con todas las tablas cargadas
    """
    datos = {}
    ruta_datos = obtener_ruta_completa('entrada')
    ruta_datos_consolidado = obtener_ruta_completa('consolidado')
    
    try:
        # Cargar tabla de ventas

        ##CONSOLIDADOS

        logging.info("Cargando tabla de ventas consolidado...")
        datos['tabla_de_ventas_consolidado'] = pd.read_excel(
            ruta_datos_consolidado + config['archivo_ventas_consolidado']
        )

        logging.info("Cargando tabla de consolidado costos...")
        datos['tabla_consolidado_costos'] = pd.read_excel(
            ruta_datos_consolidado + config['archivo_costos_consolidado']
        )

        logging.info("Cargando tabla de Sell out...")
        datos['tabla_sell_out_consolidado'] = pd.read_excel(
            ruta_datos_consolidado + config['archivo_tabla_sell_out']
        )

        logging.info("Cargando tabla de Sell out ciudades ...")
        datos['tabla_sell_out_ciudades_consolidado'] = pd.read_excel(
            ruta_datos_consolidado + config['archivo_tabla_sell_out_ciudades']
        )

        ## ENTRADA

        if config.get('archivo_ventas'):
            logging.info("Cargando tabla de ventas...")
            datos['tabla_ventas'] = pd.read_excel(
                ruta_datos + config['archivo_ventas'], 
                dtype=str
            )
        
        if config.get('archivo_clientes'):
            logging.info("Cargando tabla de clientes...")
            datos['tabla_clientes'] = pd.read_excel(
                ruta_datos + config['archivo_clientes']
            )
        
        if config.get('archivo_metas'):
            logging.info("Cargando tabla de cumplimiento...")
            datos['tabla_de_cumplimiento'] = pd.read_excel(
                ruta_datos + config['archivo_metas']
            )
        
        if config.get('archivo_costos'):
            logging.info("Cargando tabla de costos...")
            datos['tabla_de_costos'] = pd.read_excel(
                ruta_datos + config['archivo_costos']
            )

        if config.get('archivo_marca_referencia'):
            logging.info("Cargando tabla de marca referencia...")
            datos['tabla_mercado_referencia'] = pd.read_excel(
                ruta_datos + config['archivo_marca_referencia']
            )

        ### clientes grandes

        if config.get('archivo_promotora'):
            logging.info("Cargando tabla PROMOTORA...")
            datos['tabla_cliente_promotora'] = pd.read_excel(
                ruta_datos + config['archivo_promotora']
            )
        
        if config.get('archivo_makro'):
            logging.info("Cargando tabla MAKRO...")
            datos['tabla_cliente_makro'] = pd.read_excel(
                ruta_datos + config['archivo_makro'],
                skiprows=18, header=[0, 1]
            )
        
        if config.get('archivo_farmatodo'):
            logging.info("Cargando tabla FARMATODO...")
            datos['tabla_cliente_farmatodo'] = pd.read_excel(
                ruta_datos + config['archivo_farmatodo']
            )

        logging.info("Todas las tablas cargadas exitosamente")
        
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        raise
    except Exception as e:
        logging.error(f"Error al cargar datos: {e}")
        raise
    
    return datos

def limpiar_formato_numerico_excel(serie):
    """
    Limpia formato numérico específico de Excel que puede causar problemas
    como separadores de miles, espacios, y otros caracteres de formato.
    
    Args:
        serie (pd.Series): Serie con valores potencialmente mal formateados
        
    Returns:
        pd.Series: Serie con formato numérico limpio
    """
    if serie.dtype == 'object':
        # Crear copia para no modificar original
        serie_limpia = serie.copy()
        
        # Limpiar solo valores no nulos
        mask_no_nulos = serie_limpia.notna()
        
        if mask_no_nulos.any():
            # Convertir a string para limpieza
            valores_str = serie_limpia[mask_no_nulos].astype(str)
            
            # Remover espacios
            valores_str = valores_str.str.replace(' ', '')
            
            # Remover separadores de miles comunes
            valores_str = valores_str.str.replace(',', '')
            valores_str = valores_str.str.replace('.', '', regex=False)  # Solo si es separador de miles
            
            # Detectar si hay demasiados ceros (posible error de formato)
            # Esto detecta números como 472800000 que deberían ser 4728
            def corregir_ceros_extra(valor_str):
                if pd.isna(valor_str) or valor_str == 'nan':
                    return valor_str
                
                # Si el string termina en muchos ceros, posiblemente está mal formateado
                if len(valor_str) > 4 and valor_str.endswith('00000'):
                    # Contar ceros al final
                    ceros_finales = 0
                    for i in range(len(valor_str)-1, -1, -1):
                        if valor_str[i] == '0':
                            ceros_finales += 1
                        else:
                            break
                    
                    # Si tiene 5 o más ceros al final, probablemente está mal
                    if ceros_finales >= 5:
                        # Remover los ceros extra (dejar máximo 2 para decimales)
                        valor_sin_ceros = valor_str.rstrip('0')
                        if len(valor_sin_ceros) > 0:
                            return valor_sin_ceros
                
                return valor_str
            
            # Aplicar corrección de ceros
            valores_str = valores_str.apply(corregir_ceros_extra)
            
            # Actualizar solo los valores que no eran nulos
            serie_limpia[mask_no_nulos] = valores_str
        
        return serie_limpia
    
    return serie


def limpiar_dataframe_excel(df):
    """
    Función para limpiar un DataFrame que proviene de un archivo Excel,
    eliminando formato y dejando solo los datos limpios.
    
    Args:
        df (pd.DataFrame): DataFrame original del Excel
        
    Returns:
        pd.DataFrame: DataFrame limpio
    """
    # Crear una copia para no modificar el original
    df_limpio = df.copy()
    
    # 1. Eliminar filas completamente vacías
    df_limpio = df_limpio.dropna(how='all')
    
    # 2. Eliminar columnas completamente vacías
    df_limpio = df_limpio.dropna(axis=1, how='all')
    
    # 3. Limpiar nombres de columnas (más conservador)
    if df_limpio.columns.dtype == 'object':
        # Quitar espacios en blanco al inicio y final
        df_limpio.columns = df_limpio.columns.str.strip()
        # Reemplazar espacios múltiples por uno solo
        df_limpio.columns = df_limpio.columns.str.replace(r'\s+', ' ', regex=True)
        # Solo quitar caracteres realmente problemáticos (no puntos ni guiones)
        df_limpio.columns = df_limpio.columns.str.replace(r'[^\w\s\.\-_]', '', regex=True)
    
    # 4. Limpiar datos en cada columna
    for columna in df_limpio.columns:
        if df_limpio[columna].dtype == 'object':
            # Convertir a string y limpiar espacios
            df_limpio[columna] = df_limpio[columna].astype(str).str.strip()
            
            # Reemplazar 'nan', 'None', cadenas vacías por NaN
            df_limpio[columna] = df_limpio[columna].replace(['nan', 'None', '', 'NaN'], np.nan)
            
            # Limpiar espacios múltiples
            df_limpio[columna] = df_limpio[columna].str.replace(r'\s+', ' ', regex=True)
            
            # Limpiar formato numérico de Excel antes de convertir
            df_limpio[columna] = limpiar_formato_numerico_excel(df_limpio[columna])
            
            # Intentar convertir a numérico si es posible (sin warnings)
            try:
                df_limpio[columna] = pd.to_numeric(df_limpio[columna])
            except (ValueError, TypeError):
                pass  # Mantener como texto si no se puede convertir
    
    # 5. Resetear índice
    df_limpio = df_limpio.reset_index(drop=True)
    
    # 6. Información del proceso de limpieza
    filas_eliminadas = len(df) - len(df_limpio)
    columnas_eliminadas = len(df.columns) - len(df_limpio.columns)
    
    print(f"Limpieza completada:")
    print(f"- Filas eliminadas: {filas_eliminadas}")
    print(f"- Columnas eliminadas: {columnas_eliminadas}")
    print(f"- Dimensiones finales: {df_limpio.shape}")
    
    return df_limpio