"""
Archivo de configuración para el procesamiento de datos Naturesse.
Centraliza todas las configuraciones y constantes del proyecto.
"""

import os
import pandas as pd

RUTA_BASE = 'C:/Users/Usuario/Downloads/naturesse/'

RUTAS = {
    'BASE': RUTA_BASE,
    'SALIDA': os.path.join(RUTA_BASE, 'salida'),
    'DATOS_PRUEBAS': 'datos/tablas_entrada/',
    'DATOS_CONSOLIDADO': 'datos/tablas_consolidados/',
}

# NOMBRES DE ARCHIVOS DE ENTRADA
ARCHIVOS_ENTRADA = {
    'VENTAS': "Tabla ventas.xlsx",
    'VENTAS_RFM': "Tabla ventas rfm.xlsx",
    'CLIENTES': "Tabla de clientes.xlsx",
    'COSTOS': "Tabla costos.xlsx",
    'METAS': "Tabla Metas.xlsx",
    'MARCA_REFERENCIA': "Tabla marca, referencia y producto.xlsx",
    
    'CLIENTE_PROMOTORA': "MAR_PROMOTORA.xlsx",
    'CLIENTE_MAKRO': "ABR_MAKRO.xlsx",
    'CLIENTE_FARMATODO': "JUN_FARMATODO.xlsx"
}


# NOMBRES DE ARCHIVOS CONSOLIDADOS
ARCHIVOS_CONSOLIDADOS = {
    'VENTAS_CONSOLIDADOS': "Tabla ventas.xlsx",
    'COSTOS_CONSOLIDADOS': "Consolidado Costos.xlsx",
    'FAMILIA': "tabla_familia.xlsx",
    
    'CONSOLIDADO_SELL_OUT': "Tiendas Sell out.xlsx",
    'CONSOLIDADO_SELL_OUT_CIUDADES': "Tiendas Sell out ciudad descripcion.xlsx",
}

# NOMBRES DE ARCHIVOS DE SALIDA
ARCHIVOS_SALIDA = {
    'VENTAS_PROCESADAS': "tabla_ventas_procesada.xlsx",
    'CONTEO_LINEA_MERCADO': "conteo_linea_mercado.xlsx",
    'MARCA_REFERENCIA_PRODUCTO': "tabla_marca_referencia_producto.xlsx",
    'COSTOS_CONSOLIDADOS': "consolidado_costos.xlsx",
    'SELL_OUT_CIUDAD': "tiendas_sell_out_ciudad_descripcion.xlsx"
}

# CONFIGURACIÓN DE FECHAS
FECHAS = {
    'ULTIMA_FECHA_CONSOLIDADO': pd.Timestamp("2025-01-01")
}

def obtener_ruta_completa(tipo_archivo, nombre_archivo=None):
    """
    Obtiene la ruta completa para un archivo específico.
    
    Args:
        tipo_archivo (str): Tipo de archivo ('entrada', 'salida', 'base')
        nombre_archivo (str): Nombre del archivo (opcional)
        
    Returns:
        str: Ruta completa del archivo
    """
    if tipo_archivo == 'base':
        return RUTAS['BASE']
    elif tipo_archivo == 'entrada':
        return RUTAS['BASE'] + RUTAS['DATOS_PRUEBAS']
    elif tipo_archivo == 'consolidado':
        return RUTAS['BASE'] + RUTAS['DATOS_CONSOLIDADO']
    elif tipo_archivo == 'salida':
        return RUTAS['SALIDA']
    else:
        return RUTAS['BASE']


# Configuración por defecto para pandas
PANDAS_CONFIG = {
    'display.max_columns': None,
    'display.width': None,
    'display.max_colwidth': 50,
    'display.precision': 2
}

def aplicar_configuracion_pandas():
    """Aplica la configuración por defecto de pandas."""
    import pandas as pd
    for opcion, valor in PANDAS_CONFIG.items():
        pd.set_option(opcion, valor)