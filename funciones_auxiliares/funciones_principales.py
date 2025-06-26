import pandas as pd
import numpy as np
import os
import funciones_auxiliares.funciones as funciones
import logging

# Importar configuración
from funciones_auxiliares.config import (
    RUTAS, ARCHIVOS_ENTRADA, ARCHIVOS_CONSOLIDADOS, ARCHIVOS_SALIDA, FECHAS, obtener_ruta_completa, aplicar_configuracion_pandas
)

def configurar_rutas():
    """
    Configura las rutas y nombres de archivos necesarios para el procesamiento de datos a partir de la configuración global.
    
    Returns:
        dict: Diccionario con rutas y nombres de archivos
    """
    config = {
        'path': RUTAS['BASE'],
        'salida': RUTAS['SALIDA'],
        'archivo_ventas': ARCHIVOS_ENTRADA.get('VENTAS'),
        'archivo_ventas_rfm': ARCHIVOS_ENTRADA.get('VENTAS_RFM'),
        'archivo_clientes': ARCHIVOS_ENTRADA.get('CLIENTES'),
        'archivo_costos': ARCHIVOS_ENTRADA.get('COSTOS'),
        'archivo_marca_referencia': ARCHIVOS_ENTRADA.get('MARCA_REFERENCIA'),
        'archivo_metas': ARCHIVOS_ENTRADA.get('METAS'),

   
        'archivo_promotora': ARCHIVOS_ENTRADA.get('CLIENTE_PROMOTORA'),
        'archivo_makro': ARCHIVOS_ENTRADA.get('CLIENTE_MAKRO'),
        'archivo_farmatodo': ARCHIVOS_ENTRADA.get('CLIENTE_FARMATODO'),
        
        'archivo_tabla_sell_out': ARCHIVOS_CONSOLIDADOS.get('CONSOLIDADO_SELL_OUT'),
        'archivo_tabla_sell_out_ciudades': ARCHIVOS_CONSOLIDADOS.get('CONSOLIDADO_SELL_OUT_CIUDADES'),
        'archivo_ventas_consolidado': ARCHIVOS_CONSOLIDADOS.get('VENTAS_CONSOLIDADOS'),
        'archivo_costos_consolidado': ARCHIVOS_CONSOLIDADOS.get('COSTOS_CONSOLIDADOS'),
        'tabla_familia_consolidado': ARCHIVOS_CONSOLIDADOS.get('FAMILIA'),

        'ultima_fecha': FECHAS.get('ULTIMA_FECHA_CONSOLIDADO')
    }
    
    aplicar_configuracion_pandas()
    return config


def validar_formato_nombre_archivo(nombre_archivo, tipo_cliente):
    """
    Valida el formato MES_CLIENTE.xlsx del nombre de archivo
    Parámetros:
        nombre_archivo (str): Nombre del archivo a validar.
        tipo_cliente (str): Nombre esperado del cliente (por ejemplo, "PROMOTORA").

    Returns:
        tuple: (bool, str, str) indicando si el formato es válido, el mes extraído y el mensaje de error (si aplica).     
    """

    if not nombre_archivo:
        return False, None, f"No se especificó archivo para {tipo_cliente}"
    
    nombre_sin_extension = nombre_archivo.replace('.xlsx', '')
    
    if '_' not in nombre_sin_extension:
        return False, None, f"El archivo {nombre_archivo} debe tener formato MES_{tipo_cliente}.xlsx"
    
    partes = nombre_sin_extension.split('_')
    if len(partes) < 2:
        return False, None, f"El archivo {nombre_archivo} debe tener formato MES_{tipo_cliente}.xlsx"
    
    mes_archivo = partes[0].upper()
    cliente_archivo = '_'.join(partes[1:]).upper()
    
    meses_validos = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    if mes_archivo not in meses_validos:
        return False, None, f"Mes '{mes_archivo}' no válido en {nombre_archivo}"
    
    if cliente_archivo != tipo_cliente.upper():
        return False, None, f"Cliente debe ser {tipo_cliente}, encontrado {cliente_archivo} en {nombre_archivo}"
    
    return True, mes_archivo, None

def verificar_mes_correcto(mes_archivo, mes_esperado, nombre_archivo):
    """
    Verifica si el mes extraído del nombre del archivo corresponde con el mes esperado según el consolidado.
    Parámetros:
        mes_archivo (str): Mes detectado en el archivo.
        mes_esperado (str): Mes que se espera procesar.
        nombre_archivo (str): Nombre del archivo original.

    Returns:
        tuple: (bool, str) con estado de validación y mensaje explicativo.
    """
    if mes_archivo == mes_esperado:
        return True, f"✅ {nombre_archivo}: Mes correcto ({mes_esperado})"
    
    meses_orden = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']
    
    try:
        pos_archivo = meses_orden.index(mes_archivo)
        pos_esperado = meses_orden.index(mes_esperado)
        
        if pos_archivo < pos_esperado or (pos_esperado == 0 and pos_archivo > 6):
            return False, f"⚠️ {nombre_archivo}: Mes {mes_archivo} ya procesado (se esperaba {mes_esperado})"
        else:
            return False, f"⚠️ {nombre_archivo}: Mes {mes_archivo} no corresponde (se esperaba {mes_esperado})"
    except ValueError:
        return False, f"❌ Error comparando meses en {nombre_archivo}"

def validar_cliente_individual(config, datos, tipo_cliente, config_key, empresa_nombre):
    """
    Valida si un cliente grande tiene datos que pueden ser procesados en el mes actual.

    Parámetros:
        config (dict): Configuración general.
        datos (dict): Diccionario con datos ya cargados (como el consolidado).
        tipo_cliente (str): Cliente a validar (ej. "MAKRO").
        config_key (str): Clave en config para obtener el archivo del cliente.
        empresa_nombre (str): Nombre como aparece en el consolidado.

    Returns:
        dict: Diccionario con estado de procesamiento, mensajes y metadatos del archivo.
    """
    resultado = {
        'cliente': tipo_cliente,
        'procesable': False,
        'mensaje': '',
        'archivo': None,
        'mes': None
    }
    
    nombre_archivo = config.get(config_key)
    if not nombre_archivo:
        resultado['mensaje'] = f"⚠️ {tipo_cliente}: No especificado en configuración"
        return resultado
    
    resultado['archivo'] = nombre_archivo
    
    ruta_datos = obtener_ruta_completa('entrada')
    ruta_completa = os.path.join(ruta_datos, nombre_archivo)
    
    if not os.path.exists(ruta_completa):
        resultado['mensaje'] = f"⚠️ {tipo_cliente}: Archivo no encontrado ({nombre_archivo})"
        return resultado
    
    formato_ok, mes_archivo, error_formato = validar_formato_nombre_archivo(nombre_archivo, tipo_cliente)
    if not formato_ok:
        resultado['mensaje'] = f" {tipo_cliente}: {error_formato}"
        return resultado
    
    resultado['mes'] = mes_archivo
    
    try:

        from funciones_auxiliares.funciones_clientes_grandes import obtener_ultimo_mes_cliente
        
        info_consolidado = obtener_ultimo_mes_cliente(datos['tabla_sell_out_consolidado'], empresa_nombre)
        if 'error' in info_consolidado:
            resultado['mensaje'] = f" {tipo_cliente}: Error en consolidado - {info_consolidado['error']}"
            return resultado
        
        mes_esperado, _ = info_consolidado['siguiente_mes']
        
        mes_ok, mensaje_mes = verificar_mes_correcto(mes_archivo, mes_esperado, nombre_archivo)
        if mes_ok:
            resultado['procesable'] = True
            resultado['mensaje'] = f"✅ {tipo_cliente}: Listo para procesar (mes: {mes_archivo})"
        else:
            resultado['mensaje'] = f"{tipo_cliente}: {mensaje_mes}"
            
    except Exception as e:
        resultado['mensaje'] = f"{tipo_cliente}: Error al validar - {str(e)}"
    
    return resultado

def validar_clientes_grandes_simple(config, datos):
    """
    Valida todos los clientes grandes usando las funciones anteriores.
    
    Parámetros:
        config (dict): Configuración general.
        datos (dict): Diccionario con datos del consolidado.

    Returns:
        dict: Resultados de validación por cliente (incluye si es procesable o no).
    """
    print("\n🔍 VALIDANDO CLIENTES GRANDES")
    print("=" * 50)
    
    clientes_info = {
        'PROMOTORA': ('archivo_promotora', 'PROMOTORA DE COMERCIO SOCIAL'),
        'MAKRO': ('archivo_makro', 'MAKRO SUPERMAYORISTA SAS'),
        'FARMATODO': ('archivo_farmatodo', 'FARMATODO COLOMBIA SA')
    }
    
    resultados = {}
    
    for tipo_cliente, (config_key, empresa_nombre) in clientes_info.items():
        resultado = validar_cliente_individual(config, datos, tipo_cliente, config_key, empresa_nombre)
        resultados[tipo_cliente] = resultado
        print(f"{resultado['mensaje']}")
    
    print("=" * 50)
    
    procesables = [k for k, v in resultados.items() if v['procesable']]
    if procesables:
        print(f"SE PROCESARÁN: {', '.join(procesables)}")
    else:
        print(f"NO SE PROCESARÁ NINGÚN CLIENTE GRANDE")
    
    return resultados

def cargar_datos_clientes(config):
    """
    Carga las tablas de datos relacionadas con clientes grandes desde archivos Excel. Aplica validaciones y carga condicional.

    Parámetros:
        config (dict): Diccionario de configuración con rutas y nombres de archivo.

    Returns:
        dict: Diccionario con todas las tablas cargadas y resultados de validación.
    """
    
    datos = {}
    ruta_datos = obtener_ruta_completa('entrada')
    ruta_datos_consolidado = obtener_ruta_completa('consolidado')
    
    try:

        logging.info("Cargando tabla de Sell out...")
        datos['tabla_sell_out_consolidado'] = pd.read_excel(
            ruta_datos_consolidado + config['archivo_tabla_sell_out']
        )

        logging.info("Cargando tabla de Sell out ciudades ...")
        datos['tabla_sell_out_ciudades_consolidado'] = pd.read_excel(
            ruta_datos_consolidado + config['archivo_tabla_sell_out_ciudades']
        )

        print("\n📋 Procesando clientes grandes...")

        validaciones = validar_clientes_grandes_simple(config, datos)
        clientes_carga = {
            'PROMOTORA': ('archivo_promotora', 'tabla_cliente_promotora', {}),
            'MAKRO': ('archivo_makro', 'tabla_cliente_makro', {'skiprows': 18, 'header': [0, 1]}),
            'FARMATODO': ('archivo_farmatodo', 'tabla_cliente_farmatodo', {})
        }
        
        for cliente, (config_key, data_key, opciones) in clientes_carga.items():
            if validaciones[cliente]['procesable']:
                try:
                    logging.info(f"Cargando tabla {cliente}...")
                    datos[data_key] = pd.read_excel(
                        ruta_datos + config[config_key],
                        **opciones
                    )
                    print(f"   ✅ {cliente} cargado exitosamente")
                except Exception as e:
                    logging.error(f"Error cargando {cliente}: {e}")
                    datos[data_key] = None
                    print(f"   ❌ {cliente}: Error al cargar - {e}")
            else:
                datos[data_key] = None
                print(f" {cliente}: Omitido (no procesable)")

        datos['_validaciones'] = validaciones
        
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        raise
    except Exception as e:
        logging.error(f"Error al cargar datos: {e}")
        raise
    
    return datos


def cargar_datos(config):
    """
    Carga todas las tablas necesarias para el procesamiento general, incluyendo ventas, costos, cumplimiento y sell-out.

    Parámetros:
        config (dict): Diccionario de configuración.

    Returns:
        dict: Diccionario con todas las tablas leídas de los archivos Excel.
    """

    datos = {}
    ruta_datos = obtener_ruta_completa('entrada')
    ruta_datos_consolidado = obtener_ruta_completa('consolidado')
    
    try:
 
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

        logging.info("Cargando tabla familia ...")
        datos['tabla_familia_consolidado'] = pd.read_excel(
            ruta_datos_consolidado + config['tabla_familia_consolidado']
        )

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
        
        logging.info("Todas las tablas cargadas exitosamente")
        
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        raise
    except Exception as e:
        logging.error(f"Error al cargar datos: {e}")
        raise
    
    return datos

def cargar_datos_rfm(config):
    """
    Carga las tablas específicas para el análisis RFM (Recencia, Frecuencia, Valor Monetario).

    Parámetros:
        config (dict): Diccionario de configuración.

    Returns:
        dict: Diccionario con tablas específicas para análisis RFM.
    """
    
    datos = {}
    ruta_datos = obtener_ruta_completa('entrada')
    
    try:

        if config.get('archivo_ventas_rfm'):
            logging.info("Cargando tabla de ventas rfm...")
            datos['tabla_ventas_rfm'] = pd.read_excel(
                ruta_datos + config['archivo_ventas_rfm'], 
                dtype=str
            )
        
        if config.get('archivo_clientes'):
            logging.info("Cargando tabla de clientes...")
            datos['tabla_clientes'] = pd.read_excel(
                ruta_datos + config['archivo_clientes']
            )
        
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
        serie_limpia = serie.copy()
        mask_no_nulos = serie_limpia.notna()
        
        if mask_no_nulos.any():
            valores_str = serie_limpia[mask_no_nulos].astype(str)
            valores_str = valores_str.str.replace(' ', '')
            valores_str = valores_str.str.replace(',', '')
            valores_str = valores_str.str.replace('.', '', regex=False)
            
            def corregir_ceros_extra(valor_str):
                if pd.isna(valor_str) or valor_str == 'nan':
                    return valor_str
            
                if len(valor_str) > 4 and valor_str.endswith('00000'):
                    ceros_finales = 0
                    for i in range(len(valor_str)-1, -1, -1):
                        if valor_str[i] == '0':
                            ceros_finales += 1
                        else:
                            break
                
                    if ceros_finales >= 5:
                        valor_sin_ceros = valor_str.rstrip('0')
                        if len(valor_sin_ceros) > 0:
                            return valor_sin_ceros
                
                return valor_str

            valores_str = valores_str.apply(corregir_ceros_extra)
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

    df_limpio = df.copy()
    df_limpio = df_limpio.dropna(how='all')
    df_limpio = df_limpio.dropna(axis=1, how='all')

    if df_limpio.columns.dtype == 'object':
        df_limpio.columns = df_limpio.columns.str.strip()
        df_limpio.columns = df_limpio.columns.str.replace(r'\s+', ' ', regex=True)
        df_limpio.columns = df_limpio.columns.str.replace(r'[^\w\s\.\-_]', '', regex=True)
    
    for columna in df_limpio.columns:
        if df_limpio[columna].dtype == 'object':
            df_limpio[columna] = df_limpio[columna].astype(str).str.strip()
            df_limpio[columna] = df_limpio[columna].replace(['nan', 'None', '', 'NaN'], np.nan)
            df_limpio[columna] = df_limpio[columna].str.replace(r'\s+', ' ', regex=True)
            df_limpio[columna] = limpiar_formato_numerico_excel(df_limpio[columna])

            try:
                df_limpio[columna] = pd.to_numeric(df_limpio[columna])
            except (ValueError, TypeError):
                pass

    df_limpio = df_limpio.reset_index(drop=True)

    return df_limpio
