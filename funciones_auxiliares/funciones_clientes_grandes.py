"""
Módulo para procesamiento de datos de ventas por cliente (Sell-Out).

Este módulo proporciona funciones especializadas para procesar datos de ventas
de diferentes clientes mayoristas, incluyendo PROMOTORA DE COMERCIO SOCIAL,
MAKRO SUPERMAYORISTA SAS y FARMATODO COLOMBIA SA. Cada cliente tiene su propio
formato de datos y reglas de procesamiento específicas.

Funcionalidades principales:
- Mapeo y conversión de fechas y meses
- Procesamiento específico por cliente
- Agregación de ventas por tienda y producto
- Separación automática de tienda y ciudad
- Generación de consolidados para reportes
"""

import pandas as pd
from datetime import datetime

meses_mapping = {
    "ENE": "1", "FEB": "2", "MAR": "3", "ABR": "4",
    "MAY": "5", "JUN": "6", "JUL": "7", "AGO": "8",
    "SEP": "9", "OCT": "10", "NOV": "11", "DIC": "12"
}

meses_makro = {
    "ENE": "Enero", "FEB": "Febrero", "MAR": "Marzo", "ABR": "Abril",
    "MAY": "Mayo", "JUN": "Junio", "JUL": "Julio", "AGO": "Agosto",
    "SEP": "Septiembre", "OCT": "Octubre", "NOV": "Noviembre", "DIC": "Diciembre"
}

def obtener_numero_mes(nombre_mes):
    """
    Convierte el nombre abreviado de un mes a su número correspondiente.
    """
    return int(meses_mapping[nombre_mes])
    
def obtener_siguiente_mes(mes_actual, año_actual):
    """
    Calcula el siguiente mes y año basado en el mes y año actuales.
    Maneja automáticamente el cambio de año cuando el mes actual es diciembre.
    """

    numero_a_mes = {
        1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR",
        5: "MAY", 6: "JUN", 7: "JUL", 8: "AGO",
        9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"
    }
    
    num_mes_actual = int(meses_mapping[mes_actual])
    
    if num_mes_actual == 12:
        siguiente_mes_num = 1
        siguiente_año = año_actual + 1
    else:
        siguiente_mes_num = num_mes_actual + 1
        siguiente_año = año_actual
    
    siguiente_mes = numero_a_mes[siguiente_mes_num]
    
    return siguiente_mes, siguiente_año

def obtener_ultimo_mes_cliente(consolidado_sell_out, nombre_cliente):
    """
    Obtiene información del último mes registrado para un cliente específico.
    
    Busca en el consolidado de ventas el registro más reciente de un cliente
    y calcula automáticamente cuál sería el siguiente mes a procesar.
    """ 

    try:
        
        df_cliente = consolidado_sell_out[consolidado_sell_out['cliente'] == nombre_cliente].copy()
        
        if df_cliente.empty:
            return {"error": f"No se encontraron registros para el cliente: {nombre_cliente}"}
        
        df_cliente['fecha_ordenamiento'] = pd.to_datetime(
            df_cliente['año'].astype(str) + '-' + 
            df_cliente['Mes'].map(meses_mapping) + '-01'
        )
        
        df_cliente_ordenado = df_cliente.sort_values('fecha_ordenamiento')
        ultimo_registro = df_cliente_ordenado.iloc[-1]
        
        return {
            "cliente": ultimo_registro['cliente'],
            "ultimo_mes": ultimo_registro['Mes'],
            "ultimo_año": ultimo_registro['año'],
            "fecha_completa": f"{ultimo_registro['Mes']} {ultimo_registro['año']}",
            "unidades": ultimo_registro['Unidades'],
            "total_registros": len(df_cliente),
            "siguiente_mes": obtener_siguiente_mes( ultimo_registro['Mes'], ultimo_registro['año'])
        }
        
    except FileNotFoundError:
        return {"error": "Archivo no encontrado"}
    except Exception as e:
        return {"error": f"Error al procesar el archivo: {str(e)}"}

def separar_tienda_ciudad(nombre):
    """
    Separa el nombre de una tienda de su ciudad basándose en patrones específicos.
    
    Aplica reglas específicas para nombres de tiendas conocidos y usa patrones
    generales para casos no específicos.
    """

    nombre = nombre.strip()
    
    if nombre == "CARULLA PRADERA DE POTOSI":
        return "CARULLA PRADERA DE POTOSI", "POTOSI"
    elif nombre == "CARULLA MANIZALES":
        return "CARULLA MANIZALES", "MANIZALES"
    elif nombre == "CARULLA PLAZA CLARO":
        return "CARULLA PLAZA CLARO", "BOGOTA"
    
    if '-' in nombre:
        partes = [x.strip() for x in nombre.split('-')]
        tienda = partes[0]
        ciudad = partes[1]
        return tienda, ciudad
    
    return nombre, "CIUDAD DESCONOCIDA"
    
def obtener_mes_completo(nombre_mes):
    """
    Convierte el nombre abreviado de un mes a su nombre completo para MAKRO.
    """
    return meses_makro[nombre_mes]

def separar_tienda_ciudad_makro(valor):
    """
    Separa tienda y ciudad específicamente para datos de MAKRO.
    
    Aplica reglas específicas para el formato de nombres de tiendas de MAKRO,
    que pueden incluir códigos y formatos particulares.
    """

    if pd.isna(valor):
        return pd.Series(["", ""])
    
    partes = valor.split("-", 1)
    nombre = partes[1].strip() if len(partes) == 2 else valor.strip()
    nombre = nombre.upper()

    if nombre == "MAKRO CALLE 30":
        return pd.Series(["MAKRO CALLE 30", "SOLEDAD"])
    elif nombre == "ESTACION POBLADO":
        return pd.Series(["ESTACION POBLADO", "MEDELLIN"])
    elif nombre == "CALI - NORTE":
        return pd.Series(["NORTE", "CALI"])
    
    if " - " in nombre:
        partes = nombre.rsplit(" - ", 1)
        if len(partes) == 2:
            tienda, ciudad = partes
            return pd.Series([tienda.strip(), ciudad.strip()])

    return pd.Series(["Tienda Única", nombre.strip()])

## PROCESAMIENTO CLIENTE - PROMOTORA
def procesar_promotora(datos):

    """
    Procesa los datos de ventas específicos de PROMOTORA DE COMERCIO SOCIAL.
    
    Transforma los datos crudos de PROMOTORA en un formato estandarizado para
    análisis de sell-out, incluyendo separación de tienda/ciudad y agregación
    por producto y ubicación.
    
    Args:
        datos (Dict[str, pd.DataFrame]): Diccionario con DataFrames necesarios:
            - 'tabla_cliente_promotora': Datos de ventas de PROMOTORA
            - 'tabla_sell_out_consolidado': Historial consolidado de ventas
            
    Returns:
        Tuple[Optional[Dict], Optional[pd.DataFrame]]: Tupla con:
            - consolidado_sell_out: Resumen consolidado para el período
            - ventas_promotora: DataFrame detallado de ventas procesadas
            
        Retorna (None, None) si no hay datos de PROMOTORA disponibles
        
    """

    if datos.get('tabla_cliente_promotora') is None:
        return None, None
    
    informacion_consolidado= obtener_ultimo_mes_cliente(datos['tabla_sell_out_consolidado'], "PROMOTORA DE COMERCIO SOCIAL")
    archivo_entrada_promotora = datos['tabla_cliente_promotora']
    
    if 'Unidades' not in archivo_entrada_promotora.columns:
        return {"error": "No se encontró la columna 'Unidades' en el archivo de promotora"}

    archivo_entrada_promotora["Fecha"] = pd.to_datetime(archivo_entrada_promotora["Fecha"], errors='coerce')
    archivo_entrada_promotora["Mes"] = archivo_entrada_promotora["Fecha"].dt.strftime("%B").str.lower()
    archivo_entrada_promotora["Año"] = archivo_entrada_promotora["Fecha"].dt.year

    archivo_entrada_promotora = archivo_entrada_promotora[
        archivo_entrada_promotora['Nomb. Dependencia'].notna() & 
        (archivo_entrada_promotora['Nomb. Dependencia'].str.strip() != '')
    ]
    
        
    archivo_entrada_promotora[['Tienda', 'Ciudad']] = archivo_entrada_promotora['Nomb. Dependencia'].apply(
    lambda x: pd.Series(separar_tienda_ciudad(x))
    )

    siguiente_mes, siguiente_año = informacion_consolidado['siguiente_mes']
    num_mes = obtener_numero_mes(siguiente_mes)

    ventas_promotora = archivo_entrada_promotora.groupby([
       "EAN", "Mes", "Año", "Tienda", "Ciudad", "Descripción" ]).agg({"Unidades": "sum"}).reset_index()

    ventas_promotora = ventas_promotora.rename(columns={"Año": "año", "Descripción":"Descripcion"})

    pos_ean = ventas_promotora.columns.get_loc("EAN")
    ventas_promotora.insert(pos_ean + 1, "cliente", "PROMOTORA DE COMERCIO SOCIAL")
    

    pos = ventas_promotora.columns.get_loc("Unidades")
    ventas_promotora.insert(pos + 1, "NumMes", num_mes)

    total_unidades = int(ventas_promotora["Unidades"].sum())

    consolidado_sell_out = {
        "Mes": siguiente_mes,
        "año": siguiente_año,
        "cliente": "PROMOTORA DE COMERCIO SOCIAL",
        "Unidades": total_unidades,
        "NumMes": num_mes
    }
    
    return consolidado_sell_out, ventas_promotora


## PROCESAMIENTO CLIENTE - MAKRO 
def procesar_makro(datos):
    """
    Procesa los datos de ventas específicos de MAKRO SUPERMAYORISTA SAS.
    
    Transforma los datos de MAKRO que vienen en formato de columnas múltiples
    (MultiIndex) en un formato estandarizado para análisis de sell-out.
    
    Args:
        datos (Dict[str, pd.DataFrame]): Diccionario con DataFrames necesarios:
            - 'tabla_cliente_makro': Datos de ventas de MAKRO (formato MultiIndex)
            - 'tabla_sell_out_consolidado': Historial consolidado de ventas
            
    Returns:
        Tuple[Optional[Dict], Optional[pd.DataFrame]]: Tupla con:
            - consolidado_sell_out: Resumen consolidado para el período
            - Venta_Unidades_MAKRO: DataFrame detallado de ventas procesadas
            
        Retorna (None, None) si no hay datos de MAKRO disponibles
    """

    if datos.get('tabla_cliente_makro') is None:
        return None, None

    archivo_entrada_makro = datos['tabla_cliente_makro']
    informacion_consolidado= obtener_ultimo_mes_cliente(datos['tabla_sell_out_consolidado'], "MAKRO SUPERMAYORISTA SAS")
    siguiente_mes, siguiente_año = informacion_consolidado['siguiente_mes']

    mes_procesar = obtener_mes_completo(siguiente_mes)
    
    id_vars = [
        ('Tienda', 'Unnamed: 6_level_1'),
        ('Número de artículo Makro', 'Unnamed: 2_level_1'),
        ('Descripción del artículo', 'Unnamed: 1_level_1'),
        ('Codigo de barras', 'Unnamed: 5_level_1')
    ]

    df_completo = archivo_entrada_makro.copy()
    for col in id_vars:
        if col in df_completo.columns:
            df_completo = df_completo[
                df_completo[col].notna() & 
                (df_completo[col] != '') & 
                (df_completo[col] != ' ')
            ]

    columna_mes_cde = [col for col in df_completo.columns if col == (mes_procesar, 'Cde.')]
        
    MAKRO_consolidado_long = df_completo.melt(
        id_vars=id_vars, 
        value_vars=columna_mes_cde,
        var_name="Mes", 
        value_name="Unidades"
    )

    MAKRO_consolidado_long = MAKRO_consolidado_long.dropna(subset=['Unidades'])
    MAKRO_consolidado_long = MAKRO_consolidado_long[MAKRO_consolidado_long['Unidades'] != 0]
    
    MAKRO_consolidado_long["Mes"] = (MAKRO_consolidado_long["Mes"]
                                   .str.replace("Cde.", "")
                                   .str.lower())

    MAKRO_consolidado_long["Año"] = siguiente_año
    MAKRO_consolidado_long["cliente"] = "MAKRO SUPERMAYORISTA SAS"
    

    col_tienda_raw = ('Tienda', 'Unnamed: 6_level_1')

    MAKRO_consolidado_long[['Tienda', 'Ciudad']] = MAKRO_consolidado_long[col_tienda_raw].apply(
        separar_tienda_ciudad_makro
    )

    Venta_Unidades_MAKRO = MAKRO_consolidado_long.groupby([
        ('Codigo de barras', 'Unnamed: 5_level_1'), "Mes", "Año", "cliente", "Tienda", "Ciudad", ('Descripción del artículo', 'Unnamed: 1_level_1')
    ]).agg({"Unidades": "sum"}).reset_index()

    total_unidades = int(MAKRO_consolidado_long["Unidades"].sum())
    num_mes = obtener_numero_mes(siguiente_mes)

    Venta_Unidades_MAKRO = Venta_Unidades_MAKRO.rename(columns={"Año": "año", ('Codigo de barras', 'Unnamed: 5_level_1'):"EAN", ('Descripción del artículo', 'Unnamed: 1_level_1'):"Descripcion"})

    pos = Venta_Unidades_MAKRO.columns.get_loc("Unidades")
    Venta_Unidades_MAKRO.insert(pos + 1, "NumMes", num_mes)

    consolidado_sell_out = {
        "Mes": siguiente_mes,
        "año": siguiente_año,
        "cliente": "MAKRO SUPERMAYORISTA SAS",
        "Unidades": int(MAKRO_consolidado_long["Unidades"].sum()),
        "NumMes": num_mes
    }
    
    return consolidado_sell_out, Venta_Unidades_MAKRO


## PROCESAMIENTO CLIENTE - FARMATODO 
def procesar_farmatodo(datos):
    """
    Procesa los datos de ventas específicos de FARMATODO COLOMBIA SA.
    
    Transforma los datos de FARMATODO que vienen con nombres de columnas específicos
    en un formato estandarizado para análisis de sell-out.
    
    Args:
        datos (Dict[str, pd.DataFrame]): Diccionario con DataFrames necesarios:
            - 'tabla_cliente_farmatodo': Datos de ventas de FARMATODO
            - 'tabla_sell_out_consolidado': Historial consolidado de ventas
            
    Returns:
        Tuple[Optional[Dict], Optional[pd.DataFrame]]: Tupla con:
            - consolidado_sell_out: Resumen consolidado para el período
            - farmatodo_tabla: DataFrame detallado de ventas procesadas
            
        Retorna (None, None) si no hay datos de FARMATODO disponibles
    """
    
    if datos.get('tabla_cliente_farmatodo') is None:
        return None, None

    archivo_farmatodo = datos['tabla_cliente_farmatodo']
    informacion_consolidado= obtener_ultimo_mes_cliente(datos['tabla_sell_out_consolidado'], "FARMATODO COLOMBIA SA")
    siguiente_mes, siguiente_año = informacion_consolidado['siguiente_mes']
    num_mes = obtener_numero_mes(siguiente_mes)

    farmatodo_tabla = archivo_farmatodo.rename(columns={
    "UNIDADES_VENDIDAS": "Unidades", 
    "CIUDAD": "Ciudad", 
    "NOMBRE_TIENDA": "Tienda",
    "DESCRIPCION_ITEM": "Descripcion", 
    "UPC": "EAN", 
    "SUP_NAME": "SUP_NAME"
    })

    farmatodo_tabla["Mes"] = siguiente_mes
    farmatodo_tabla["año"] = siguiente_año
    farmatodo_tabla["cliente"] = "FARMATODO COLOMBIA SA"

    farmatodo_tabla = farmatodo_tabla.groupby([
        "EAN", "Mes", "año", "cliente", "Tienda", "Ciudad", "Descripcion"
    ]).agg({"Unidades": "sum"}).reset_index()

    pos = farmatodo_tabla.columns.get_loc("Unidades")
    farmatodo_tabla.insert(pos + 1, "NumMes", num_mes)

    consolidado_sell_out = {
        "Mes": siguiente_mes,
        "año": siguiente_año,
        "cliente": "FARMATODO COLOMBIA SA",
        "Unidades": int(archivo_farmatodo["UNIDADES_VENDIDAS"].sum()),
        "NumMes": num_mes
    }
    
    return consolidado_sell_out, farmatodo_tabla
