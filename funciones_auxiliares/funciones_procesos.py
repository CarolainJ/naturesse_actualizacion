import pandas as pd
import numpy as np
import funciones_auxiliares.funciones as funciones
import logging
from funciones_auxiliares.funciones_principales import (limpiar_dataframe_excel)
import unicodedata

def limpiar_referencia(valor):
    """
    Limpia una referencia de producto eliminando tildes, espacios, guiones y convierte a minúsculas.
    Parámetros:
        valor (str): Valor original de la referencia.

    Returns:
        str: Valor limpio y normalizado de la referencia.
    """
    if pd.isna(valor):
        return ''
    valor_str = str(valor).strip().lower().replace(" ", "").replace("-", "")
    valor_str = unicodedata.normalize('NFKD', valor_str).encode('ASCII', 'ignore').decode('utf-8')
    return valor_str

## PROCESAMIENTO DE VENTAS
def procesar_ventas(tabla_ventas, ultima_fecha, path):
    """
    Procesa y transforma la tabla de ventas: filtra fechas, normaliza campos, asigna marcas y calcula cantidades internas.
    
    Parámetros:
        tabla_ventas (pd.DataFrame): DataFrame de ventas sin procesar.
        ultima_fecha (pd.Timestamp): Fecha límite para considerar ventas nuevas.
        path (str): Ruta base para guardar archivos (no se usa explícitamente aquí, pero puede ser útil en versiones extendidas).

    Returns:
        pd.DataFrame: DataFrame procesado y enriquecido.
    """
    logging.info("Iniciando procesamiento de ventas...")

    tabla_ventas["Fecha"] = tabla_ventas["Fecha"].apply(lambda x: x.strip() if isinstance(x, str) else x)
    tabla_ventas["Fecha"] = pd.to_datetime(tabla_ventas["Fecha"], errors="coerce")

    registros_antes = len(tabla_ventas)
    tabla_ventas = tabla_ventas[tabla_ventas["Fecha"] > ultima_fecha]
    registros_despues = len(tabla_ventas)
    logging.info(f"Filtrado por fecha: {registros_antes} -> {registros_despues} registros")
    
    tabla_ventas.rename(columns={"Desc. item": "Desc.item"}, inplace=True)
    tabla_ventas["Tipo de identificacion"] = np.nan

    cols = list(tabla_ventas.columns)
    cols.insert(3, cols.pop(cols.index("Tipo de identificacion")))
    tabla_ventas = tabla_ventas[cols]
    
    tabla_ventas = tabla_ventas.iloc[:, :12]
    tabla_ventas["Marca"] = tabla_ventas.apply(
        lambda row: funciones.asignar_marca(row["Desc.item"], row.get("Marca", np.nan)), 
        axis=1
    )
    
    tabla_ventas["TieneX"] = tabla_ventas["Desc.item"].apply(funciones.tiene_x)
    tabla_ventas["Cantidades"] = tabla_ventas["Desc.item"].apply(funciones.extraer_cantidad)
    tabla_ventas["Producto"] = tabla_ventas["Desc.item"].apply(funciones.extraer_producto)
    
    tabla_ventas.drop(columns=["TieneX"], inplace=True)
    tabla_ventas["Producto_normalizado"] = tabla_ventas["Producto"].apply(funciones.normalizar_producto)
    tabla_ventas.drop(columns=["Producto"], inplace=True)
    tabla_ventas.rename(columns={"Producto_normalizado": "Producto"}, inplace=True)
    
    tabla_ventas["Cantidades"] = tabla_ventas["Cantidades"].apply(funciones.normalizar_cantidad)
    tabla_ventas["Cantidades"] = tabla_ventas.apply(
        lambda row: "1UND" if row["Cantidades"] == "NA" and not pd.isna(row["Desc.item"]) else row["Cantidades"], 
        axis=1
    )
    
    tabla_ventas["Cantidad inv.sell_Int"] = tabla_ventas.apply(funciones.calcular_cantidad_sell_int, axis=1)
    
    return tabla_ventas
    
def filtrar_registros_nuevos_ventas(df_entrada, df_consolidado):
    """
    Compara la tabla de ventas nueva con el consolidado para identificar los registros nuevos.

    Parámetros:
        df_entrada (pd.DataFrame): Datos nuevos de ventas.
        df_consolidado (pd.DataFrame): Consolidado histórico.

    Returns:
        pd.DataFrame o None: Registros nuevos detectados; si no se detectan, retorna None.
    """
    campos_clave = [
        'Marca', 
        'Linea de mercado',
        'Razon social',
        'Fecha', 
        'Cantidad inv.',
        'Valor bruto local'
    ]

    df_entrada['Fecha'] = pd.to_datetime(df_entrada['Fecha'])
    df_consolidado['Fecha'] = pd.to_datetime(df_consolidado['Fecha'])
    
    ultimo_registro_normalizado = df_consolidado[campos_clave].astype(str)
    ultimo_registro_normalizado = limpiar_dataframe_excel(ultimo_registro_normalizado).iloc[-1]

    fecha_max_consolidado = df_consolidado['Fecha'].max()
    df_filtrado_fecha = df_entrada[df_entrada['Fecha'] >= fecha_max_consolidado].copy()

    df_filtrado_fecha = df_filtrado_fecha.reset_index(drop=True)

    entrada_normalizada = df_filtrado_fecha[campos_clave].astype(str)
    entrada_normalizada = limpiar_dataframe_excel(entrada_normalizada).reset_index(drop=True)

    coincidencias = entrada_normalizada.eq(ultimo_registro_normalizado).all(axis=1)

    if coincidencias.any():
        idx = coincidencias[coincidencias].index[0]
        df_nuevos = df_filtrado_fecha.loc[idx+1:].copy()
        df_nuevos = df_nuevos.rename(columns={"EAN 13": "EAN"})
        print(f"Se encontró el último registro del consolidado en la entrada. Nuevos desde el índice {idx + 1}.")
    else:
        df_nuevos = None
        print("El último registro del consolidado NO se encontró en la entrada. No se establecen registros nuevos. ")

    return df_nuevos

## PROCESAMIENTO DE CLIENTES
def procesar_clientes(tabla_clientes):
    """
    Limpia la tabla de clientes, eliminando duplicados basados en el campo nit.

    Parámetros:
        tabla_clientes (pd.DataFrame): DataFrame de clientes sin procesar.

    Returns:
        pd.DataFrame: DataFrame de clientes procesado y sin duplicados.    
    """

    tabla_clientes['nit'] = tabla_clientes['nit'].astype(str).str.strip()
    tabla_clientes = tabla_clientes.drop_duplicates(subset=["nit"])
    
    return tabla_clientes

## PROCESAMIENTO LINEA MERCADO
def crear_conteo_linea_mercado(tabla_ventas, año_debug=None):
    """
    Agrupa las ventas por semana, mes, año y otras categorías para contar compras por línea de mercado.

    Parámetros:
        tabla_ventas (pd.DataFrame): Tabla de ventas ya procesada.
        año_debug (int, opcional): Año para propósitos de depuración (no se usa actualmente).

    Returns:
        pd.DataFrame: Tabla agrupada con el conteo de compras (numCompras) por semana.
    """

    tabla_ventas = tabla_ventas.copy()
    tabla_ventas["Fecha"] = pd.to_datetime(tabla_ventas["Fecha"], errors='coerce')
    
    def ceiling_date_week_start_monday(fecha):
        fecha = pd.to_datetime(fecha)
        if fecha.weekday() == 0:
            return fecha.normalize()
        else:
            return (fecha - pd.to_timedelta(fecha.weekday(), unit='d')).normalize()
        

    tabla_ventas["Semana"] = tabla_ventas["Fecha"].apply(ceiling_date_week_start_monday)
    tabla_ventas = tabla_ventas.dropna(subset=["Semana"])
  
    meses_espanol = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
        5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }
    
    tabla_ventas["Mes"] = tabla_ventas["Semana"].dt.month.map(meses_espanol)
    tabla_ventas["Año"] = tabla_ventas["Semana"].dt.year.astype(str)
    tabla_ventas["Mes_num"] = tabla_ventas["Semana"].dt.month.astype(str).str.zfill(2)


    columnas_agrupacion = ["Año", "Mes", "Semana", "Mes_num"]
    if "Linea de mercado" in tabla_ventas.columns:
        columnas_agrupacion.append("Linea de mercado")
    if "Razon social" in tabla_ventas.columns:
        columnas_agrupacion.append("Razon social")
    
    resultado = tabla_ventas.groupby(columnas_agrupacion).size().reset_index(name="numCompras")
       
    return resultado

## PROCESAMIENTO DE COSTOS
def consolidar_costos_historicos(datos, tabla_familia_consolidado, año_actual):
    """
    Consolida la información de costos unificando el histórico y la nueva entrada si aplica.

    Parámetros:
        datos (dict): Diccionario que contiene tablas como 'tabla_consolidado_costos' y opcionalmente 'tabla_de_costos'.
        tabla_familia_consolidado (pd.DataFrame): Tabla con referencias cruzadas para unir datos.
        año_actual (int): Año de referencia para verificar si se deben añadir nuevos registros.

    Returns:
        pd.DataFrame: Tabla consolidada de costos, uniendo histórico y potenciales registros del mes siguiente.
    """

    tabla_costos_consolidado = datos['tabla_consolidado_costos']

    if datos.get('tabla_de_costos') is None:
        return tabla_costos_consolidado
    
    tabla_costo_entrada = datos['tabla_de_costos']
    tabla_costo_entrada = tabla_costo_entrada.rename(
        columns={"Costo prom. uni.": "Costo prom.", "Desc. item": "Desc.item"}
    )[["Marca", "Referencia", "Desc.item", "Instalación", "U.M.", "Costo prom.", "Mes", "Año"]]


    if "Incluir referencia(Si/No)" not in tabla_costo_entrada.columns:
        tabla_costo_entrada["Incluir referencia(Si/No)"] = None

    tabla_costos_consolidado["Referencia"] = tabla_costos_consolidado["Referencia"].astype(str)
    tabla_familia_consolidado["Referencia"] = tabla_familia_consolidado["Referencia"].astype(str)
    consolidado_costo = tabla_costos_consolidado.merge(tabla_familia_consolidado, on="Referencia", how="left")


    if "Incluir referencia(Si/No).y" in consolidado_costo.columns:
        consolidado_costo = consolidado_costo.rename(columns={"Incluir referencia(Si/No).y": "Incluir referencia(Si/No)"})
    elif "Incluir referencia(Si/No)" not in consolidado_costo.columns:
        consolidado_costo["Incluir referencia(Si/No)"] = None


    consolidado_costo_filtrado = consolidado_costo[
        ["Referencia", "Desc.item", "Instalación", "U.M.", "Costo prom.", "Mes", "Año", "Marca", "Incluir referencia(Si/No)"]
    ]

    consolidado_año_actual = consolidado_costo_filtrado[consolidado_costo_filtrado["Año"] == año_actual]
    if len(consolidado_año_actual) > 0:

        meses_orden = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        
    
        consolidado_año_actual['mes_num'] = consolidado_año_actual['Mes'].str.lower().map(meses_orden)
        ultimo_mes_num = consolidado_año_actual['mes_num'].max()
        
        proximo_mes_num = ultimo_mes_num + 1 if ultimo_mes_num < 12 else 1
        proximo_año = año_actual if ultimo_mes_num < 12 else año_actual + 1
        
        meses_nombres = {v: k for k, v in meses_orden.items()}
        proximo_mes_nombre = meses_nombres[proximo_mes_num]
        
        tabla_costo_entrada_filtrada = tabla_costo_entrada[
            (tabla_costo_entrada["Mes"].str.lower().str.strip() == proximo_mes_nombre) & 
            (tabla_costo_entrada["Año"] == proximo_año)
        ]
        
        if len(tabla_costo_entrada_filtrada) == 0:
            print(f"No se encontraron registros para {proximo_mes_nombre} {proximo_año} en la tabla de entrada")
            return consolidado_costo_filtrado
        else:
            print(f"Se encontraron {len(tabla_costo_entrada_filtrada)} registros para {proximo_mes_nombre} {proximo_año}")
    
    else:
        print("No hay registros del año actual en el consolidado. Procesando todos los del año actual de entrada.")
        tabla_costo_entrada_filtrada = tabla_costo_entrada[tabla_costo_entrada["Año"] == año_actual]


    consolidado_historico = consolidado_costo_filtrado[consolidado_costo_filtrado["Año"] < año_actual]
    consolidado_año_actual_existente = consolidado_costo_filtrado[consolidado_costo_filtrado["Año"] == año_actual]

    consolidado_final = pd.concat([
        consolidado_historico, 
        consolidado_año_actual_existente, 
        tabla_costo_entrada_filtrada
    ], ignore_index=True)
    
    return consolidado_final