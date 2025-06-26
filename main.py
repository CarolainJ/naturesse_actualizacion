import pandas as pd
import os

from funciones_auxiliares.funciones_principales import (configurar_rutas, cargar_datos)
from funciones_auxiliares.funciones_procesos import (procesar_ventas, filtrar_registros_nuevos_ventas, procesar_clientes, crear_conteo_linea_mercado, consolidar_costos_historicos)

def ejecutar_actualizacion_ventas():

    if datos.get('tabla_ventas') is None:
        print(" No se encontró la tabla de ventas actual. Se omite el procesamiento.")
        return #
    
    ruta_salida = os.path.join(config['salida'], 'Tabla ventas.xlsx')
    df_nuevos = filtrar_registros_nuevos_ventas(
        datos['tabla_ventas'], 
        datos['tabla_de_ventas_consolidado']
    )

    if df_nuevos is None:
        print(" No hay nuevos registros de ventas para procesar, no fue encontrado el último registro del consolidado.")
        datos['tabla_de_ventas_consolidado'].to_excel(ruta_salida, index=False)
        return

    if df_nuevos.empty:
        print("No hay nuevos registros de ventas para procesar.")
        return

    print("\n🔄 Procesando tabla de ventas...")
    tabla_ventas_procesada = procesar_ventas(
        df_nuevos, 
        config['ultima_fecha'], 
        config['path']
    )
    
    print("\n Concatenando con consolidado histórico de ventas...")
    df_consolidado = datos['tabla_de_ventas_consolidado']
    df_consolidado_ventas_actualizado = pd.concat([df_consolidado, tabla_ventas_procesada], ignore_index=True)
    df_consolidado_ventas_actualizado.to_excel(ruta_salida, index=False)

    print(f" Archivo ventas actualizado guardado en: {ruta_salida}")

    return df_consolidado_ventas_actualizado

def ejecutar_procesamiento_clientes():
   
    print("\n🔄 Procesando tabla de clientes...")

    if datos.get('tabla_clientes') is None:
        print(" No se encontró la tabla de clientes. Se omite el procesamiento.")
        return
    
    tabla_clientes_procesada = procesar_clientes(
        datos['tabla_clientes']
    )

    ruta_salida = os.path.join(config['salida'], 'Tabla de clientes.xlsx')
    tabla_clientes_procesada.to_excel(ruta_salida, index=False)

    print(f" Archivo clientes actualizado guardado en: {ruta_salida}")
    return tabla_clientes_procesada

def ejecutar_creacion_tabla_conteo_mercado(df_consolidado_ventas_actualizado):
    print("\n🔄 Creando tabla de conteo mercado...") 
    
    tabla_conteo_mercado = crear_conteo_linea_mercado(
        df_consolidado_ventas_actualizado
    )

    ruta_salida = os.path.join(config['salida'], 'Conteo linea de mercado.xlsx')
    tabla_conteo_mercado.to_excel(ruta_salida, index=False)

    print(f" Archivo tabla conteo de mercado guardado en: {ruta_salida}")

def ejecutar_actualizar_costos(datos, tabla_marca_referencia):
    print("\n 🔄 Actualizando tabla de consolidado historico de costos...")
    consolidado_costos_historico = consolidar_costos_historicos(
        datos, 
        tabla_marca_referencia, 
        2025
    )

    ruta_salida = os.path.join(config['salida'], 'Consolidado Costos.xlsx')
    consolidado_costos_historico.to_excel(ruta_salida, index=False)
    print(f" Archivo tabla de costos actualizada guardada en: {ruta_salida}")


if __name__ == "__main__":

    import warnings
    warnings.filterwarnings("ignore")

    # 1. CONFIGURACIÓN INICIAL
    print("📁 Configurando rutas y parámetros...")
    config = configurar_rutas()

    # 2. CARGA DE DATOS
    print("\n📊 Cargando datos desde archivos Excel...")
    datos = cargar_datos(config)
    df_consolidado_ventas_actualizado = None

    # 3. PROCESAMIENTO DE VENTAS
    df_consolidado_ventas_actualizado = ejecutar_actualizacion_ventas()

    # 4. PROCESAMIENTO DE CLIENTES
    df_clientes = ejecutar_procesamiento_clientes()

    # 5. TABLA DE CONTEO MERCADO
    if df_consolidado_ventas_actualizado is not None:
        ejecutar_creacion_tabla_conteo_mercado(df_consolidado_ventas_actualizado)
    else: 
        ejecutar_creacion_tabla_conteo_mercado(datos['tabla_de_ventas_consolidado'])
    
    # 6. PROCESAMIENTO COSTOS
    ejecutar_actualizar_costos(datos, datos['tabla_familia_consolidado'])