import pandas as pd
import os

from funciones_principales import (configurar_rutas, cargar_datos, limpiar_formato_numerico_excel, limpiar_dataframe_excel)
from funciones_procesos import (procesar_ventas, filtrar_registros_nuevos_ventas, procesar_clientes, crear_conteo_linea_mercado, crear_tabla_marca_referencia, consolidar_costos_historicos)
from funciones_clientes_grandes import (procesar_promotora, procesar_makro ,procesar_farmatodo)

procesar_promotora
def ejecutar_actualizacion_ventas():

    if datos.get('tabla_ventas') is None:
        print("⚠️ No se encontró la tabla de ventas actual. Se omite el procesamiento.")
        return #
   
    df_nuevos = filtrar_registros_nuevos_ventas(
        datos['tabla_ventas'], 
        datos['tabla_de_ventas_consolidado']
    )

    if df_nuevos.empty:
        print("⚠️ No hay nuevos registros de ventas para procesar.")
        return

    print("\n🔄 Procesando tabla de ventas...")
    tabla_ventas_procesada = procesar_ventas(
        df_nuevos, 
        config['ultima_fecha'], 
        config['path']
    )
    
    print("\n📦 Concatenando con consolidado histórico...")
    df_consolidado = datos['tabla_de_ventas_consolidado']
    df_consolidado_ventas_actualizado = pd.concat([df_consolidado, tabla_ventas_procesada], ignore_index=True)

    ruta_salida = os.path.join(config['salida'], 'listado_consolidado_ventas_actualizado.xlsx')
    df_consolidado_ventas_actualizado.to_excel(ruta_salida, index=False)

    print(f"✅ Archivo ventas actualizado guardado en: {ruta_salida}")

    return df_consolidado_ventas_actualizado

def ejecutar_procesamiento_clientes():
   
    print("\n🔄 Procesando tabla de clientes...")

    if datos.get('tabla_clientes') is None:
        print("⚠️ No se encontró la tabla de clientes. Se omite el procesamiento.")
        return  # No hace nada si no hay tabla
    
    tabla_clientes_procesada = procesar_clientes(
        datos['tabla_clientes']
    )

    ruta_salida = os.path.join(config['salida'], 'tabla_clientes_actualizada.xlsx')
    tabla_clientes_procesada.to_excel(ruta_salida, index=False)

    print(f"✅ Archivo clientes actualizado guardado en: {ruta_salida}")

def ejecutar_creacion_tabla_conteo_mercado(df_consolidado_ventas_actualizado):
    print("\n🔄 Creando tabla de conteo mercado...") ### CAMBIAR EN LA FUNCION, LA SALIDA DEL ARCHIVO 
    tabla_conteo_mercado = crear_conteo_linea_mercado(
        df_consolidado_ventas_actualizado
    )

    ruta_salida = os.path.join(config['salida'], 'tabla_conteo_mercado_actual.xlsx') ## PREGUNTAR SI PONERLO CON LA HORA Y FECHA
    tabla_conteo_mercado.to_excel(ruta_salida, index=False)

    print(f"✅ Archivo tabla conteo de mercado actualizado guardado en: {ruta_salida}")

## TABLA MARCA REFERENCIA (DEBE SUMINISTRARSE, LA QUE ENVIAN)
def ejecutar_tabla_marca_referencia(df_consolidado_ventas_actualizado):
    print("\n🔄 Creando tabla de marca y referencia nueva...")
    tabla_marca_referencia = crear_tabla_marca_referencia(
        datos['tabla_mercado_referencia'], 
        df_consolidado_ventas_actualizado
    )
    
    ruta_salida = os.path.join(config['salida'], 'tabla_marca_referencia_actual.xlsx')
    tabla_marca_referencia.to_excel(ruta_salida, index=False)

    print(f"✅ Archivo tabla marca de referencia actualizado guardado en: {ruta_salida}")

    return tabla_marca_referencia

def ejecutar_actualizar_costos(datos, tabla_marca_referencia):
    print("\n🔄 Actualizando tabla de consolidado historico...")
    consolidado_costos_historico = consolidar_costos_historicos(
        datos, 
        tabla_marca_referencia
    )

    ruta_salida = os.path.join(config['salida'], 'tabla_consolidado_costos.xlsx')
    consolidado_costos_historico.to_excel(ruta_salida, index=False)
    print(f"✅ Archivo tabla de costos actualizada guardada en: {ruta_salida}")


def actualizar_consolidado_y_guardar_excel(datos):

    print("\n🔄 Creando consolidado Sell Out...")
    
    # Procesar cada cliente con manejo de None
    consolidado_promotora, _ = procesar_promotora(datos)
    consolidado_makro, _ = procesar_makro(datos)
    consolidado_farmatodo, _ = procesar_farmatodo(datos)

    # Lista para acumular los dataframes válidos
    dataframes = [datos['tabla_sell_out_consolidado']]  # Este siempre se incluye

    if consolidado_promotora is not None:
        dataframes.append(pd.DataFrame([consolidado_promotora]))

    if consolidado_makro is not None:
        dataframes.append(pd.DataFrame([consolidado_makro]))

    if consolidado_farmatodo is not None:
        dataframes.append(pd.DataFrame([consolidado_farmatodo]))

    # Concatenar solo si hay algo más que el original
    df_sell_out_actualizado = pd.concat(dataframes, ignore_index=True)

    nombre_archivo_excel =  "tabla_sell_out_consolidado_actualizado.xlsx"
    ruta_salida = os.path.join(config['salida'], nombre_archivo_excel)
    df_sell_out_actualizado.to_excel(ruta_salida, index=False)

    return df_sell_out_actualizado

def actualizar_consolidado_sell_out_ciudades_y_guardar_excel(datos):
    print("\n🔄 Creando consolidado Sell Out Ciudad Descripción...")

    # Procesar clientes
    _, df_promotora = procesar_promotora(datos)
    _, df_makro = procesar_makro(datos)
    _, df_farmatodo = procesar_farmatodo(datos)

    # Lista para acumular los dataframes válidos
    dataframes_ciudades = []

    # Agregar la tabla base si existe
    base_ciudades = datos.get("tabla_sell_out_ciudades_consolidado")
    if base_ciudades is not None:
        dataframes_ciudades.append(base_ciudades)

    # Agregar los procesados solo si existen
    if df_promotora is not None:
        dataframes_ciudades.append(df_promotora)

    if df_makro is not None:
        dataframes_ciudades.append(df_makro)

    if df_farmatodo is not None:
        dataframes_ciudades.append(df_farmatodo)

    # Verificar que haya al menos una tabla para concatenar
    if not dataframes_ciudades:
        print("⚠️ No hay datos para consolidar Sell Out Ciudades.")
        return None

    # Concatenar
    df_sell_out_actualizado_ciudades = pd.concat(dataframes_ciudades, ignore_index=True)

    # Guardar el resultado
    nombre_archivo_excel = "tabla_sell_out_ciudad_consolidado_actualizada.xlsx"
    ruta_salida = os.path.join(config['salida'], nombre_archivo_excel)
    df_sell_out_actualizado_ciudades.to_excel(ruta_salida, index=False)

    print(f"✅ Consolidado actualizado y guardado en {ruta_salida}")

    return df_sell_out_actualizado_ciudades

    

if __name__ == "__main__":

     # 1. CONFIGURACIÓN INICIAL
    print("📁 Configurando rutas y parámetros...")
    config = configurar_rutas()

    # 2. CARGA DE DATOS
    print("\n📊 Cargando datos desde archivos Excel...")
    datos = cargar_datos(config)

    # 3. PROCESAMIENTO DE VENTAS
    df_consolidado_ventas_actualizado = ejecutar_actualizacion_ventas()

    # 4. PROCESAMIENTO DE CLIENTES
    ejecutar_procesamiento_clientes()

    # 5. TABLA DE CONTEO MERCADO
    if df_consolidado_ventas_actualizado is not None:
        ejecutar_creacion_tabla_conteo_mercado(df_consolidado_ventas_actualizado)
    else:
        ejecutar_creacion_tabla_conteo_mercado(datos['tabla_de_ventas_consolidado'])
    
    # 6. TABLA DE MARCA Y REFERENCIA
    if df_consolidado_ventas_actualizado is not None:
        tabla_marca_referencia = ejecutar_tabla_marca_referencia(df_consolidado_ventas_actualizado)
    else:
        tabla_marca_referencia = ejecutar_tabla_marca_referencia(datos['tabla_de_ventas_consolidado'])

    # 7. PROCESAMIENTO COSTOS
    ejecutar_actualizar_costos(datos, tabla_marca_referencia)


    # 8. PROCESAMIENTO DE CLIENTES GRANDES
    ## 8.1 Actualización tabla Sell out 
    actualizar_consolidado_y_guardar_excel(datos)

    ## 8.2 Actualización tabla Sell out Ciudad y Descripción
    actualizar_consolidado_sell_out_ciudades_y_guardar_excel(datos)
    