import pandas as pd
import os

from funciones_auxiliares.funciones_principales import (configurar_rutas, cargar_datos_clientes)
from funciones_auxiliares.funciones_clientes_grandes import (procesar_promotora, procesar_makro ,procesar_farmatodo)

def actualizar_consolidado_y_guardar_excel(datos):
    
    consolidado_promotora, _ = procesar_promotora(datos)
    consolidado_makro, _ = procesar_makro(datos)
    consolidado_farmatodo, _ = procesar_farmatodo(datos)

    dataframes = [datos['tabla_sell_out_consolidado']]

    if consolidado_promotora is not None:
        dataframes.append(pd.DataFrame([consolidado_promotora]))

    if consolidado_makro is not None:
        dataframes.append(pd.DataFrame([consolidado_makro]))

    if consolidado_farmatodo is not None:
        dataframes.append(pd.DataFrame([consolidado_farmatodo]))

    df_sell_out_actualizado = pd.concat(dataframes, ignore_index=True)

    nombre_archivo_excel =  "Tiendas Sell out.xlsx"
    ruta_salida = os.path.join(config['salida'], nombre_archivo_excel)
    df_sell_out_actualizado.to_excel(ruta_salida, index=False)

    print(f" Consolidado actualizado y guardado en {ruta_salida}")

    return df_sell_out_actualizado

def actualizar_consolidado_sell_out_ciudades_y_guardar_excel(datos):

    _, df_promotora = procesar_promotora(datos)
    _, df_makro = procesar_makro(datos)
    _, df_farmatodo = procesar_farmatodo(datos)

    dataframes_ciudades = []

    base_ciudades = datos.get("tabla_sell_out_ciudades_consolidado")
    if base_ciudades is not None:
        dataframes_ciudades.append(base_ciudades)

    if df_promotora is not None:
        dataframes_ciudades.append(df_promotora)

    if df_makro is not None:
        dataframes_ciudades.append(df_makro)

    if df_farmatodo is not None:
        dataframes_ciudades.append(df_farmatodo)

    if not dataframes_ciudades:
        print("No hay datos para consolidar Sell Out Ciudades.")
        return None

    df_sell_out_actualizado_ciudades = pd.concat(dataframes_ciudades, ignore_index=True)

    nombre_archivo_excel = "Tiendas Sell out ciudad descripcion.xlsx"
    ruta_salida = os.path.join(config['salida'], nombre_archivo_excel)
    df_sell_out_actualizado_ciudades.to_excel(ruta_salida, index=False)

    print(f" Consolidado actualizado y guardado en {ruta_salida}")
    return df_sell_out_actualizado_ciudades


# 1. CONFIGURACIÓN INICIAL
print("📁 Configurando rutas y parámetros...")
config = configurar_rutas()

# 2. CARGA DE DATOS
print("\n📊 Cargando datos desde archivos Excel...")
datos = cargar_datos_clientes(config)

# 3. PROCESAMIENTO DE CLIENTES GRANDES
## 3.1 Actualización tabla Sell out 
print("\n 🔄 Creando consolidado Sell Out...")
actualizar_consolidado_y_guardar_excel(datos)

## 3.2 Actualización tabla Sell out Ciudad y Descripción
print("\n 🔄 Creando consolidado Sell Out Ciudad Descripción...")
actualizar_consolidado_sell_out_ciudades_y_guardar_excel(datos)
