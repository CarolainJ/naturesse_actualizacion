"""
Módulo para análisis RFM (Recency, Frequency, Monetary) de clientes por canal de venta.

Este módulo proporciona una clase completa para realizar análisis RFM de clientes,
permitiendo segmentar a los clientes en diferentes categorías basadas en su comportamiento
de compra reciente, frecuencia de compra y valor monetario de sus transacciones.
"""

import os
import re

import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Tuple, Optional, List

from funciones_auxiliares.funciones_principales import (configurar_rutas, cargar_datos_rfm)

class RFMAnalysis:
    """
    Clase para realizar análisis RFM (Recency, Frequency, Monetary) de clientes.
    
    El análisis RFM es una técnica de segmentación de clientes que evalúa tres dimensiones:
    - Recency (Recencia): Qué tan reciente fue la última compra del cliente
    - Frequency (Frecuencia): Con qué frecuencia compra el cliente
    - Monetary (Monetario): Cuánto dinero gasta el cliente
    
    Attributes:
        canales_interes (List[str]): Lista de canales de venta a analizar
        categorias_rfm (Dict[str, List[int]]): Mapeo de scores RFM a categorías de clientes
    """

    def __init__(self):
        """
        Inicializa la clase RFMAnalysis con los canales de interés y categorías RFM predefinidas.
        """
         
        self.canales_interes = [
            "003 - WEBSITE", "007 - HOTELES", "011 - REDES SOCIALES",
            "016 - INSTITUCIONAL/EMPRESA", "009 - PUNTO DE VENTA", "013 - ECOMMERCE"
        ]
    
        self.categorias_rfm = {
            'Campeones': [555, 554, 544, 545, 454, 455, 445],
            'Leales': [543, 444, 435, 355, 354, 345, 344, 335],
            'Con potencial': [553, 551, 552, 541, 542, 533, 532, 531, 452, 451, 442, 
                             441, 431, 453, 433, 432, 423, 353, 352, 351, 342, 341, 333, 323],
            'Nuevos clientes': [512, 511, 422, 421, 412, 411, 311],
            'Movilizar': [525, 524, 523, 522, 521, 515, 514, 513, 425, 424, 413, 414, 415, 315, 314, 313],
            'Prestar atención': [535, 534, 443, 434, 343, 334, 325, 324],
            'Ballenas': [155, 154, 144, 214, 215, 115, 114, 113],
            'Cerca de hibernar': [331, 321, 312, 221, 213],
            'En riesgo': [255, 254, 245, 244, 253, 252, 243, 242, 235, 234, 225, 224, 
                         153, 152, 145, 143, 142, 135, 134, 133, 125, 124],
            'Hibernando': [332, 322, 231, 241, 251, 233, 232, 223, 222, 132, 123, 122, 212, 211],
            'Inactivos': [111, 112, 121, 131, 141, 151]
        }
    
    def limpiar_nombres_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia y estandariza los nombres de las columnas de un DataFrame.
        
        Aplica las siguientes transformaciones:
        - Convierte a minúsculas
        - Reemplaza espacios con guiones bajos
        - Elimina caracteres especiales (acentos, puntos)
        
        Args:
            df (pd.DataFrame): DataFrame con columnas a limpiar
            
        Returns:
            pd.DataFrame: DataFrame con nombres de columnas estandarizados
            
        """

        df.columns = df.columns.str.lower()
        df.columns = df.columns.str.replace(' ', '_', regex=False)
        df.columns = df.columns.str.replace('ó', 'o', regex=False)
        df.columns = df.columns.str.replace('.', '', regex=False)
        return df
    
    def limpiar_nit(self, nit_series: pd.Series) -> pd.Series:
        """
        Limpia los números de identificación tributaria (NIT) eliminando sufijos.
        
        Remueve todo lo que esté después del primer guión en el NIT,
        manteniendo solo la parte principal del número de identificación.
        """
        
        return nit_series.astype(str).str.replace(r'-.*', '', regex=True)
    
    def cargar_y_limpiar_datos(self, bd_ventas, bd_clientes) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Carga y limpia los datos de ventas y clientes aplicando transformaciones estándar.
        
        Procesa ambos DataFrames aplicando:
        - Limpieza de nombres de columnas
        - Limpieza de NITs
        - Conversión de tipos de datos
        - Eliminación de duplicados en clientes
        - Estandarización de nombres de columnas """

        bd_ventas = self.limpiar_nombres_columnas(bd_ventas)
        bd_ventas['nit'] = self.limpiar_nit(bd_ventas['nit'])
        
        if 'valor_bruto_local' in bd_ventas.columns:
            bd_ventas['valor_bruto_local'] = bd_ventas['valor_bruto_local'].astype(str).str.replace(',', '.')
            bd_ventas['valor_bruto_local'] = pd.to_numeric(bd_ventas['valor_bruto_local'], errors='coerce')

        bd_clientes = self.limpiar_nombres_columnas(bd_clientes)
        bd_clientes.columns = ['tipo_identificacion', 'nit', 'razon_social']
        bd_clientes['nit'] = self.limpiar_nit(bd_clientes['nit'])
        
        bd_clientes = bd_clientes.drop_duplicates(keep='first')
        bd_clientes = bd_clientes.drop('tipo_identificacion', axis=1)
        
        return bd_ventas, bd_clientes
    
    def filtrar_datos_ventas(self, bd_ventas: pd.DataFrame, bd_clientes: pd.DataFrame, ruta_salida) -> pd.DataFrame:
        """
        Filtra y procesa los datos de ventas aplicando reglas de negocio específicas.
        
        Aplica los siguientes filtros y transformaciones:
        - Excluye referencias de productos específicas
        - Elimina registros con referencia nula
        - Combina datos de ventas con información de clientes
        - Exporta NITs sin razón social para revisión
        - Convierte fechas a formato datetime
        
        Args:
            bd_ventas (pd.DataFrame): DataFrame con datos de ventas limpios
            bd_clientes (pd.DataFrame): DataFrame con datos de clientes limpios
            ruta_salida (str): Ruta donde guardar archivos de salida
            
        Returns:
            pd.DataFrame: DataFrame de ventas filtrado y enriquecido con datos de clientes
            
        Note:
            - Se excluyen las referencias: 10133, 10137, 10138, 10135, 13035, 13034, 1501, 1502
            - Se genera un archivo Excel con NITs que no tienen razón social asociada
            - Se realiza un LEFT JOIN entre ventas y clientes por NIT
        """

        referencias_excluir = [10133, 10137, 10138, 10135, 13035, 13034, 1501, 1502]
        bd_ventas = bd_ventas[~bd_ventas['referencia'].isin(referencias_excluir)]
        bd_ventas = bd_ventas[bd_ventas['referencia'].notna()]

        if 'razon_social' in bd_ventas.columns:
            bd_ventas = bd_ventas.drop(columns=['razon_social'])

        bd_ventas = pd.merge(bd_ventas, bd_clientes, on='nit', how='left')
        
        nit_sin_razonsocial = bd_ventas[bd_ventas['razon_social'].isna()]
        ruta_salida_nit = os.path.join(ruta_salida, 'nit_sin_razonsocial.xlsx')
        nit_sin_razonsocial.to_excel(ruta_salida_nit, index=False)
        print(f" Resultados de  nits sin razon social guardados en {ruta_salida_nit}")


        if 'fecha' in bd_ventas.columns:
            bd_ventas['fecha'] = pd.to_datetime(bd_ventas['fecha'], errors='coerce')
    
        return bd_ventas
    
    def calcular_rfm_scores(self, df: pd.DataFrame, fecha_analisis: date) -> pd.DataFrame:
        """
        Calcula los scores RFM (Recency, Frequency, Monetary) para cada cliente.
        
        Calcula las tres métricas fundamentales del análisis RFM:
        - Recency: Días desde la última compra hasta la fecha de análisis
        - Frequency: Número total de transacciones del cliente
        - Monetary: Valor total de las compras del cliente
        
        Cada métrica se convierte en un score de 1-5 usando quintiles:
        - Recency: 5 = más reciente, 1 = menos reciente
        - Frequency: 5 = más frecuente, 1 = menos frecuente  
        - Monetary: 5 = mayor valor, 1 = menor valor
        
        Args:
            df (pd.DataFrame): DataFrame con columnas: customer_id, order_date, revenue
            fecha_analisis (date): Fecha de referencia para calcular la recencia
            
        Returns:
            pd.DataFrame: DataFrame con scores RFM y métricas calculadas
            
        """
        fecha_analisis = pd.to_datetime(fecha_analisis)
        rfm_df = df.groupby('customer_id').agg({
            'order_date': lambda x: (fecha_analisis - x.max()).days,  # Recency
            'customer_id': 'count',  # Frequency
            'revenue': 'sum'  # Monetary
        }).rename(columns={
            'order_date': 'recency_days',
            'customer_id': 'frequency',
            'revenue': 'monetary'
        })
        
        rfm_df['recency_score'] = pd.qcut(rfm_df['recency_days'], 5, labels=[5,4,3,2,1], duplicates='drop')
        rfm_df['frequency_score'] = pd.qcut(rfm_df['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5], duplicates='drop')
        rfm_df['monetary_score'] = pd.qcut(rfm_df['monetary'], 5, labels=[1,2,3,4,5], duplicates='drop')
        
        rfm_df['rfm_score'] = (rfm_df['recency_score'].astype(str) + 
                              rfm_df['frequency_score'].astype(str) + 
                              rfm_df['monetary_score'].astype(str)).astype(int)
        
        return rfm_df.reset_index()
    
    def asignar_categoria_rfm(self, score: int) -> str:
        """
        Asigna una categoría de cliente basada en el score RFM.
        
        Mapea scores RFM numéricos (e.g., 555, 234, 111) a categorías de negocio
        que describen el comportamiento y valor del cliente.
        
        Args:
            score (int): Score RFM de 3 dígitos (e.g., 555, 234, 111)
            
        Returns:
            str: Categoría del cliente ('Campeones', 'Leales', 'En riesgo', etc.)
                 Retorna 'No clasificado' si el score no coincide con ninguna categoría
                 
        """

        for categoria, scores in self.categorias_rfm.items():
            if score in scores:
                return categoria
        return 'No clasificado'
    
    def procesar_rfm_por_canal(self, ruta_salida, bd_agregada_completa: pd.DataFrame, 
                              canales: Optional[List[str]] = None,
                              fecha_analisis: date = None,
                              ) -> pd.DataFrame:
    
        """
        Procesa el análisis RFM para cada canal de venta y genera resultados consolidados.
        
        Ejecuta el análisis RFM completo para cada canal de interés:
        1. Filtra datos por canal
        2. Calcula scores RFM
        3. Asigna categorías de cliente
        4. Consolida resultados de todos los canales
        5. Exporta resultados a CSV
        
        Args:
            ruta_salida (str): Ruta donde guardar los archivos de resultados
            bd_agregada_completa (pd.DataFrame): DataFrame con datos agregados por canal
            canales (Optional[List[str]]): Lista de canales a procesar. 
                                         Si es None, usa self.canales_interes
            fecha_analisis (Optional[date]): Fecha de referencia para el análisis.
                                           Si es None, usa la fecha actual
                                           
        Returns:
            pd.DataFrame: DataFrame consolidado con análisis RFM de todos los canales
            
        Note:
            - Se genera un archivo CSV 'rfm_segmentos.csv' con todos los resultados
            - Si no hay datos para procesar, retorna un DataFrame vacío
        """

        if fecha_analisis is None:
            fecha_analisis = date.today()
        
        canales = self.canales_interes
        total_segmentos = []
        
        for canal in canales:
            bd_canal = bd_agregada_completa[bd_agregada_completa['canal'] == canal].copy()
            if bd_canal.empty:
                continue

            rfm_resultado = self.calcular_rfm_scores(bd_canal, fecha_analisis)
            rfm_resultado['canal'] = canal
            total_segmentos.append(rfm_resultado)
   
        if total_segmentos:
            total_segmentos_df = pd.concat(total_segmentos, ignore_index=True)
            total_segmentos_df['categoria'] = total_segmentos_df['rfm_score'].apply(self.asignar_categoria_rfm)
            
            total_segmentos_df = total_segmentos_df.rename(columns={
                'customer_id': 'id_cliente',
                'recency_days': 'dias_recencia',
                'frequency': 'num_transacciones',
                'monetary': 'monto',
                'recency_score': 'recencia_punt',
                'frequency_score': 'frecuencia_punt',
                'monetary_score': 'valor_punt',
                'rfm_score': 'score_rfm'
            })
            
            total_segmentos_df['ingresos'] = total_segmentos_df['monto']

            ruta_salida_csv = os.path.join(ruta_salida, 'rfm_segmentos.csv')
            total_segmentos_df.to_csv(ruta_salida_csv, index=False)
            print(f" Resultados de segmentos Rfm guardados en {ruta_salida_csv}")
            
            return total_segmentos_df
        else:
            return pd.DataFrame()
    
    def ejecutar_analisis_completo(self, ventas, clientes, 
                                  ruta_salida: str) -> pd.DataFrame:
        
        """
        Ejecuta el pipeline completo de análisis RFM desde datos crudos hasta resultados finales.
        
        Este método orquesta todo el proceso de análisis RFM:
        1. Carga y limpia los datos de ventas y clientes
        2. Aplica filtros de negocio
        3. Agrega datos por canal, cliente y fecha
        4. Convierte valores a miles para mejor manejo
        5. Ejecuta análisis RFM por canal
        6. Exporta resultados finales
        
        Args:
            ventas (pd.DataFrame): DataFrame crudo con datos de transacciones
            clientes (pd.DataFrame): DataFrame crudo con datos de clientes
            ruta_salida (str): Ruta del directorio donde guardar todos los archivos de salida
            
        Returns:
            pd.DataFrame: DataFrame con resultados completos del análisis RFM
            
        Generated Files:
            - nit_sin_razonsocial.xlsx: NITs sin información de cliente para revisión
            - rfm_segmentos.csv: Resultados detallados por cliente y canal
            - rfm_resultados.xlsx: Resultados finales consolidados
            
        Data Pipeline:
            ventas + clientes → limpieza → filtros → agregación → RFM → categorización → export
            
        """

        bd_ventas, bd_clientes = self.cargar_y_limpiar_datos(ventas, clientes)
        bd_ventas = self.filtrar_datos_ventas(bd_ventas, bd_clientes, ruta_salida)
        bd_agregada_completa = (bd_ventas
                               .groupby(['linea_de_mercado', 'razon_social', 'fecha'])['valor_bruto_local']
                               .sum()
                               .reset_index())
        
        bd_agregada_completa.columns = ['canal', 'customer_id', 'order_date', 'revenue']
        bd_agregada_completa['order_date'] = pd.to_datetime(bd_agregada_completa['order_date'])
        bd_agregada_completa['revenue_miles'] = bd_agregada_completa['revenue'] / 1000
        
        bd_agregada_completa['revenue'] = bd_agregada_completa['revenue_miles']

        resultados_rfm = self.procesar_rfm_por_canal(ruta_salida, bd_agregada_completa)

        if not resultados_rfm.empty:
            ruta_salida_final = os.path.join(ruta_salida, "rfm_resultados.xlsx")
            resultados_rfm.to_excel(ruta_salida_final, index=False)
            print(f" Resultados Rfm guardados en {ruta_salida_final}")
        
        return resultados_rfm
    

# EJECUCIÓN DEL ANÁLISIS
analizador = RFMAnalysis()

print("📁 Configurando rutas y parámetros...")
config = configurar_rutas()

print("\n📊 Cargando datos desde archivos Excel...")
datos = cargar_datos_rfm(config)

print("\n 🔄 Generando resultados RFM ")
ruta_salida = os.path.join(config['salida'], 'rfm/')
resultados = analizador.ejecutar_analisis_completo(
    datos['tabla_ventas_rfm'], 
    datos['tabla_clientes'], 
    ruta_salida
)