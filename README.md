# Proyecto Naturesse

Este proyecto realiza el procesamiento automatizado de los códigos de **Naturesse**, incluyendo la consolidación de clientes grandes, análisis RFM, ventas, y actualización de costos.

---

## 📥 Clonar el repositorio

Puedes obtener este repositorio de dos formas:

1. Clonarlo desde la terminal:

```bash
git clone https://github.com/CarolainJ/naturesse_actualizacion.git
```

2. O descargando el archivo ZIP desde GitHub y extrayéndolo en tu equipo.

---

## 🔧 Requisitos

### Crear entorno virtual (opcional pero recomendado)

Antes de instalar las dependencias, se recomienda crear un entorno virtual:

En Windows:
```bash
python -m venv venv
.\venv\Scripts\activate
```

En macOS/Linux:
```bash
python3 -m venv venv
source venv/bin/activate
```


Antes de ejecutar el proyecto, asegúrate de:

1. Tener **Python 3.8 o superior** instalado.
2. Instalar las dependencias con:

```bash
pip install -r requirements.txt
```
(Debes tener el entorno virtual activo)

---

## ⚙️ Configuración

1. Abre el archivo `config.py` ubicado en la carpeta `funciones_auxiliares`.
2. Modifica la variable `BASE_DIR` con la ruta local donde tienes alojado el proyecto.

---

## ▶️ Ejecución

### 1. Consolidar clientes grandes

Ejecuta el siguiente comando:

```bash
python consolidar_clientes_grandes.py
```

> **Importante**: Los archivos de clientes grandes deben tener el nombre con el formato `MES_CLIENTE`, donde `MES` son las tres letras en mayúscula del mes a procesar.  
> Ejemplos válidos:
> - `JUN_FARMATODO`
> - `MAR_MAKRO`
> - `ABR_PROMOTORA`

---

### 2. Análisis RFM

Para ejecutar el análisis RFM, usa:

```bash
python rfm.py
```

> **Recomendación**: se deben tener al menos **6 meses de datos** para obtener resultados representativos.

---

### 3. Procesamiento general

Para ejecutar el procesamiento de ventas, clientes, conteo por línea de mercado y actualización de costos, ejecuta:

```bash
python main.py
```

---

## 📁 Estructura del proyecto

```
.
├── main.py
├── consolidar_clientes_grandes.py
├── rfm.py
├── requirements.txt
├── .gitignore
├── README.md
│
├── funciones_auxiliares/
│   ├── config.py
│   ├── funciones.py
│   ├── funciones_clientes_grandes.py
│   ├── funciones_principales.py
│   └── funciones_procesos.py
│
└── datos/
    ├── tablas_consolidados/
    └── tablas_entrada/
```

---

## ❗ Notas importantes

- **NO modifiques los nombres de los archivos de entrada**, excepto en el caso de los clientes grandes, que deben seguir el formato `MES_CLIENTE`.
- Todos los resultados generados por los scripts se almacenan en la carpeta `SALIDAS`.
- **Los archivos de salida no reemplazan automáticamente** los datos históricos o consolidados.
- Si deseas conservar estos resultados de forma definitiva, **mueve manualmente** los archivos de la carpeta `SALIDAS` a:
  - `datos/tablas_consolidados/` o
  - `datos/tablas_entrada/`, según corresponda.

---