# Entrega 3 Big Data - Spark

## Integrantes

- **Laura León** - 17-10307
- **Juan Cuevas** - 19-10056
- **Anya Marcano** - 19-10336

## Instalación

Para instalar las dependencias necesarias para el proyecto, se debe ejecutar el siguiente comando en la terminal:

```bash
./env.sh --build
```

Esto instalará las dependencias necesarias para el proyecto y creará un entorno virtual en la carpeta `venv`.
Para activar el entorno virtual, se debe ejecutar el siguiente comando en la terminal:

```bash
source venv/bin/activate
```

> **Nota**
>
> En caso de que quiera desactivar el entorno virtual, se debe ejecutar el siguiente comando en la terminal:
> ```bash
> deactivate
> ```
>
> Y si desea eliminar el entorno virtual, se debe ejecutar el siguiente comando en la terminal:
> ```bash
> ./env.sh --clean
> ```

## Uso

Para ejecutar el servidor de conexión, se debe ejecutar el siguiente comando en la terminal:

```bash
start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:$SPARK_VERSION
```

> **Nota**
>
> Para detener el servidor, se debe ejecutar el siguiente comando en la terminal:
> ```bash
> stop-connect-server.sh
> ```

Para ejecutar el cliente de conexión, se debe ejecutar el siguiente comando en la terminal:

```bash
./main.py <n> <file>
```

o

```bash
python main.py <n> <file>
```

> **Nota**
>
> El arhivo `main.py` tiene un hashbang que permite ejecutarlo directamente como un script de Python. Por lo que no es necesario ejecutar el comando `python` antes del archivo.
> Sin embargo, esto depende de la configuración de su sistema operativo y de la versión de Python que tenga instalada.

Donde `<n>` es el número de nodos y `<file>` es el archivo de texto que se desea procesar.

### Ejemplo de uso

Para ejecutar el cliente de conexión, se debe ejecutar el siguiente comando en la terminal:

```bash
./main.py 3 data.csv
```

## Entrenamiento de los modelos

Para entrenar los modelos, se debe ejecutar el siguiente comando en la terminal:

```bash
./train.py
```

Esto entrenará los modelos y guardará los resultados en la carpeta `models`.

> **Importante**
>
> Para poder entrenar los modelos, se debe de tener la Base de datos `spotify.sqlite` en la carpeta la raíz del proyecto.
>
> Si desea descargar la base de datos, se puede hacer click en el siguiente [enlace](https://www.kaggle.com/datasets/maltegrosse/8-m-spotify-tracks-genre-audio-features).

## Correlación

Para calcular la correlación entre las variables, se debe ejecutar el siguiente comando en la terminal:

```bash
./correlation.py <n>
```

Donde `<n>` es el número de filas que se desea procesar para calcular la correlación (es opcional). Por defecto, el número de filas es 100000.

> **Advertencia**
>
> Es posible usar todo el Dataset, pero tomen en cuenta que son mas de 10.000.000 filas y puede tardar mucho tiempo en procesarse, o incluso puede que no se procese por exceder la memoria disponible.
