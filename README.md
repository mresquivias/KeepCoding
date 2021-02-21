# KeepCoding
€€ Big Data Processing

Este proyecto consta de dos capas principales:
  - Una capa, llamad **stream layer**, en la cual, recibimos datos de señales de móvilesa enviadas través de un docker instalado en una instancia de *Kafka*. Esta capa, realiza un prcesamiento en streaming en la cual, en ventanas de 5 minutos, se va recogiendo los datos, enriqueciéndolos con metadatos de una tabla quetenemos creada en *PostgreSQL* en *Google CLoud* llamada **user_metadata**:
  - ![alt text][/mresquivias/KeepCoding/blob/big-data-processing/user_metadata.png?raw=true]

  - Estos datos enriquecidos, tras una serie de operaciones se añaden a *PostgreSQL* en una nuueva tabla llamada **bytes**, en la cual tenemos los datos agrupados por id, id de antena y aplicación:
  - https://github.com/mresquivias/KeepCoding/blob/big-data-processing/bytes.png?raw=true

  - A su vez, estos datos se van almacenando en formato *parquet* en *GoogleStorage* o en local mediante una partición por año, mes, día y hora.
  - https://github.com/mresquivias/KeepCoding/blob/big-data-processing/parquet.jpg?raw=true

  - Una vez almacenados, entra en juego la siguiente capa, llamada **batch layer**. Esta capa, al contrario que la anterior, no procesa los datos en streaming, si no que lee los datos de GoogleStorage o local, los procesa y luego los guarda en las tablas de *PostgreSQL*.
  - Este procesamiento consiste en, aprovechando la partición del formato parquet, agrupar los datos de la tabla **bytes** por horas, resultando en la tabla **bytes_hourly**:
  - https://github.com/mresquivias/KeepCoding/blob/big-data-processing/bytes_hourly.png?raw=true

  - A su vez, también calcula, aprovechando los metadatos, si los usuarios se han pasado de su cuota de datos, y lo almacena en una nueva tabla en PostgresSQL llamada **user_quota_limit**
  - https://github.com/mresquivias/KeepCoding/blob/big-data-processing/user_quota_limit.png?raw=true

  - Por último añdimos una nuca capa llamada **serving layer**, en la cual, aprovechando la instancia que ya teníamos creada, montamos otro docker con *Superset* que nos sirve para conectarnos a la basse de datos de *PostgreSQL* y, desde ahí, crear visualizaciones de datos.
  - https://github.com/mresquivias/KeepCoding/blob/big-data-processing/Dashboard.jpg?raw=true
