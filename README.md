# ExcelProcessing

Proyecto para procesar un excel donde las columnas se hacen match con las columnas del nuevo archivo, se homologa columnas con valores por defecto y se realiza consulta a la base de datos para completar columnas que se requiera en el archivo final.
El directorio input contiene 3 json que permite definir:
* excel_columns describe las columnas del archivo inicial con el nuevo nombre que se mapea en el archivo final
* homologate describe la consulta a base de datos y cual sera el nuevo nombre de la columna recuperada, asi como tambien aquellas nuevas columnas que se describen con valores por defecto
* output_columns describe las columnas finales del archivo de salida con la especificacion de la longitud de caracteres permitidos.
