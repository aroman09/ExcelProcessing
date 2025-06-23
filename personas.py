
import pandas as pd
import json
import oracledb
from sqlalchemy import create_engine, text
import sys

INPUT_FILE = "input/archivo.xlsx"
OUTPUT_FILE = "output/personas.xlsx"
usuario = "tu_usuario"
clave = "tu_clave"
host = "localhost"
puerto = 1111
service_name = "orclpdb1"
DB_URI = f"oracle+oracledb://{usuario}:{clave}@{host}:{puerto}/?service_name={service_name}"
ORACLE_CLIENT_PATH = r"C:\ruta\oracle\instantclient_23_8"


def main():
    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_PATH)
    config_columns = cargar_json("config/excel_columns.json")
    config_extra = cargar_json("config/homologate.json")
    config_output = cargar_json("config/output_columns.json")

    column_mapping = config_columns["column_mapping"]
    columns_max = config_output["column_order"]
    column_order= list(columns_max.keys())

    df = cargar_excel(INPUT_FILE)
    
    df = aplicar_valores_fijos(df, config_extra["fixed_values"])

    engine = create_engine(DB_URI)
    verificar_conexion(engine)
    df = completar_columnas_db(df, config_extra["database"], engine)
    df = renombrar_columnas(df, column_mapping)
    df = truncar_columnas(df, columns_max)
    df = reordenar_columnas(df, column_order)

    df = df.astype(str).replace(["nan","NaN","None","NoneType"],"")

    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name="RECMIGRATION", index=False)

        workbook  = writer.book
        worksheet = writer.sheets["RECMIGRATION"]

        header_format = workbook.add_format({
            'border': 0
        })

        for col_num, value in enumerate(df.columns):
            worksheet.write(0, col_num, value, header_format)

    print(f"✅ Archivo generado: {OUTPUT_FILE}")

def cargar_json(ruta):
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)

def cargar_excel(ruta):
    return pd.read_excel(ruta)

def verificar_conexion(engine):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM dual"))
        print("Conexion establecida con la base de datos")
    except Exception as e:
        print("❌ Error al conectar a la base de datos")
        sys.exit(1)

def reemplazar_coma(df,point_columns):
    for col in point_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',','.', regex=False)
    return df

def truncar_columnas(df,columns_max):
    for col, max_len in columns_max.items():
        if col in df.columns:
            df[col] = df[col].astype(str).str.slice(0, max_len)
    return df

def completar_columnas_db(df, config_db, engine):
    source_column = config_db["source_column"]
    query_template = config_db["query"]
    match_columns = config_db["match_columns"]
    output_columns = config_db["output_columns"]
    batch_size = config_db["batch_size"]
    default_value = config_db.get("default", "")

    ids = df[source_column].dropna().astype(str).unique().tolist()
    resultados = {}
    
    for i in range(0, len(ids), batch_size):
        batch = ids[i:i + batch_size]
        condiciones = " OR ".join([f"BIC = '{val}'" for val in batch])
        query = query_template.format(condiciones)
        try:
            df_result = pd.read_sql(text(query), con=engine)
            if not df_result.empty:
                for _, row in df_result.iterrows():
                    resultados[str(row["bic"])] = {col:row.get(col,default_value) for col in match_columns}
        except Exception as e:
            print(f"❌ Error al ejecutar query para batch {i//batch_size + 1}: {e}")
    
    for col in output_columns:
        df[col]=df[source_column].astype(str).str.strip().map(
            lambda x: resultados.get(x,{}).get(match_columns[output_columns.index(col)],default_value)
        )
    return df

def aplicar_valores_fijos(df, valores_fijos):
    for col, val in valores_fijos.items():
        df[col] = val
    return df

def renombrar_columnas(df, mapping):
    for new_name, name_excel in mapping.items():
        if name_excel in df.columns:
            df[new_name] = df[name_excel]
    return df

def reordenar_columnas(df, orden):
    columnas_actuales = df.columns.tolist()
    columnas_faltantes = [col for col in orden if col not in columnas_actuales]

    for col in columnas_faltantes:
        df[col] = ""

    # Reordenar según orden deseado
    return df[orden]

if __name__ == "__main__":
    main()
