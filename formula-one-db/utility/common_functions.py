# Databricks notebook source
def merge_data_to_table(df, target_table, merge_condition):

    df.createOrReplaceTempView('temp_v')
    columnsList  = df.schema.names

    update_columns = ", ".join([f"tgt.{col} = src.{col}" for col in columnsList])
    insert_columns = ", ".join(columnsList)
    insert_values = ", ".join([f"src.{col}" for col in columnsList])

    spark.sql(f"""
    MERGE INTO {target_table} tgt
    USING temp_v src
    ON {merge_condition}
    WHEN MATCHED THEN
    UPDATE SET {update_columns}
    WHEN NOT MATCHED THEN
    INSERT ({insert_columns}) VALUES ({insert_values})
    """
    )
