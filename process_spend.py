import pandas as pd

from google.cloud import bigquery

client = bigquery.Client()

identifier_column_name = 'Supplier_Name_Normalized'
amount_column_name = 'Spend'

client = bigquery.Client()
query="" # Query here
spend_df = client.query(query).to_dataframe()
spend_df['Client_ID'] = spend_df['Client_ID'].astype(str)

df = spend_df.sort_values(by=identifier_column_name)
df['supplier_spend'] = pd.to_numeric(df[amount_column_name], errors='coerce')
df['supplier_total_spend'] = df.groupby(identifier_column_name)['supplier_spend'].transform('sum')

df = df.assign(all_supplier_total_spend=df['supplier_spend'].sum())
df.drop_duplicates(
    subset=[identifier_column_name, 'supplier_total_spend', 'all_supplier_total_spend'],
    keep='first',
    inplace=True
)

df['supplier_spend_percentage'] = round((df['supplier_total_spend'] / df['all_supplier_total_spend']) * 100, 4)
df = df.sort_values(by='supplier_total_spend', ascending=False)
df['cumulative_sum'] = df['supplier_spend_percentage'].cumsum()

threshold_value = 90.0
spend_percent_condition = df['supplier_spend_percentage'] > 0
cumulative_sum_condition = df['cumulative_sum'] <= threshold_value
df = df[spend_percent_condition & cumulative_sum_condition]

df_to_insert = spend_df[spend_df[identifier_column_name].isin(df[identifier_column_name])]
df_to_insert = df_to_insert.merge(df[[identifier_column_name, 'supplier_spend_percentage']], on=identifier_column_name, how='left')
df_to_insert.rename(columns={'supplier_spend_percentage': 'Spend_Percentage'}, inplace=True)
df_to_insert = df_to_insert.sort_values(by=identifier_column_name, ascending=False)

table_id = ''
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('Client_ID', 'STRING'),
        bigquery.SchemaField('Supplier_Name_Normalized', 'STRING'),
        bigquery.SchemaField('Spend', 'Float'),
        bigquery.SchemaField('Region', 'STRING'),
        bigquery.SchemaField('Country', 'STRING'),
        bigquery.SchemaField('Practice', 'STRING'),
        bigquery.SchemaField('Category', 'STRING'),
        bigquery.SchemaField('Subcategory', 'STRING'),
        bigquery.SchemaField('Spend_Percentage', 'FLOAT'),
    ],
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
)
job = client.load_table_from_dataframe(
    df_to_insert, table_id, job_config=job_config
)
job.result()  # Wait for the job to complete

client.close()
