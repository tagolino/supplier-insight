import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta

from google.cloud import bigquery

client = bigquery.Client()

p2p_po_query = ""  # Query here
p2p_po_df = client.query(p2p_po_query).to_dataframe()

supplier_name_global_table_query =  ""  # Query here
supplier_name_global_table_df = client.query(supplier_name_global_table_query).to_dataframe()

sql_query =  ""  # Query here

rt_rate_df = client.query(sql_query).to_dataframe()

rt_rate_df['eff_at'] = pd.to_datetime(rt_rate_df['EFFDT'])
rt_rate_df['eff_year'] = rt_rate_df['eff_at'].dt.year
rt_rate_df['eff_month'] = rt_rate_df['eff_at'].dt.month

rt_rate_df = rt_rate_df.sort_values("eff_at", ascending=False)

def convert_po_value(row, rt_df):
    if not pd.isna(row["PO_CREATED_DATE"]):
        created_at = pd.to_datetime(row["PO_CREATED_DATE"])
        created_at = (created_at + relativedelta(months=1)).replace(day=1) - timedelta(days=1)
        curr_rt_df = rt_df[(rt_df["FROM_CUR"] == row["LINE_CURRENCY"]) & (rt_df['eff_at'] <= created_at)]
    else:
        curr_rt_df = rt_df[(rt_df["FROM_CUR"] == row["LINE_CURRENCY"])]
    if curr_rt_df.empty:
        row["effective_at"] = None
        row["rate_multiplier"] = 0.0
        row["Value_PO_converted"] = 0.0
    else:
        curr_rt_df = curr_rt_df.sort_values("eff_at", ascending=False)
        rt_rate = curr_rt_df.iloc[0].to_dict()
        row["effective_at"] = rt_rate["eff_at"]
        row["rate_multiplier"] = rt_rate["RATE_MULT"]
        row["Value_PO_converted"] = row["Value_PO"] * row["rate_multiplier"]
    
    return row
    
identifier_column_name = 'Supplier_Name_Original'
    
po_counts_df = p2p_po_df.groupby(identifier_column_name).size().reset_index(name='total_po')
grand_total_po = po_counts_df['total_po'].sum()
po_counts_df['po_grand_total'] = grand_total_po
po_counts_df = po_counts_df.sort_values(by='total_po', ascending=False)
po_counts_df['total_po_percentage'] = round((po_counts_df['total_po'] / po_counts_df['po_grand_total']) * 100, 4)
po_counts_df['cumulative_sum'] = po_counts_df['total_po_percentage'].cumsum()

threshold_value = 90.0
total_percent_condition = po_counts_df['total_po_percentage'] > 0
cumulative_sum_condition = po_counts_df['cumulative_sum'] <= threshold_value
po_counts_df = po_counts_df[total_percent_condition & cumulative_sum_condition]  # get top records only by threshold value

df_to_insert = p2p_po_df[p2p_po_df[identifier_column_name].isin(po_counts_df[identifier_column_name])]
df_to_insert = df_to_insert.merge(po_counts_df[[identifier_column_name, 'total_po_percentage']], on=identifier_column_name, how='left')
df_to_insert.rename(columns={'total_po_percentage': 'PO_Count_Percentage'}, inplace=True)
df_to_insert = df_to_insert.sort_values(by='PO_Count_Percentage', ascending=False)
df_to_insert = pd.merge(df_to_insert, supplier_name_global_table_df, left_on='Supplier_Name_Original', right_on='ORG_NAME_VARIATION', how='left')
df_to_insert['Supplier_Name_Normalized'] = df_to_insert['ORG_NAME'].fillna(df_to_insert['Supplier_Name_Original'])
df_to_insert = df_to_insert.drop(columns=['ORG_NAME', 'ORG_NAME_VARIATION'])

not_usd_data_df = df_to_insert[(df_to_insert["LINE_CURRENCY"].notna()) & (df_to_insert["LINE_CURRENCY"] != "") & (df_to_insert["LINE_CURRENCY"] != "USD") & (df_to_insert["Value_PO"] > 0)]
not_usd_data_df = not_usd_data_df.apply(lambda row: convert_po_value(row, rt_rate_df), axis=1)
df_to_insert = pd.concat([df_to_insert, not_usd_data_df[["effective_at", "rate_multiplier", "Value_PO_converted"]]], axis=1)

df_to_insert["Value_PO"] = df_to_insert["Value_PO"].astype(float)
df_to_insert["Value_PO_converted"] = df_to_insert["Value_PO_converted"].astype(float)
df_to_insert["rate_multiplier"] = df_to_insert["rate_multiplier"].astype(float)

table_id = ''
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField('ClientID', 'INTEGER'),
        bigquery.SchemaField('PO_Number', 'STRING'),
        bigquery.SchemaField('Supplier_Name_Original', 'STRING'),
        bigquery.SchemaField('Supplier_Name_Normalized', 'STRING'),
        bigquery.SchemaField('Region', 'STRING'),
        bigquery.SchemaField('Country', 'STRING'),
        bigquery.SchemaField('PO_CREATED_DATE', 'DATETIME'),
        bigquery.SchemaField('LINE_CURRENCY', 'STRING'),
        bigquery.SchemaField('Value_PO', 'FLOAT64'),
        bigquery.SchemaField('Value_PO_converted', 'FLOAT64'),
        bigquery.SchemaField('rate_multiplier', 'FLOAT64'),
        bigquery.SchemaField('effective_at', 'DATETIME'),
        bigquery.SchemaField('PO_Date', 'DATETIME'),
        bigquery.SchemaField('PO_Count_Percentage', 'FLOAT'),
    ],
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
)
job = client.load_table_from_dataframe(
    df_to_insert, table_id, job_config=job_config
)
job.result()  # Wait for the job to complete

client.close()
