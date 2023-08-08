import pandas as pd

from typing import List
from google.cloud import bigquery


def process_s2c_dataset():
    # processing data from

    client = bigquery.Client()

    supplier_summary_query = """
    SELECT
    *
    FROM
    `lisle-pbps-analytics-platform.lisle_pbps_s2c.PROJECT_SUMMARY_REFINED`  -- 34113
    """
    supplier_summary_df = client.query(supplier_summary_query).to_dataframe()
    print(len(supplier_summary_df))
    print(len(supplier_summary_df.columns))

    supplier_detail_query = """
    SELECT
    *
    FROM
    `lisle-pbps-analytics-platform.lisle_pbps_ia.RPT_PROJECT_SUPPLIER_DETAIL` -- 36748
    """
    supplier_detail_df = client.query(supplier_detail_query).to_dataframe()
    print(len(supplier_detail_df))
    print(len(supplier_detail_df.columns))

    supplier_contact_query = """
    SELECT
    *
    FROM
    `lisle-pbps-analytics-platform.lisle_pbps_ia.RPT_PROJECT_SUPPLIER_CONTACT`  -- 93
    """
    supplier_contact_df = client.query(supplier_contact_query).to_dataframe()
    print(len(supplier_contact_df))
    print(len(supplier_contact_df.columns))

    supplier_summary_df['PROJECT_ID'] = supplier_summary_df['PROJECT_ID'].astype(str)

    supplier_df = supplier_summary_df.merge(supplier_detail_df, on='PROJECT_ID', how='left')
    supplier_df = supplier_df.merge(supplier_contact_df, on='PROJECT_ID', how='left')

    print(len(supplier_df))
    print(len(supplier_df.columns))

    supplier_name_count = supplier_df.groupby('Supplier_Name').size().reset_index(name='Count')
    print('NAME COUNT: {}'.format(len(supplier_name_count.columns)))


def process_spend_dataset(csv_path, identifier_column_name, amount_column_name):
    # processing data from Spend table
    identifier_column_name = 'NORMALIZED_SUPPLIER_NAME'
    amount_column_name = 'TOTAL_SPEND'

    client = bigquery.Client()
    query="""
    SELECT
    CUSTOMER_PARTY_ID,
    NORMALIZED_SUPPLIER_NAME,
    TOTAL_SPEND,
    LOCATION_REGION,
    LOCATION_COUNTRY,
    PRACTICE_L1_2022,
    CATEGORY_L2_2022,
    SUBCATEGORY_L3_2022
    FROM
    `lisle-pbps-analytics-platform.lisle_pbps_crossclients_discovery.Spend_Analytics_DataAssessment_Stg4_UC1UC3_v2`
    WHERE
    FLAG_AID_STATUS = 'Pass' AND
    FLAG_PRIORITY = 'Priority' AND
    FLAG_REMOVED_CLIENTS = 'Retained' AND
    FLAG_SPEND_LEGIT='Legitimate'
    """
    spend_df = client.query(query).to_dataframe()
    print(len(spend_df))

    df = spend_df.sort_values(by=identifier_column_name)
    df['supplier_spend'] = pd.to_numeric(df[amount_column_name], errors='coerce')
    df['supplier_total_spend'] = df.groupby(identifier_column_name)['supplier_spend'].transform('sum')

    df = df.assign(all_supplier_total_spend=df['supplier_spend'].sum())
    df.drop_duplicates(
        subset=[identifier_column_name, 'supplier_total_spend', 'all_supplier_total_spend'],
        keep='first',
        inplace=True
    )
    # can do supplier nme normalize here and recompute if needed
    df['supplier_spend_percentage'] = round((df['supplier_total_spend'] / df['all_supplier_total_spend']) * 100, 0)
    df = df.sort_values(by='supplier_total_spend', ascending=False)
    df['cumulative_sum'] = df['supplier_spend_percentage'].cumsum()

    threshold_value = 90.0
    spend_percent_condition = df['supplier_spend_percentage'] > 0.0
    cumulative_sum_condition = df['cumulative_sum'] <= threshold_value
    df = df[spend_percent_condition & cumulative_sum_condition]  # get top records only by threshold value
    print(len(df))
    # for i, row in df.iterrows():  # can remove, just use to check dataset before generating output file
    #     data = row.to_dict()
    #     print(
    #         data[identifier_column_name],
    #         data['supplier_total_spend'],
    #         data['supplier_spend_percentage'],
    #         data['cumulative_sum']
    #     )
    #     # ctr = 0
    #     # for i, _row  in spend_df[spend_df[identifier_column_name] == data[identifier_column_name]].iterrows():
    #     #     ctr += 1
    #     #     spend_data = _row.to_dict()
    #     #     print(
    #     #         spend_data[identifier_column_name],
    #     #         spend_data[amount_column_name]
    #     #     )
    #     #     if ctr == 10:
    #     #         break

    df_to_insert = spend_df[spend_df[identifier_column_name].isin(df[identifier_column_name])]
    column_mapping = {
        'CUSTOMER_PARTY_ID': 'Client_ID',
        'NORMALIZED_SUPPLIER_NAME': 'Supplier_Name_Normalized',
        'TOTAL_SPEND': 'Spend',
        'LOCATION_REGION': 'Region',
        'LOCATION_COUNTRY': 'Country',
        'PRACTICE_L1_2022': 'Practice',
        'CATEGORY_L2_2022': 'Category',
        'SUBCATEGORY_L3_2022': 'Subcategory',
    }
    df_to_insert = df_to_insert.loc[:, list(column_mapping.keys())].rename(columns=column_mapping)
    df_to_insert = df_to_insert.sort_values(by=column_mapping[identifier_column_name], ascending=False)
    print(len(df_to_insert))

    # table_id = 'lisle-pbps-analytics-platform.lisle_pbps_supplier_factset_discovery.Spend_Supplier_Factset_Table'
    # job_config = bigquery.LoadJobConfig(
    #     schema=[
    #         bigquery.SchemaField('Client_ID', 'STRING'),
    #         bigquery.SchemaField('Supplier_Name_Original', 'STRING'),
    #         bigquery.SchemaField('Supplier_Name_Normalized', 'STRING'),
    #         bigquery.SchemaField('Spend', 'Float'),
    #         bigquery.SchemaField('Region', 'STRING'),
    #         bigquery.SchemaField('Country', 'STRING'),
    #         bigquery.SchemaField('Practice', 'STRING'),
    #         bigquery.SchemaField('Category', 'STRING'),
    #         bigquery.SchemaField('Subcategory', 'STRING'),
    #     ],
    #     write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    # )
    # job = client.load_table_from_dataframe(
    #     df_to_insert, table_id, job_config=job_config
    # )
    # job.result()  # Wait for the job to complete
    # print(f"Loaded {job.output_rows}")

def process_p2p_pr_dataset(csv_path, identifier_column_name: List, count_column_name):
    print('Processing: {}'.format(csv_path))
    # processing data from P2P PR table

    client = bigquery.Client()
    query="""
    SELECT
        *
    FROM 
        `lisle-pbps-analytics-platform.lisle_pbps_prm.PRM_HEADER`
    LIMIT 100
    """
    df = client.query(query).to_dataframe()

    df = df.dropna(subset=['Supplier'])
    df['supplier_pr_count'] = df.groupby('Supplier')[count_column_name].transform('count')

    df = df.drop_duplicates(subset=['Supplier', 'Supplier_Id', 'supplier_pr_count'])
    df = df.sort_values(by='supplier_pr_count', ascending=False)

    for i, row in df.iterrows():  # can remove, just use to check dataset before generating output file
        data = row.to_dict()
        print(
            i,
            data['Supplier'],
            data['Supplier_Id'],
            data['supplier_pr_count']
        )

if __name__ == '__main__':
    for spend_csv_path, identifier_column, amount_column in [
            # ('SPEND_Ignite All Final Data.csv', 'SUPPLIER_NAME', 'TRANSACTION_VALUE'),
            # ('SPEND_Ignite Legacy Client Final Data.csv', 'SUPPLIER_ACCENTURE', 'AMT_LOCAL')
            ('Spend_Analytics_DataMaturity_Stg3_TaxonomyMapping_UC2_5000.csv', 'NORMALIZED_SUPPLIER_NAME', 'TOTAL_SPEND')
        ]:
        process_spend_dataset(spend_csv_path, identifier_column, amount_column)
    # for p2p_pr_csv_path, identifier_columns, count_column in [
    #         ('P2P_PR_PRM Data.csv', ['Supplier', 'Supplier_Id'], 'PR_Number')
    #     ]:
    #     process_p2p_pr_dataset(p2p_pr_csv_path, identifier_columns, count_column)
