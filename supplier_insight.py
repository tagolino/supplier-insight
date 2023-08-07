import pandas as pd

from typing import List


def process_spend_dataset(csv_path, identifier_column_name, amount_column_name):
    print('Processing: {}'.format(csv_path))
    ignite_df = pd.read_csv(csv_path)
    ignite_df = ignite_df.sort_values(by=identifier_column_name)
    ignite_df['supplier_spend'] = pd.to_numeric(ignite_df[amount_column_name], errors='coerce')
    ignite_df['supplier_total_spend'] = ignite_df.groupby(identifier_column_name)['supplier_spend'].transform('sum')

    ignite_df = ignite_df.assign(all_supplier_total_spend=ignite_df['supplier_spend'].sum())
    ignite_df.drop_duplicates(
        subset=[identifier_column_name, 'supplier_total_spend', 'all_supplier_total_spend'],
        keep='first',
        inplace=True
    )
    # can do supplier nme normalize here and recompute if needed
    ignite_df['supplier_spend_percentage'] = round((ignite_df['supplier_total_spend'] / ignite_df['all_supplier_total_spend']) * 100, 0)
    ignite_df = ignite_df.sort_values(by='supplier_total_spend', ascending=False)
    ignite_df['cumulative_sum'] = ignite_df['supplier_spend_percentage'].cumsum()
    
    threshold_value = 90.0
    ignite_df = ignite_df[ignite_df['cumulative_sum'] <= threshold_value]  # get top records only by threshold value

    ctr = 0
    for i, row in ignite_df.iterrows():  # can remove, just use to check dataset before generating output file
        ctr += 1
        data = row.to_dict()
        print(
            i,
            data[identifier_column_name],
            data[amount_column_name],
            data['supplier_spend'],
            data['supplier_total_spend'],
            data['all_supplier_total_spend'],
            data['supplier_spend_percentage'],
            data['cumulative_sum']
        )
        if ctr == 10:
            break

    # ignite_output_columns = [identifier_column_name, 'supplier_total_spend', 'supplier_spend_percentage', 'cumulative_sum']

    # # generate different output files for now but can be put in a single file if needed
    # ignite_df.to_csv('output_{}'.format(csv_path), index=False, columns=ignite_output_columns)

def process_p2p_pr_dataset(csv_path, identifier_column_name: List, count_column_name):
    print('Processing: {}'.format(csv_path))
    df = pd.read_csv(csv_path, dtype={'Supplier_Id': str})
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
    
    df.to_csv('output_{}'.format(csv_path), index=False, columns=['Supplier', 'Supplier_Id', 'supplier_pr_count'])

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
