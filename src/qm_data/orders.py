import pandas as pd
from collections import defaultdict
from qm_buildings import file_loader as fl


def orders_before_week(df: pd.DataFrame, week: int, order_column: str, order_name_column:str, date_column: str) -> pd.DataFrame:
    orders = {}
    df = df.copy()
    header = df.iloc[0]
    df = df.iloc[1:]
    df.columns = header
    df = df[df[date_column]<week]
    for order in df[order_column].to_list():
        order_info = df[df[order_column] == order]
        order_info = order_info.iloc[0]
        order_name = order_info[order_name_column]
        if "UA" not in order_name:
            orders[order] = order_name
    return orders
            

def copy_row(df, row_copy_from, row_copy_to, start_column):
    match_copy_from = df.iloc[:, 0] == row_copy_from
    match_copy_to = df.iloc[:, 0] == row_copy_to
    match_start = df.iloc[0, :] == start_column
    
    index_copy_from = df.index[match_copy_from][0]
    index_copy_to = df.index[match_copy_to][0]
    column_start = df.columns[match_start][0]
    
    df.loc[index_copy_to, column_start:] = df.loc[index_copy_from, column_start:]
    df = df.loc[index_copy_to:, :]
    return df


def replace_index_header(df):
    header = df.iloc[0, 1:].to_list()
    agents = df.iloc[1:, 0].to_list()
    df = df.iloc[1:, 1:]
    df.columns = header
    df.index = agents
    return df


def orders_per_sector(df, accumulate_label, orders):
    df = df.copy()
    orders = [order for order in orders if order in list(df)]
    sectors = df[accumulate_label].to_list()
    df = df[orders]
    df.index = sectors
    return df


def convert_to_int(df):
    df.fillna(0, inplace=True)
    remove_thousand = lambda x: str(x).replace("'", "")
    to_int = lambda x: int(x)
    df = df.map(remove_thousand)
    df = df.map(to_int)
    return df


def rows_with_values(df):
    df = convert_to_int(df)
    dict = defaultdict(list)
    for row in df.index:
        for col in list(df):
            if int(df.loc[row, col]) > 0:
                dict[row].append(col)
    return dict


def filter_by_column(df, row_label, filter_values):
    df = df.copy()
    matches = df.iloc[:, 0] == row_label
    first_index = df.index[matches][0]
    filter = (df.iloc[first_index].isna()) | (df.iloc[first_index].isin(filter_values+[row_label]))
    df = df.loc[:, filter]
    return df


def accumulate_rows(df, index_label, accumulate_label, changed_sectors):
    accumulate = {}
    for agent in set(df[index_label].to_list()):
        df_agent = df[df[index_label] == agent]
        agent_sectors = df_agent[accumulate_label].to_list()
        agent_changed_sectors = [sector for sector in agent_sectors if sector in changed_sectors]
        accumulate[agent] = agent_changed_sectors
        
    return accumulate
    
    
def orders_per_agent(sectors_with_orders, agents_with_sectors):
    orders = {}
    for agent in agents_with_sectors.keys():
        agent_orders = set()
        for sector in agents_with_sectors[agent]:
            for order in sectors_with_orders[sector]:
                agent_orders.add(order)
        if agent_orders:
            orders[agent] = list(agent_orders)
    return orders


def df_to_record(df, label, value):
    matches = df[df[label] == value]
    print(matches)
    row = matches.iloc[0]
    record = row.to_dict()
    return record


def create_export(agents_info, agent_label, excluded_agents, agents_with_sectors, sector_info, sectors_with_orders, orders):
    agents = agents_with_sectors.keys()
    sector_info_labels = list(sector_info)
    records = []
    max_sector = 0
    max_order = 0
    for agent in agents:
        row = {}
        if agent in excluded_agents:
            continue
        agent_info = agents_info[agents_info[agent_label] == agent]
        agent_info = agent_info.iloc[0].to_dict()
        row.update(agent_info)
        agent_sectors = set()
        agent_orders = set()
        for sector in agents_with_sectors[agent]:
            if sector in sectors_with_orders.keys():
                agent_sectors.add(sector)
                for order in sectors_with_orders[sector]:
                    agent_orders.add(order)
        for i, sector in enumerate(agent_sectors):
            row[f"ZGB_PLZ_{i}"] = sector
            for column_label in sector_info_labels:
                row[f"{column_label}_{i}"] = sector_info.loc[sector][column_label]
        for i, order in enumerate(agent_orders):
            row[f"Auftrag_{i}"] = order
            row[f"Auftragsname_{i}"] = orders[order]
        records.append(row)
        max_sector = max(max_sector, len(agent_sectors))
        max_order = max(max_order, len(agent_orders))
    info_list = list(agent_info.keys())
    
    sector_list = [f"{column_label}_{i}" for i in range(max_sector) for column_label in sector_info_labels]
    order_nr_list = [f"Auftrag_{i}" for i in range(max_order)]
    order_name_list = [f"Auftragsname_{i}" for i in range(max_order)]
    order_list = [item for pair in zip(order_nr_list, order_name_list) for item in pair]
    header = info_list + sector_list + order_list
    export = pd.DataFrame.from_records(records)
    export = export[header]
    return export
    

def agents_with_orders():
    
    df = pd.DataFrame({'col1': [1,2,3], 'col2': [4,5,6], 'col3': [7,8,9], 'col4': [10, 11,12], 'col5': [13, 14, 15]})
    file = fl.load_file("Volumenauswertung auswählen.")
    df = pd.read_csv(file, sep=";", encoding='windows-1252', header=None, low_memory=False)

    order_file = fl.load_file("Auftragsdatei auswählen.")
    df_orders = pd.read_csv(order_file, sep=";", encoding='windows-1252', header=None, low_memory=False)
    
    week = input("Versionswechsel im Format JJJJWW eingeben:")
    orders = orders_before_week(df_orders, week, 'Nummer', 'Name', 'Routierung_Zustellwoche')
    orders_list = list(orders.keys())
    df = filter_by_column(df, 'Sendungen', ['Sendungen Total'])
    df = filter_by_column(df, 'Auftragsnummer', orders_list)
    df = copy_row(df, 'Auftragsnummer', 'Depot', 'Sendungen Total')
    header = df.iloc[0, :].to_list()
    df = df.iloc[1:, :]
    df.columns = header
    df = df[~df["Depot"].isin(['60','61', '62', '63', '64'])]

    df_order = orders_per_sector(df, 'ZGB-PLZ', orders_list)
    sectors_with_orders = rows_with_values(df_order)

    df_agents = df[['Zusteller', 'ZGB-PLZ']]
    changed_sectors_file = fl.load_file("Angepasste ZGB auswählen.")
    changed_sectors_df = pd.read_csv(changed_sectors_file, sep=";", encoding='windows-1252', header=0, low_memory=False)
    changed_sectors_list = changed_sectors_df['ZGB-PLZ'].to_list()
    agents_with_sectors = accumulate_rows(df_agents, 'Zusteller', 'ZGB-PLZ', changed_sectors_list)

    agents_info = ['Zusteller', 'Anrede', 'Vorname', 'Name']
    df_agents_info = df[agents_info]

    sector_info = ['ZGB-PLZ', 'Depot', 'ZGB', 'ZGB-Name']
    df_sector_info = df[sector_info].copy()
    df_sector_info['Depot-ZGB'] = df_sector_info['Depot'] + "-" + df_sector_info['ZGB']
    sectors = df_sector_info.loc[:, 'ZGB-PLZ']
    df_sector_info = df_sector_info[['Depot-ZGB', 'ZGB-Name']]
    df_sector_info.index = sectors

    export = create_export(df_agents_info, 'Zusteller', ['0', '556873'], agents_with_sectors, df_sector_info, sectors_with_orders, orders)
    save_path = fl.save_file("Export speichern.")
    export.to_csv(save_path, sep=";", encoding='windows-1252', index=False)