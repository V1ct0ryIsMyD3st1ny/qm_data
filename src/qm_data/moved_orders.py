import qm_buildings.file_loader as fl
import pandas as pd
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class volume_config:
    """Configuration for the file 'Volumenauswertung' where
    info_row: row where the Zusteller-Info is located
    oder_row: row where the Auftragsnummer is located
    order_col: column where the first Auftrag is located
    agent_columns: list of column names to save in the report
    """
    info_row: int = 33
    order_row: int = 6
    order_col: int = 24
    
    agent_columns: list[str] = field(default_factory=lambda: ['Name', 'Vorname', 'Anrede'])
    

def read_volume(filepath: str) -> pd.DataFrame:
    """Read the volumenauswertung and only keep the first entry of each order

    Args:
        filepath (str): Filepath of the Volumenauswertung

    Returns:
        pd.Dataframe: Dataframe where all the duplicate orders are removed
    """
    options = {
        'sep': ';',
        'header': 0,
        'usecols': lambda col: 'Unnamed' in col or 'Sendungen Total' in col or col == 'Sendungen',
        'dtype': 'string',
        'encoding': 'windows-1252'
    }
    df = pd.read_csv(filepath, **options)
    return df
    

def cleanup_volume(volume: pd.DataFrame, changed_sectors: list[str], config: volume_config) -> pd.DataFrame:
    """Copy down the Auftragsnummer row to match the Zusteller-Info row.
    Only keep rows with ZGB that were changed and set the leading row as column labels.

    Args:
        volume (pd.Dataframe): Volumenauswertung
        changed_sectors (list[str]): list of ZGB that were changed
        config (volume_config): Instance of the configuration

    Returns:
        pd.Dataframe: Copy of volume with correct column labels and unncessairy rows
    """
    header_row = config.info_row
    first_order_row, first_order_column = config.order_row, config.order_col
    header_row -= 1
    first_order_row -= 1
    header = (
        volume.iloc[header_row, :first_order_column].to_list() +
        volume.iloc[first_order_row, first_order_column:].to_list()
    )
    volume = volume.iloc[header_row + 1:].copy()
    volume.columns = header
    volume = volume[volume['ZGB-PLZ'].isin(changed_sectors)]
    volume.reset_index(inplace=True, drop=True)
    volume.fillna('0', inplace=True)
    return volume


def add_sectors_to_agents(volume: pd.DataFrame, config: volume_config) -> list[dict[str, str]]:
    """Create a list of all agents with their corresponding info and sectors.

    Args:
        volume (pd.Dataframe): cleaned up Volumenauswertung
        config (volumen_config): Instance of configuration

    Returns:
        list[dict[str, str]]: Mapping of all agents with their info and sectors.
    """
    volume = volume.iloc[:, :config.order_col].copy()
    volume.set_index('Zusteller', inplace=True)
    agents = []
    for agent in set(volume.index.to_list()):
        info = {'Zusteller': agent}
        df = volume.loc[[agent]]
        first_row = df.iloc[0]
        for column in config.agent_columns:
            info[column] = first_row[column]
        sectorPLZ = df.loc[:, 'ZGB-PLZ'].to_list()
        depotNumbers = df.loc[:, 'Depot'].to_list()
        zgbNumbers = df.loc[:, 'ZGB'].to_list()
        sectorNumbers = [f'{depot}-{zgb}' for depot, zgb in zip(depotNumbers, zgbNumbers)]
        sectorNames = df.loc[:, 'ZGB-Name'].to_list()
        info['ZGB'] = list(zip(sectorPLZ, sectorNumbers, sectorNames))
        agents.append(info)
    return agents


def import_moved_orders(filepath: str, week: str) -> list[str]:
    """Read the Export from Intranet - Benutzerverwaltung - Extranet - Aufträge.
    Keep only orders with Routierungsdatum before week.


    Args:
        filepath (str): Path of the Auftraege-File
        week (str): calendary week in the form JJJJmm

    Returns:
        list[str]: List of all orders before week.
    """
    options = {
        'sep': ';',
        'header': 0,
        'dtype': 'string',
        'encoding': 'windows-1252'
    }
    df = pd.read_csv(filepath, **options)
    orders = []
    foundNumbers = set()
    df = df[df['Routierung_Zustellwoche'] < week]
    for i in df.index:
        number = df.loc[i, 'Auftragsnummer']
        if number not in foundNumbers:
            foundNumbers.add(number)
            name = df.loc[i, 'Auftragsname']
            orders.append([number, name])
    return orders


def import_changed_sectors(filepath: str) -> list[str]:
    """Read file containing all ZGB with changes.

    Args:
        filepath (str): Filepath of the file containing changed ZGB.

    Returns:
        list[str]: List of all ZGB in the file.
    """
    options = {
        'sep': ';',
        'header': 0,
        'dtype': 'string',
        'encoding': 'windows-1252'
    }
    changed_sectors = pd.read_csv(filepath, **options)
    sectors = changed_sectors.loc[:, 'ZGB-PLZ'].to_list()
    return sectors


def assign_orders_to_sectors(volume: pd.DataFrame, orders: list[str], config: volume_config) -> defaultdict[str, list[str]]:
    """Per ZGB select all orders that have at least one mailing.

    Args:
        volume (pd.Dataframe): Clean Volumenauswertung.
        orders (list[str]): List of orders
        config (volume_config): Instance of configuration.

    Returns:
        defaultdict[str, list[str]]: Dictionary of all sectors and orders with at least one mailing.
    """
    volume = volume.set_index('ZGB-PLZ')
    volume = volume.iloc[:, config.order_col - 1:]
    orders = [order for order in orders if order[0] in list(volume)]
    orderNumbers = [order[0] for order in orders]
    volume = volume[orderNumbers]
    volume = volume.map(lambda x: int(x.replace("'", "")))
    ordersPerSector = defaultdict(list)
    for sector in volume.index:
        for order in orders:
            if volume.loc[sector, order[0]] > 0:
                ordersPerSector[sector].append(order)
    return ordersPerSector


def assgin_orders_to_agents(agents: list[dict[str, str]], orders: dict[str, list[str]]) -> list[dict[str, str]]:
    """Collect all orders per agent and add them to their record.

    Args:
        agents (list[dict[str, str]]): List of records of agents.
        orders (dict[str, list[str]]): Dictionary of orders per sector.

    Returns:
        list[dict[str, str]]: List of all records per agent that have orders.
    """
    agentsTotal = []
    for agent in agents:
        agentTotal = agent
        for i, sector in enumerate(agent['ZGB']):
            agentTotal[f'Depot-ZGB_{i}'] = sector[1]
            agentTotal[f'ZGB-Name_{i}'] = sector[2]
            for j, order in enumerate(orders[sector[0]]):
                agentTotal[f'Auftragsnummer_{j}'] = order[0]
                agentTotal[f'Auftragsname_{j}'] = order[1]
                if 'Auftragsnummer_0' in agentTotal.keys():
                    agentsTotal.append(agentTotal)
    return agentsTotal
    
    
def create_header(agents: list[dict[str, str]], config: volume_config) -> list[str]:
    """Create a header containig:
    - agent_info
    - total of all sectors
    - total of all orders

    Args:
        agents (list[dict[str, str]]): List of records of agents.
        config (volumen_config): Instance of configuration.

    Returns:
        list[str]: List of the agent info and every sector/order in range(max)
    """
    max_sector = len({k for agent in agents for k in agent.keys() if 'Depot-ZGB' in k})
    max_order = len({k for agent in agents for k in agent.keys() if 'Auftragsnummer' in k})
    
    sectorNumbers = [f'Depot-ZGB_{i}' for i in range(max_sector)]
    sectorNames = [f'ZGB-Name_{i}' for i in range(max_sector)]
    sectorColumns =  [item for pair in zip(sectorNumbers, sectorNames) for item in pair]
    orderNumbers = [f'Auftragsnummer_{i}' for i in range(max_order)]
    orderNames = [f'Auftragsname_{i}' for i in range(max_order)]
    orderColumns = [item for pair in zip(orderNumbers, orderNames) for item in pair]
    header = (
        ['Zusteller'] +
        config.agent_columns +
        sectorColumns +
        orderColumns 
    )
    return header

    
def create_report() -> None:
    """User selects Volumenauwertung, Auftragsdatei, ZGB-Datei, Woche des Versionswechsel.
    Create Report of all Zusteller with their changed ZGB that have moved orders.
    """
    
    volumenauswertung = fl.load_file('Volumenauswertung auswählen')
    zgb_datei = fl.load_file('ZGB-Datei auswählen')
    auftragsdatei = fl.load_file('Auftragsdatei auswählen')
    week = input("Versionswechsel im Format JJJJWW eingeben:")
    week = str(week)

    config = volume_config()
    volume = read_volume(volumenauswertung)
    changed_sectors = import_changed_sectors(zgb_datei)
    volume = cleanup_volume(volume, changed_sectors, config)
    agents = add_sectors_to_agents(volume, config)
    moved_orders = import_moved_orders(auftragsdatei, week)
    orders = assign_orders_to_sectors(volume, moved_orders, config)
    agents = assgin_orders_to_agents(agents, orders)
    header = create_header(agents, config)
    df = pd.DataFrame(agents)
    df = df[header]
    saveas_path = fl.save_file('Export-Datei speichern')
    options = {
        'sep': ';',
        'index': False,
        'encoding': 'windows-1252'
    }
    df.to_csv(saveas_path, **options)
    

if __file__ == '__main__':
    create_report()