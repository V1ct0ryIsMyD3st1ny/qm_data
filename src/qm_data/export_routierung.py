import re
import glob
import win32com.client
import pythoncom
import pandas as pd
from bs4 import BeautifulSoup
from tkinter import filedialog as fd, messagebox as mb
from io import StringIO
from qm_buildings import file_loader as fl


def extract_zustelldatum(msg):
    html_body = msg.HTMLBody
    soup = BeautifulSoup(html_body, "html.parser")
    tables = soup.find_all('table')

    if tables:
        df_list = pd.read_html(StringIO(str(tables)))
        for id, df in enumerate(df_list):
            if id == 1:
                for i in range(10):
                    if str(df.iloc[i][1]).isnumeric():
                        auftrag_nr = df.iloc[i][1]
                        auftrag_name = df.iloc[i+2][1]
                        avisierung = df.iloc[i+4][1]
                        return auftrag_name, auftrag_nr, avisierung
                    
    raise ValueError("Keine Auftragsnummer gefunden.")


def select_directory(title: str) -> str:
    #Loop until file is selected.
    while True:
        filepath = fd.askdirectory(title=title)
        if filepath:
            return filepath
        #If no file selected ask for retry.
        else:
            retry = mb.askretrycancel(
                title='Kein Ordner ausgewählt',
                message="Willst du es nochmal versuchen"
            )
            if not retry:
                raise KeyboardInterrupt("Nutzer hat den Import abgebrochen.")


def read_mails(mails_path: str, week: int) -> pd.DataFrame:  
    auftraege = []
    namen = []
    avisierungen = []
    dates = []
    
    xl=win32com.client.Dispatch("Excel.Application",pythoncom.CoInitialize())
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    
    for file in glob.glob(fr"{mails_path}" + "/*.msg"):
        msg = outlook.OpenSharedItem(file)
        if 'Quickroutierung durchgeführt' in msg.Subject:
            date_received = msg.ReceivedTime
            auftrag_name, auftrag_nr, avisierung = extract_zustelldatum(msg)
            match = re.search(r'\b(\d{2})/(\d{4})\b', avisierung)
            if match is None:
                raise ValueError("Keine Avisierung gefunden.")
            else:
                woche, jahr = match.group(1), match.group(2)
                if jahr <= week[:4] and woche < week[:-2]:
                    pass
                auftraege.append(auftrag_nr)
                namen.append(auftrag_name)
                avisierungen.append(avisierung)
                
                dt = date_received.strftime("%Y-%m-%d")
                dates.append(dt)

    df = pd.DataFrame({'Auftragsnummer': auftraege, 'Auftragsname': namen, 'Avisierung': avisierungen, 'Erhalten': dates})
    return df


def create_report():
    mails_path = select_directory("Ordner mit Mails auswählen")
    week = input('Gib die Kalenderwoche des Versionswechsels im Format YYYYMM ein.')
    df = read_mails(mails_path, week)
    
    options = {
        "sep": ";",
        "encoding": "windows-1252",
        "index": False
    }
    df.to_csv(mails_path, **options)
    print(f'Datei gespeichert unter {mails_path}')