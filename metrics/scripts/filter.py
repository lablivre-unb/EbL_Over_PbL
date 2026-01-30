import pandas as pd
import os
from datetime import datetime

INPUT_PATH = 'metrics/data/bronze/prs.csv'
OUTPUT_FOLDER = 'metrics/data/silver'
OUTPUT_FILE = 'prs.csv'

# Melhores projetos baseado em nota
UNB_MDS_ORG = 'unb-mds'
UNB_MDS_PROJECTS = [
    '2025-2-Mural-UnB', 
    '2025-2-OncoMap', 
    'Projeto-P.I.T.E.R', 
    '2025-2-Synapse', 
    '2025-2-Squad-01'
]

OTHER_ORGS = [
    {"platform": "GitHub", "org": "GovHub-br"},
    {"platform": "GitHub", "org": "lablivre-unb"},
    {"platform": "GitLab", "org": "lappis-unb"}
]

# Faixa de tempo do semestre 2025.2
START_DATE = '2025-08-01 00:00:00'
END_DATE = '2025-12-22 23:59:59'

def process_data():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    if not os.path.exists(INPUT_PATH):
        print(f"Erro: Arquivo {INPUT_PATH} não encontrado.")
        return

    df = pd.read_csv(INPUT_PATH)

    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['merged_at'] = pd.to_datetime(df['merged_at'], errors='coerce')

    start = pd.to_datetime(START_DATE).tz_localize('UTC') if df['created_at'].dt.tz else pd.to_datetime(START_DATE)
    end = pd.to_datetime(END_DATE).tz_localize('UTC') if df['created_at'].dt.tz else pd.to_datetime(END_DATE)

    mask_unb_mds = (df['org'].str.lower() == UNB_MDS_ORG.lower()) & \
                   (df['repo'].isin(UNB_MDS_PROJECTS))

    org_masks = []
    for target in OTHER_ORGS:
        m = (df['platform'].str.lower() == target['platform'].lower()) & \
            (df['org'].str.lower() == target['org'].lower())
        org_masks.append(m)
    mask_others = pd.concat(org_masks, axis=1).any(axis=1)

    mask_time = (
        ((df['created_at'] >= start) & (df['created_at'] <= end)) |
        ((df['merged_at'] >= start) & (df['merged_at'] <= end))
    )

    filtered_df = df[(mask_unb_mds | mask_others) & mask_time]

    output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)
    filtered_df.to_csv(output_path, index=False)
    
    print("--- Processamento Concluído ---")
    print(f"Total na Bronze: {len(df)}")
    print(f"Total na Silver: {len(filtered_df)}")
    print(f"Período aplicado: {START_DATE} até {END_DATE}")
    print(f"Arquivo salvo em: {output_path}")

if __name__ == "__main__":
    process_data()