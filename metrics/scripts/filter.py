import pandas as pd
import os
from datetime import datetime

INPUT_PATH = "metrics/data/bronze/prs.csv"
OUTPUT_FOLDER = "metrics/data/silver"
OUTPUT_FILE = "prs.csv"

# Melhores projetos baseado em nota
UNB_MDS_ORG = "unb-mds"
UNB_MDS_PROJECTS = [
    # 2025.2
    "2025-2-Mural-UnB",
    #"2025-2-OncoMap",
    #"Projeto-P.I.T.E.R",
    #"2025-2-Synapse",
    #"2025-2-Squad-01",
    # 2025.1
    #"2025-1-NoFluxoUNB",
    "Sonorus-2025.1",
    #"DFemObras-2025.1",
    #"2025-1-GovInsights",
    #"2025-1-RelatAI",
    # 2024.2
    "2024-2-AcheiUnB",
    #"2024-2-Squad06",
    #"2024-2-ChamaControl",
    #"Gastos-DF-2024-02",
    #"2024-2-SuaFinanca",
    # 2024.1
    "2024-1-forUnB",
    #"2024-1-MinasDeCultura",
    #"2024-1-Squad02-CulturaTransparente",
    #"2024-1-Squad08",
    #"2024-1-Squad-10",
]

MDSREQ_FGA_UNB_ORG = "mdsreq-fga-unb"
MDSREQ_FGA_UNB_PROJECTS = [
    # 2025.2
    #"REQ-2025.2-T01-DataBuilders",
    #"REQ-2025.2-T01-PPBM",
    #"REQ-2025.2-T02-ProJuris",
    "REQ-2025.2-T02-RxHospitalar",
    #"REQ-2025.2-T01-ST-APP",
    # 2025.1
    #"2025.1-T01-AdvogaAI",
    #"2025.1-T01-SeuPontoDigital",
    "2025.1-T01-VidracariaModelo",
    #"2025.1-T01-CORIGGE",
    #"2025.1-T02-CanadaIntercambio",
    # 2024.2
    "2024.2-T03-CafeDoSitio",
    #"2024.2-T01-IdeaSpace",
    #"2024.2-T03-CerradoTech",
    #"2024.2-T01-FamintosBurguer",
    #"2024.2-T01-CD-MOJ",
    # 2024.1
    #"2024.1-Echoeasy",
    #"2024.1-RISO-",
    #"2024.1-Est-dio-de-Beleza-Keuany",
    #"2024.1-Crystaleum-2",
    "2024.1-ObjeX",
]

OTHER_ORGS = [
    {"platform": "GitHub", "org": "GovHub-br"},
    {"platform": "GitHub", "org": "lablivre-unb"},
    {"platform": "GitLab", "org": "lappis-unb/decidimbr"},
    {"platform": "GitHub", "org": "decidim"},
    {"platform": "GitHub", "org": "microsoft"},
]

# Faixas de tempo dos semestres
SEMESTERS = [
    {"name": "2024.1", "start": "2024-03-18 00:00:00", "end": "2024-09-21 23:59:59"},
    {"name": "2024.2", "start": "2024-10-14 00:00:00", "end": "2025-02-22 23:59:59"},
    {"name": "2025.1", "start": "2025-03-24 00:00:00", "end": "2025-07-26 23:59:59"},
    {"name": "2025.2", "start": "2025-08-01 00:00:00", "end": "2025-12-22 23:59:59"},
]


def process_data():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    if not os.path.exists(INPUT_PATH):
        print(f"Erro: Arquivo {INPUT_PATH} não encontrado.")
        return

    df = pd.read_csv(INPUT_PATH)

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["merged_at"] = pd.to_datetime(df["merged_at"], errors="coerce")

    mask_unb_mds = (df["org"].str.lower() == UNB_MDS_ORG.lower()) & (
        df["repo"].isin(UNB_MDS_PROJECTS)
    )

    mask_mdsreq_fga_unb = (df["org"].str.lower() == MDSREQ_FGA_UNB_ORG.lower()) & (
        df["repo"].isin(MDSREQ_FGA_UNB_PROJECTS)
    )

    org_masks = []
    for target in OTHER_ORGS:
        m = (df["platform"].str.lower() == target["platform"].lower()) & (
            df["org"].str.lower() == target["org"].lower()
        )
        org_masks.append(m)
    mask_others = pd.concat(org_masks, axis=1).any(axis=1)

    # Aplicar filtro de tempo para todos os semestres
    semester_masks = []
    for semester in SEMESTERS:
        start = (
            pd.to_datetime(semester["start"]).tz_localize("UTC")
            if df["created_at"].dt.tz
            else pd.to_datetime(semester["start"])
        )
        end = (
            pd.to_datetime(semester["end"]).tz_localize("UTC")
            if df["created_at"].dt.tz
            else pd.to_datetime(semester["end"])
        )

        mask_time = ((df["created_at"] >= start) & (df["created_at"] <= end)) | (
            (df["merged_at"] >= start) & (df["merged_at"] <= end)
        )
        semester_masks.append(mask_time)

    # Combinar todas as máscaras de semestres (OR)
    mask_any_semester = pd.concat(semester_masks, axis=1).any(axis=1)

    filtered_df = df[
        (mask_unb_mds | mask_mdsreq_fga_unb | mask_others) & mask_any_semester
    ]

    output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)
    filtered_df.to_csv(output_path, index=False)

    print("Processamento Concluído")
    print(f"- Total na Bronze: {len(df)}")
    print(f"- Total na Silver: {len(filtered_df)}")
    print("Semestres aplicados:")
    for semester in SEMESTERS:
        print(f"- {semester['name']}: {semester['start']} até {semester['end']}")
    print(f"Arquivo salvo em: {output_path}")


if __name__ == "__main__":
    process_data()
