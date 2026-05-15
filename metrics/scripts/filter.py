import pandas as pd
import os
from datetime import datetime

INPUT_PATH = "metrics/data/bronze/prs.csv"
OUTPUT_FOLDER = "metrics/data/silver"
OUTPUT_FILE = "prs.csv"

# Projetos UNB-MDS
UNB_MDS_ORG = "unb-mds"
UNB_MDS_PROJECTS = [
    "2025-2-Mural-UnB",
    "Sonorus-2025.1",
    "2024-2-AcheiUnB",
    "2024-1-forUnB",
]

# Projetos MDSREQ-FGA-UNB
MDSREQ_FGA_UNB_ORG = "mdsreq-fga-unb"
MDSREQ_FGA_UNB_PROJECTS = [
    "REQ-2025.2-T02-RxHospitalar",
    "2025.1-T01-VidracariaModelo",
    "2024.2-T03-CafeDoSitio",
    "2024.1-ObjeX",
]

# Projetos FGA-EPS-MDS (novo!)
FGA_EPS_MDS_ORG = "fga-eps-mds"
FGA_EPS_MDS_PROJECTS = [
    "2025.2-Valhalla",
    "2025.2-Valhalla-Docs",
    "2025.1-VaiPelaSombra-docs",
    "2025.1-VaiPelaSombra-FrontEnd",
    "2025.1-VaiPelaSombra-BackEnd,",
    "2025.1-VaiPelaSombra-API",
    "2024-2-GEROcuidado-Docs",
    "2024-2-GEROcuidado-APIForum",
    "2024-2-GEROcuidado-Front",
    "2024-2-GEROcuidado-APIUsuario",
    "2024-2-GEROcuidado-APISaude",
    "2024-1-GEROcuidado-Front",
    "2024-1-GEROcuidado-Doc",
    "2024-1-GEROcuidado-APISaude",
    "2024-1-GEROcuidado-APIUsuario",
    "2024-1-GEROcuidado-APIForum",
]

# Outros projetos (EbL e benchmarks de mercado)
OTHER_ORGS = [
    {"platform": "GitLab", "org": "lappis-unb/decidimbr"},
    {"platform": "GitHub", "org": "decidim"},
    {"platform": "GitHub", "org": "microsoft"},
    {"platform": "GitHub", "org": "flutter"},
    {"platform": "GitHub", "org": "facebook"},
    {"platform": "GitHub", "org": "kubernetes"},
    {"platform": "GitHub", "org": "tensorflow"},
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

    # Máscaras para projetos acadêmicos
    mask_unb_mds = (df["org"].str.lower() == UNB_MDS_ORG.lower()) & (
        df["repo"].isin(UNB_MDS_PROJECTS)
    )

    mask_mdsreq_fga_unb = (df["org"].str.lower() == MDSREQ_FGA_UNB_ORG.lower()) & (
        df["repo"].isin(MDSREQ_FGA_UNB_PROJECTS)
    )

    mask_fga_eps_mds = (df["org"].str.lower() == FGA_EPS_MDS_ORG.lower()) & (
        df["repo"].isin(FGA_EPS_MDS_PROJECTS)
    )

    # Máscara para outros projetos (EbL e benchmarks)
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
        (mask_unb_mds | mask_mdsreq_fga_unb | mask_fga_eps_mds | mask_others)
        & mask_any_semester
    ]

    output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)
    filtered_df.to_csv(output_path, index=False)

    print("Processamento Concluído (v2)")
    print(f"- Total na Bronze: {len(df)}")
    print(f"- Total na Silver: {len(filtered_df)}")
    print("\nOrganizações incluídas:")
    print(f"  • {UNB_MDS_ORG}: {len(UNB_MDS_PROJECTS)} repos")
    print(f"  • {MDSREQ_FGA_UNB_ORG}: {len(MDSREQ_FGA_UNB_PROJECTS)} repos")
    print(f"  • {FGA_EPS_MDS_ORG}: {len(FGA_EPS_MDS_PROJECTS)} repos")
    print(f"  • Outros (EbL + Mercado): {len(OTHER_ORGS)} orgs")
    print("\nSemestres aplicados:")
    for semester in SEMESTERS:
        print(f"  • {semester['name']}: {semester['start']} até {semester['end']}")
    print(f"\nArquivo salvo em: {output_path}")


if __name__ == "__main__":
    process_data()
