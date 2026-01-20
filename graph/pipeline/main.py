import json
import config
from services import github_service, gitlab_service


def save_json(data, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved in {filename}.\n")


def main():
    print("Extracting contribution data...\n")

    # 1. Executa GitHub (se configurado)
    if config.GITHUB_TOKEN and config.GITHUB_ORG:
        try:
            print("\n--- GITHUB ---")
            gh_data = github_service.process_organization(config.GITHUB_ORG)
            save_json(gh_data, f"github_{config.GITHUB_ORG}.json")
        except Exception as e:
            print(f"Error GitHub: {e}")

    # 2. Executa GitLab (se configurado)
    if config.GITLAB_TOKEN and config.GITLAB_ORG:
        try:
            print("\n--- GITLAB ---")
            gl_data = gitlab_service.process_gitlab_group(config.GITLAB_ORG)
            save_json(gl_data, f"gitlab_{config.GITLAB_ORG.replace('/', '_')}.json")
        except Exception as e:
            print(f"Error GitLab: {e}")


if __name__ == "__main__":
    main()
