import json
import os
import itertools

# Caminho do arquivo de entrada (gerado pelo filter_users.py)
INPUT_FILE = '../pipeline/data/graph_interactions_merged.json'
# Caminho do arquivo de saída (com categorias e novos nós)
OUTPUT_FILE = '../pipeline/data/graph_interactions_categorized.json'

# --- 1. CONFIGURAÇÃO DE CATEGORIAS EXISTENTES (PRESERVAR DO BANCO) ---
# Formato: username,Categoria
RAW_CATEGORIES_EXISTING = """
gusmoles,Frontend
Vinicius-Ribeiro04,Frontend
RochaCarla,Coordination
Arthrok,Infra
egewarth,Data
alvesisaque,Coordination
joycejdm,Data
brunapinos,Coordination
bot-do-jao,Infra
BrunaNayara,Developer
ednunes,Developer
bottinolucas,Data
davi-aguiar-vieira,Data
TiagoSBittencourt,Data
LuizaMaluf,Data
marcusmartinss,Infra
mat054,Data
Gxaite,Data
guilhermedfs,Developer
LeoSilvaGomes,Developer
renatocoral,Coordination
roddas,Infra
VictorJorgeFGA,Developer
flaviovl,Developer
ericbky,Data
suzaneduarte,Developer
Juan-Ricarte,Developer
CarolinaBarb,Developer
WillxBernardo,Data
MaiconMares,Developer
giovanniacg,Developer
VitorB2002,Developer
zlimaz,Security
algorithmorphic,Infra
paulohtfs,Developer
oo7gabriel,Developer
leonardogm,Developer
anaipva,Developer
hugorochaffs,Developer
Gustavo_MR,Developer
eduardaq2805,Developer
gaubiela,Developer
daniela0412,Developer
luccameds,Developer
leomichalski,Infra
Dexmachi,Infra
lelamo2002,Developer
GustavoHenriqueRS,Developer
mateuscavati,Security
Joao-amoedo,Data
CorreiaJV,Developer
MauricioMachadoFF,Developer
Thais-ra,Research
GZaranza,Data
PauloGoncalvesLima,Developer
GeovaneSFT,Developer
RenanGirao,Product
"""

# --- 2. NOVOS DADOS MANUAIS (SOLICITAÇÃO DO USUÁRIO) ---
# Formato: Nome,Imagem,Grupo
MANUAL_DATA_RAW = """
Breno_Gomes,https://media.licdn.com/dms/image/v2/C4E03AQFsf0LlXAfTqw/profile-displayphoto-shrink_800_800/profile-displayphoto-shrink_800_800/0/1638191130602?e=1770854400&v=beta&t=yQ5XmWt12R7Cixl7nhSMX6STHIDgF6_VBcnOH7TBKck,UI/UX
Marina_Alves,https://media.licdn.com/dms/image/v2/D4D03AQEcXFwpgtD7Yw/profile-displayphoto-crop_800_800/B4DZpQK0S5JUAI-/0/1762281619449?e=1770854400&v=beta&t=y70m3-G51MTSH0c4Pul_b2zmNeCohyrzX_dOV4yom7I,UI/UX
Clara_Barbosa,https://media.licdn.com/dms/image/v2/D4D03AQFlQ54DW4eLFQ/profile-displayphoto-shrink_800_800/profile-displayphoto-shrink_800_800/0/1718376509708?e=1770854400&v=beta&t=9cntPrQMLqYY49FmQzgcdCnlfJZaPTKoB-iWxStDWTk,UI/UX
Maria_Clara,https://media.licdn.com/dms/image/v2/D4D03AQHfLb0F59SAvA/profile-displayphoto-crop_800_800/B4DZjfH_0HHYAM-/0/1756090074859?e=1770854400&v=beta&t=fPOsFSzWZt51cTSSc9eLM3GKNozmVbMw4L8gQ6iF_r4,UI/UX
Lucas_Guimarães,https://media.licdn.com/dms/image/v2/D5603AQGoLZ7wPuOUfA/profile-displayphoto-crop_800_800/B56ZvhHMPnIUAI-/0/1769008320396?e=1770854400&v=beta&t=A9C0dlHwdl-YE6_Gzv-isvBIOngK3z4HRDP6DfSsks8,UI/UX
Ronivaldo_Junior,https://media.licdn.com/dms/image/v2/D4D03AQEAo9GSdonu8Q/profile-displayphoto-shrink_800_800/profile-displayphoto-shrink_800_800/0/1728356971628?e=1770854400&v=beta&t=rQ76-rKbPm04EyowJaE0tuKYuWGTTNLv5lNe74HPdWw,Research
Anna,https://media.licdn.com/dms/image/v2/D4D03AQEewOkeylQvaA/profile-displayphoto-crop_800_800/B4DZjIrOJOH0AQ-/0/1755713433149?e=1770854400&v=beta&t=opTpfIiiNT0_mqqWtW6YLEjuR1eoMWbaiyxnEL51YlM,Research
Kizia_Fonsêca,https://media.licdn.com/dms/image/v2/D4D03AQEu2X-QLbvNiw/profile-displayphoto-crop_800_800/B4DZmwMTCUGgAI-/0/1759597641039?e=1770854400&v=beta&t=ILKusRh0SemQtAu0iPyfE8lFsSEvoHgxTLCOkjCXrAM,Research
Lana_Vitória,https://media.licdn.com/dms/image/v2/D4D03AQFT21yFi4Nr_w/profile-displayphoto-crop_800_800/B4DZp1bWWaH0AI-/0/1762906696925?e=1770854400&v=beta&t=G8csW9l9I84rzyQaUKlN8OZmzyTzheo--uOk4MuP3Mc,Research
Cibelly_Lourenco,https://media.licdn.com/dms/image/v2/D4D03AQHCXpBTO3E7Aw/profile-displayphoto-shrink_800_800/profile-displayphoto-shrink_800_800/0/1709592005687?e=1770854400&v=beta&t=I8Adm-zOTi56SSOmbSIJ8eqWrhubvETLVpiqsSqbSfw,Dados
Luiza_Davison,https://media.licdn.com/dms/image/v2/D4D03AQGr-SLVkcziXg/profile-displayphoto-shrink_800_800/B4DZWmYULtGkAc-/0/1742253148125?e=1770854400&v=beta&t=FU0aAca12DgDMI0nBtv7MCqixtd31zBVeQZpH8g7JFw,Product
Paula_Ribeiro,www.linkedin.com/in/paulalgrr?miniProfileUrn=urn%3Ali%3Afs_miniProfile%3AACoAAClk7CIBCNnRfS4mJYY6r7g8bkipJ9n8Zpk&lipi=urn%3Ali%3Apage%3Ad_flagship3_company%3BlQ82mNkDSMyadLPwZykVgA%3D%3D,Product
Thalita_Quelita,https://media.licdn.com/dms/image/v2/D4D03AQGPD7pLRz2Mwg/profile-displayphoto-shrink_800_800/B4DZQxEICLGUAg-/0/1735989953238?e=1770854400&v=beta&t=IxAyGhp_NfkJQa6tWvA3iTr5SBmAjCCWhmXhUfJwr-0,Marketing
Caetano,,Marketing
Juliana_Petrocchi,https://media.licdn.com/dms/image/v2/D4D03AQFou2quM5Meww/profile-displayphoto-crop_800_800/B4DZm55IE4JcAI-/0/1759760385072?e=1770854400&v=beta&t=dEqe12cHcpc3JhxbobjvUsZCS0Y1-_kPqKQOjZYfots,Coordination
Mateus_Cavalcante,https://media.licdn.com/dms/image/v2/D4D03AQEDohVsOsfIqA/profile-displayphoto-shrink_800_800/B4DZeAMBD8G8Ac-/0/1750202324074?e=1770854400&v=beta&t=9CqZlhgC3sUlntbIceH4Xy-kpZ_3bzENbWrT85JgjHo,Security
"""

DEFAULT_CATEGORY = "Community"

def parse_categories(csv_text):
    cat_map = {}
    for line in csv_text.strip().split('\n'):
        if ',' in line:
            parts = line.split(',')
            user = parts[0].strip()
            if len(parts) == 2:
                cat_map[user] = parts[1].strip()
    return cat_map

def parse_manual_nodes(csv_text):
    nodes = {}
    for line in csv_text.strip().split('\n'):
        if not line.strip(): continue
        
        parts = line.split(',')
        if len(parts) >= 3:
            user = parts[0].strip()
            group = parts[-1].strip()
            # Imagem é tudo que sobrou no meio
            img = ",".join(parts[1:-1]).strip()
            
            # Normalizar URL
            if img and not img.startswith('http') and not img.startswith('www'):
                img = 'https://' + img
            elif img.startswith('www'):
                 img = 'https://' + img
            
            # Normalizar Grupo Dados -> Data
            if group == 'Dados':
                group = 'Data'
                
            nodes[user] = {
                "id": user,
                "group": group,
                "img": img,
                "val": 1.0,
                "sources": ["Manual Injection"]
            }
    return nodes

def create_link(source, target):
    return {
        "source": source,
        "target": target,
        "value": 1.0,
        "shared_repos": ["Manual Connection"],
        "interactions": []
    }

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Erro: {INPUT_FILE} não encontrado.")
        return

    print(f"Lendo {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 1. Carregar mapeamentos
    existing_cat_map = parse_categories(RAW_CATEGORIES_EXISTING)
    manual_nodes_map = parse_manual_nodes(MANUAL_DATA_RAW)
    
    existing_ids = {node['id'] for node in data.get('nodes', [])}
    
    # 2. Atualizar grupos existentes e consolidar membros
    group_members = {}
    
    # --- Passo A: Injetar novos nós PRIMEIRO ---
    for uid, node_data in manual_nodes_map.items():
        if uid not in existing_ids:
            new_node = {
                "id": uid,
                "group": node_data['group'],
                "img": node_data['img'],
                "val": 1.0,
                "sources": node_data['sources']
            }
            data['nodes'].append(new_node)
            existing_ids.add(uid)
            print(f"Injetado: {uid} ({node_data['group']})")
        else:
            # Se já existe, atualiza metadados
            for n in data['nodes']:
                if n['id'] == uid:
                    n['group'] = node_data['group']
                    if node_data['img']:
                        n['img'] = node_data['img']
                    break

    # --- Passo B: Reclassificar todos (existentes e novos) ---
    for node in data.get('nodes', []):
        uid = node['id']
        category = DEFAULT_CATEGORY
        
        # Prioridade da categoria: Manual > Mapeamento Existente > Default
        if uid in manual_nodes_map:
            category = manual_nodes_map[uid]['group']
        elif uid in existing_cat_map:
            category = existing_cat_map[uid]
            
        node['group'] = category
        
        if category not in group_members:
            group_members[category] = []
        group_members[category].append(uid)
        
    # debug grupos
    for g, m in group_members.items():
        print(f"Grupo {g}: {len(m)} membros")

    # 3. Processar Conexões
    new_links = []
    
    # Helper set para não duplicar conexões
    existing_links_set = set()
    for link in data.get('links', []):
        s, t = sorted([link['source'], link['target']])
        existing_links_set.add(f"{s}|{t}")
        
    def add_edge(u, v):
        if u == v: return
        if u not in existing_ids or v not in existing_ids:
            return
            
        key = f"{sorted([u, v])[0]}|{sorted([u, v])[1]}"
        if key not in existing_links_set:
            new_links.append(create_link(u, v))
            existing_links_set.add(key)

    # --- CLIQUES ---
    CLIQUE_GROUPS = ["Coordination", "UI/UX", "Research", "Security", "Product"]
    
    # Super grupo Product + Marketing (Thalita e Caetano)
    product_extensions = group_members.get("Marketing", [])
    
    # Rodar Cliques Padrão
    for gname in CLIQUE_GROUPS:
        target_members = set(group_members.get(gname, []))
        
        # Lógica especial Produto: incluir Marketing para fins de conexão, mas manter group original
        if gname == "Product":
            target_members.update(product_extensions)
            
        members_list = list(target_members)
        if len(members_list) > 1:
            print(f"Processando Clique {gname}: {len(members_list)} membros")
            for u1, u2 in itertools.combinations(members_list, 2):
                add_edge(u1, u2)

    # --- CONEXÕES ESPECÍFICAS ---
    
    # Lucas_Guimarães
    lg = "Lucas_Guimarães"
    add_edge(lg, "RochaCarla")
    add_edge(lg, "alvesisaque")
    add_edge(lg, "brunapinos")
    for fe in group_members.get("Frontend", []):
        add_edge(lg, fe)
    for pr in group_members.get("Product", []):
        add_edge(lg, pr)
    
    # Marina_Alves
    ma = "Marina_Alves"
    add_edge(ma, "RochaCarla")
    add_edge(ma, "alvesisaque")
    for fe in group_members.get("Frontend", []):
        add_edge(ma, fe)
    
    # Breno
    bren = "Breno_Gomes"
    add_edge(bren, "RochaCarla")
    add_edge(bren, "brunapinos")
    add_edge(bren, "Luiza_Davison")

    # Maria
    maria = "Maria_Clara"
    add_edge(maria, "Luiza_Davison")
    add_edge(maria, "egewarth")
    add_edge(maria, "RochaCarla")
    add_edge(maria, "alvesisaque")
    
    # Clara
    clara = "Clara_Barbosa"
    add_edge(clara, "RochaCarla")
    for fe in group_members.get("Frontend", []):
        add_edge(clara, fe)
    
    # Cibelly_Lourenco -> egewarth, RochaCarla, alvesisaque
    cl = "Cibelly_Lourenco"
    for t in ["egewarth", "RochaCarla", "alvesisaque", "WillxBernardo"]:
        add_edge(cl, t)
        
    # Luiza_Davison -> ednunes, RochaCarla, alvesisaque
    ld = "Luiza_Davison"
    for t in ["ednunes", "RochaCarla", "alvesisaque"]:
        add_edge(ld, t)
    
    # Paula_Ribeiro -> RochaCarla, alvesisaque
    pr = "Paula_Ribeiro"
    for t in ["RochaCarla", "alvesisaque", "giovanniacg", "CarolinaBarb"]:
        add_edge(pr, t)
    
    # Thalita_Quelita -> RochaCarla
    tq = "Thalita_Quelita"
    for t in ["RochaCarla"]:
        add_edge(tq, t)
    
    # Caetano -> RochaCarla
    ca = "Caetano"
    for t in ["RochaCarla"]:
        add_edge(ca, t)
    
    # Mateus_Cavalcante -> paulohtfs, roddas, GustavoHenriqueRS
    mc = "Mateus_Cavalcante"
    for t in ["paulohtfs", "roddas", "GustavoHenriqueRS"]:
        add_edge(mc, t)

    # Isaque
    isaq = "alvesisaque"
    for re in group_members.get("Research", []):
        add_edge(isaq, re)

    # Carla
    carla = "RochaCarla"
    for re in group_members.get("Research", []):
        add_edge(carla, re)
        
    # Juliana
    ju = "Juliana_Petrocchi"
    add_edge = (ju, "engewarth")

    data['links'].extend(new_links)
    print(f"Total de conexões adicionadas: {len(new_links)}")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Arquivo salvo: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
