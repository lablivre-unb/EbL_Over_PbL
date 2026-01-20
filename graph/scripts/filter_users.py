import json
import os
import copy

# --- CONFIGURAÇÕES ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, '../pipeline/data/graph_interactions.json')
OUTPUT_FILE = os.path.join(BASE_DIR, '../pipeline/data/graph_interactions_merged.json')

# Sua lista de identidades (Primeiro nome = ID Oficial/Mestre)
RAW_LIST = """
gusmoles,
Vinicius-Ribeiro04,
RochaCarla,rocha.carla
Arthrok,Arthrok
egewarth,egewarth
alvesisaque,alvesisaque
joycejdm,joyce.jdm
brunapinos,brunapinos
bot-do-jao,bot-do-jao
BrunaNayara,
ednunes,Edu_25
bottinolucas,
davi-aguiar-vieira,davideaguiarvieira
TiagoSBittencourt,TiagoSBittencourt
LuizaMaluf,
marcusmartinss,
mat054,mat054
Gxaite,Gxaite
guilhermedfs,guilhermedfs14
LeoSilvaGomes,LeoSilvaGomes
renatocoral,renatocoral
roddas,roddas
VictorJorgeFGA,VictorJorgeFGA
flaviovl,flavio.vl
ericbky,eric.bky
suzaneduarte,ssuzane9
Juan-Ricarte,Juan-Ricarte
CarolinaBarb,CarolinaBarb
WillxBernardo,WillxBernardo
MaiconMares,MaiconMares
caiooliv,
giovanniacg,giovanniacg
VitorB2002,VitorB2002
zlimaz,zlimaz
algorithmorphic,
paulohtfs,paulohtfs
oo7gabriel,oo7gabriel
,leonardogm
,anaipva
hugorochaffs,hugorochaffs
,Gustavo_MR
,eduardaq2805
gaubiela,gaubiela
,daniela0412
luccameds,luccameds
leomichalski,leomichalski
Dexmachi,Dexmachi
lelamo2002,
GustavoHenriqueRS,gustavohenriqueprivado
mateuscavati,mateuscavati
CorreiaJV,CorreiaJV,
MauricioMachadoFF
Thais-ra,Thais-ra
GZaranza,GZaranza
Joao-amoedo,joao-amoedo
PauloGoncalvesLima,PauloGoncalvesLima
GeovaneSFT,GeovaneSFT
RenanGirao,
"""

def normalize(text):
    """Padroniza IDs para comparação (remove @, espaços, lowercase)"""
    if not text:
        return ""
    return str(text).strip().lower().replace('@', '')

def build_identity_map(raw_text):
    """
    Cria um mapa: { 'alias_normalizado': 'ID_MESTRE_ORIGINAL' }
    Ex: { 'rocha.carla': 'RochaCarla', 'rochacarla': 'RochaCarla' }
    """
    mapping = {}
    valid_masters = set()

    for line in raw_text.strip().split('\n'):
        # Separa por vírgula e remove vazios
        parts = [p.strip() for p in line.split(',') if p.strip()]
        
        if not parts:
            continue

        # O primeiro item não vazio é o MESTRE (Canonical ID)
        master_id = parts[0] 
        valid_masters.add(master_id)

        # Mapeia TODAS as partes (inclusive o próprio mestre) para o ID Mestre
        for part in parts:
            clean = normalize(part)
            mapping[clean] = master_id
            
    return mapping, valid_masters

def merge_nodes(original_nodes, identity_map):
    merged_nodes = {} # Key: MasterID -> NodeObject

    for node in original_nodes:
        if 'id' not in node:
            continue
        
        norm_id = normalize(node['id'])
        
        # Só processa se estiver na nossa lista (Whitelist)

        if norm_id in identity_map:
            master_id = identity_map[norm_id]
            
            # Se é a primeira vez que vemos esse Mestre, cria o nó
            if master_id not in merged_nodes:
                new_node = copy.deepcopy(node)
                new_node['id'] = master_id # Garante o ID correto
                new_node['sources'] = set(new_node.get('sources', [])) # Converte lista para set
                merged_nodes[master_id] = new_node
            else:
                # Se o Mestre já existe (estamos mesclando um alias ou duplicata)
                existing = merged_nodes[master_id]
                
                # 1. Soma Valor (Peso)
                existing['val'] = (existing.get('val', 1) or 1) + (node.get('val', 1) or 1)
                
                # 2. Mescla Sources (Repositórios)
                existing['sources'].update(node.get('sources', []))
                
                # 3. Prioriza imagem válida (Se a atual for ruim e a nova for boa, troca)
                # (Lógica simples: assume que o primeiro da lista RAW_LIST tem a imagem boa. 
                # Se quiser algo mais complexo, verificamos se a URL termina em png)

    # Converte sets de volta para listas para JSON
    for nid in merged_nodes:
        merged_nodes[nid]['sources'] = list(merged_nodes[nid]['sources'])

    return list(merged_nodes.values())

def merge_links(original_links, identity_map, valid_masters):
    merged_links = {} # Key: "SourceID|TargetID" -> LinkObject

    for link in original_links:
        # Pega IDs crus
        s_raw = link['source']['id'] if isinstance(link['source'], dict) else link['source']
        t_raw = link['target']['id'] if isinstance(link['target'], dict) else link['target']
        
        s_norm = normalize(s_raw)
        t_norm = normalize(t_raw)

        # Verifica se ambos estão no mapa
        if s_norm in identity_map and t_norm in identity_map:
            master_source = identity_map[s_norm]
            master_target = identity_map[t_norm]

            # REGRA CRUCIAL: Ignora auto-links (Ex: RochaCarla interagindo com rocha.carla)
            if master_source == master_target:
                continue

            # Chave única para o link (Direcional)
            link_key = f"{master_source}|{master_target}"

            if link_key not in merged_links:
                # Cria novo link limpo
                new_link = {
                    "source": master_source,
                    "target": master_target,
                    "value": link.get('value', 0),
                    "shared_repos": set(link.get('shared_repos', [])),
                    "interactions": link.get('interactions', []) # Lista de objetos
                }
                merged_links[link_key] = new_link
            else:
                # Funde com link existente
                existing = merged_links[link_key]
                existing['value'] += link.get('value', 0)
                existing['shared_repos'].update(link.get('shared_repos', []))
                existing['interactions'].extend(link.get('interactions', []))

    # Limpeza final dos links (converter sets para listas)
    final_links = []
    for lv in merged_links.values():
        lv['shared_repos'] = list(lv['shared_repos'])
        final_links.append(lv)

    return final_links

def main():
    print("--- INICIANDO MERGE DE IDENTIDADES ---")
    
    # 1. Mapa de Identidade
    id_map, valid_masters = build_identity_map(RAW_LIST)
    print(f"Mapeamento criado para {len(id_map)} aliases apontando para {len(valid_masters)} usuários únicos.")

    # 2. Carregar
    if not os.path.exists(INPUT_FILE):
        print("Arquivo de entrada não encontrado.")
        return
        
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 3. Processar
    final_nodes = merge_nodes(data.get('nodes', []), id_map)
    final_links = merge_links(data.get('links', []), id_map, valid_masters)

    # 4. Salvar
    output = {"nodes": final_nodes, "links": final_links}
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)

    print("--- CONCLUÍDO ---")
    print(f"Nós Originais: {len(data.get('nodes', []))} -> Finais: {len(final_nodes)}")
    print(f"Links Originais: {len(data.get('links', []))} -> Finais: {len(final_links)}")
    print(f"Arquivo salvo em: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()