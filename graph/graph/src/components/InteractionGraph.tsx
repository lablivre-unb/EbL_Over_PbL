import React, { useState, useRef, useCallback, useEffect, useMemo } from 'react';
import ForceGraph2D, { type LinkObject, type NodeObject } from 'react-force-graph-2d';

// --- TIPAGEM ---
type Interaction = {
  actor: string;
  target: string;
  type: string;
  repo: string;
};

type GraphNode = {
  id: string;
  group: string;
  val: number;
  img: string;
  sources: string[];
} & NodeObject;

type GraphLink = {
  source: string | GraphNode;
  target: string | GraphNode;
  value: number;
  shared_repos: string[];
  interactions: Interaction[];
} & LinkObject;

interface GraphData {
  nodes: GraphNode[];
  links: GraphLink[];
}

// --- CORES DOS TIMES ---
const GROUP_COLORS: Record<string, string> = {
  "Frontend": "#a855f7",     // Purple 500
  "Coordination": "#f59e0b", // Amber 500
  "Data": "#10b981",         // Emerald 500
  "Infra": "#06b6d4",        // Cyan 500
  "Developer": "#3b82f6",    // Blue 500
  "Security": "#ef4444",     // Red 500
  "Product": "#f97316",      // Orange 500
  "UI/UX": "#ec4899",        // Pink 500
  "Research": "#14b8a6",     // Teal 500
  "Marketing": "#eab308",    // Yellow 500
  "Community": "#9ca3af",    // Gray 400
};

export default function InteractionGraph({ data }: { data: GraphData }) {
  const fgRef = useRef<any>();
  const [hoverLink, setHoverLink] = useState<GraphLink | null>(null);

  // Cache de imagens
  const imagesRef = useRef<Record<string, HTMLImageElement>>({});
  // Estado contador para forçar redesenho
  const [, setImagesLoaded] = useState(0);

  // --- CONFIGURAÇÃO DE FÍSICA (ESPAÇAMENTO) ---
  useEffect(() => {
    // Pequeno delay para garantir que o engine inicializou
    const timer = setTimeout(() => {
      if (fgRef.current) {
        // 1. Aumenta a Repulsão (Charge)
        fgRef.current.d3Force('charge').strength(-120);

        // 2. Aumenta a Distância dos Links
        fgRef.current.d3Force('link').distance(50);

        // 3. Reaquece a simulação para aplicar as mudanças
        fgRef.current.d3ReheatSimulation();
      }
    }, 100);
    return () => clearTimeout(timer);
  }, []); // Roda apenas uma vez na montagem

  // --- PRÉ-CARREGAMENTO SIMPLIFICADO ---
  useEffect(() => {
    let loadedCount = 0;

    data.nodes.forEach((node) => {
      if (imagesRef.current[node.id]) return;

      const img = new Image();
      img.onload = () => {
        imagesRef.current[node.id] = img;
        loadedCount++;
        if (loadedCount % 5 === 0 || loadedCount === data.nodes.length) {
          setImagesLoaded(prev => prev + 1);
        }
      };
      img.onerror = () => { };
      img.src = node.img;
    });
  }, [data]);

  const memoizedGraphData = useMemo(() => {
    return {
      nodes: data.nodes.map(n => ({ ...n })),
      links: data.links.map(l => ({ ...l }))
    };
  }, [data]);

  // --- DESENHO DO NÓ (BLINDADO) ---
  const nodeCanvasObject = useCallback((node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const size = 12 + (node.val || 1);
    const fontSize = 12 / globalScale;

    // Cor do Grupo
    const groupColor = GROUP_COLORS[node.group] || GROUP_COLORS['Community'];

    // --- CAMADA 1: FUNDO ---
    ctx.beginPath();
    ctx.arc(node.x, node.y, size, 0, 2 * Math.PI, false);
    ctx.fillStyle = '#64748b';
    ctx.fill();
    ctx.lineWidth = 2 / globalScale;
    ctx.strokeStyle = groupColor; // Borda da cor do time
    ctx.stroke();

    // --- CAMADA 2: IMAGEM ---
    const img = imagesRef.current[node.id];

    if (img && img.naturalWidth > 0) {
      try {
        ctx.save();
        ctx.beginPath();
        ctx.arc(node.x, node.y, size - 1, 0, Math.PI * 2, true);
        ctx.closePath();
        ctx.clip();
        ctx.drawImage(img, node.x - size, node.y - size, size * 2, size * 2);
        ctx.restore();
      } catch (err) { }
    } else {
      // --- CAMADA 3: INICIAL ---
      const initial = (node.id || "?").charAt(0).toUpperCase();
      ctx.font = `bold ${size}px Sans-Serif`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillStyle = '#ffffff';
      ctx.fillText(initial, node.x, node.y);
    }

    // --- CAMADA 4: NOME ---
    ctx.font = `${fontSize}px Sans-Serif`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';

    const textWidth = ctx.measureText(node.id).width;
    ctx.fillStyle = 'rgba(255, 255, 255, 0.7)';
    ctx.fillRect(node.x - textWidth / 2 - 2, node.y + size + 2, textWidth + 4, fontSize + 4);

    ctx.fillStyle = '#000';
    ctx.fillText(node.id, node.x, node.y + size + fontSize / 2 + 4);

  }, []);

  // --- DESENHO DO LINK (GRADIENTE) ---
  const linkCanvasObject = useCallback((link: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const start = link.source;
    const end = link.target;

    // Garantia de coordenadas
    if (!start || !end || start.x === undefined || end.x === undefined) return;

    const sourceColor = GROUP_COLORS[start.group] || GROUP_COLORS['Community'];
    const targetColor = GROUP_COLORS[end.group] || GROUP_COLORS['Community'];

    // Cria gradiente linear do source até o target
    const gradient = ctx.createLinearGradient(start.x, start.y, end.x, end.y);
    gradient.addColorStop(0, sourceColor);
    gradient.addColorStop(1, targetColor);

    ctx.beginPath();
    ctx.moveTo(start.x, start.y);
    ctx.lineTo(end.x, end.y);

    ctx.strokeStyle = gradient;
    ctx.lineWidth = Math.min((link.value || 1) * 0.5, 5) / globalScale; // Mesma largura anterior
    ctx.stroke();
  }, []);

  return (
    <div className="relative w-full h-screen bg-gray-50 overflow-hidden">
      <ForceGraph2D
        ref={fgRef}
        graphData={memoizedGraphData}

        nodeCanvasObject={nodeCanvasObject}
        nodePointerAreaPaint={(node: any, color, ctx) => {
          const size = 12 + ((node.val || 1) * 2);
          ctx.fillStyle = color;
          ctx.beginPath();
          ctx.arc(node.x, node.y, size, 0, 2 * Math.PI, false);
          ctx.fill();
        }}

        // Link Rendering Customizado
        linkCanvasObject={linkCanvasObject}
        linkDirectionalParticles={(link: any) => link.interactions?.length > 0 ? 2 : 0}
        linkDirectionalParticleSpeed={0.005}

        onLinkHover={(link: any) => setHoverLink(link)}

        // Aumentei o cooldownTicks para a animação ter tempo de estabilizar na posição nova
        cooldownTicks={200}
        onEngineStop={() => fgRef.current.zoomToFit(400)}
      />

      {/* --- TOOLTIP --- */}
      {hoverLink && (
        <div className="absolute top-4 right-4 bg-white/95 backdrop-blur shadow-2xl rounded-lg border border-gray-200 p-4 w-80 max-h-[80vh] overflow-y-auto z-50">
          <h3 className="text-sm font-bold text-gray-500 uppercase tracking-wide border-b pb-2 mb-2">
            Detalhes da Conexão
          </h3>
          <div className="flex justify-between items-center mb-4">
            <div className="text-center w-1/2 break-words">
              <span className="block font-bold text-gray-800 text-sm" style={{ color: GROUP_COLORS[(hoverLink.source as any).group] }}>
                {typeof hoverLink.source === 'object' ? (hoverLink.source as any).id : hoverLink.source}
              </span>
            </div>
            <div className="text-gray-400">↔</div>
            <div className="text-center w-1/2 break-words">
              <span className="block font-bold text-gray-800 text-sm" style={{ color: GROUP_COLORS[(hoverLink.target as any).group] }}>
                {typeof hoverLink.target === 'object' ? (hoverLink.target as any).id : hoverLink.target}
              </span>
            </div>
          </div>
          <div className="mb-4">
            <p className="text-xs font-semibold text-gray-400 mb-1">Repositórios Compartilhados</p>
            <div className="flex flex-wrap gap-1">
              {hoverLink.shared_repos?.map((repo: string) => (
                <span key={repo} className="px-2 py-0.5 bg-gray-100 text-gray-600 rounded text-[10px] border">
                  {repo}
                </span>
              ))}
            </div>
          </div>
          {hoverLink.interactions?.length > 0 ? (
            <div>
              <p className="text-xs font-semibold text-gray-400 mb-2">Interações ({hoverLink.interactions.length})</p>
              <div className="space-y-2">
                {hoverLink.interactions.map((act: Interaction, i: number) => (
                  <div key={i} className="flex flex-col bg-slate-50 p-2 rounded border border-slate-100 text-xs">
                    <div className="flex justify-between items-start mb-1">
                      <span className={`font-bold px-1.5 rounded 
                                    ${act.type === 'MERGED_PR' ? 'bg-purple-100 text-purple-700' : ''}
                                    ${act.type === 'REVIEWED_PR' ? 'bg-blue-100 text-blue-700' : ''}
                                    ${act.type === 'CLOSED_ISSUE' ? 'bg-red-100 text-red-700' : ''}
                                `}>
                        {act.type.replace('_', ' ')}
                      </span>
                    </div>
                    <div className="text-gray-600">
                      <span className="font-semibold text-gray-900">{act.actor}</span> em {act.repo}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="text-xs text-gray-400 italic text-center py-2 bg-gray-50 rounded">
              Apenas colegas de time
            </div>
          )}
        </div>
      )}
    </div>
  );
}