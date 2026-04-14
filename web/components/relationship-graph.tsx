"use client";

import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import { Expand, X } from "lucide-react";
import { toPng } from "html-to-image";

import { AppPanel, Button, FieldLabel, SelectControl, StatusBadge } from "@/components/ui";
import { GraphData, ReviewQueueItem } from "@/lib/types";
import { runAccountGraphAnalysis, saveAccountGraphCapture } from "@/lib/api";

type GraphNode = GraphData["nodes"][number];
type GraphEdge = GraphData["edges"][number];

type NodeMeta = {
  label: string;
  color: string;
  tone: "neutral" | "accent" | "warning" | "danger" | "persona";
};

type NodeType = GraphNode["type"];

type SimNode = {
  id: string;
  label: string;
  type: NodeType;
  avatarUrl?: string;
  baseWeight: number;
  degree: number;
  pinned: boolean;
  x: number;
  y: number;
  z: number;
  vx: number;
  vy: number;
  vz: number;
};

type SimEdge = {
  id: string;
  source: string;
  target: string;
  type: string;
  weight: number;
  color: string;
  label: string;
};

type GraphModel = {
  nodes: SimNode[];
  edges: SimEdge[];
  nodeMap: Record<string, SimNode>;
  adjacency: Record<string, Array<{ id: string; weight: number; edgeType: string }>>;
  typeCounts: Record<string, number>;
  edgeWeightRange: { min: number; max: number };
};

type PersonRisk = {
  id: string;
  score: number;
  level: "critical" | "high" | "medium" | "low";
  reasons: string[];
};

type ProjectedNode = {
  id: string;
  sx: number;
  sy: number;
  scale: number;
  depth: number;
  radius: number;
};

type RiskPalette = {
  fill: string;
  glow: string;
  badgeClass: string;
};

type PersonCardModel = {
  username: string;
  initials: string;
  gradient: string;
  avatarUrl: string;
};

const NODE_META: Record<string, NodeMeta> = {
  account: { label: "hesap", color: "#24d1c3", tone: "accent" },
  organization: { label: "orgut", color: "#e9c46a", tone: "warning" },
  category: { label: "kategori", color: "#7c94b6", tone: "neutral" },
  commenter: { label: "yorumcu", color: "#9b8cff", tone: "persona" },
  verdict: { label: "karar", color: "#f2994a", tone: "warning" },
  threat: { label: "tehdit", color: "#ff647c", tone: "danger" },
  summary: { label: "ozet", color: "#6b7685", tone: "neutral" },
};

const EDGE_LABEL: Record<string, string> = {
  linked_to: "orgut bagi",
  posted_about: "tehdit bagi",
  flagged_by: "bayrakli yorum",
  commented_on: "yorum etkilesimi",
  related_to: "ikincil bag",
  matches_category: "kategori ortusmesi",
  grouped: "gruplu iliski",
};

const TYPE_ORDER = ["account", "organization", "threat", "category", "commenter", "verdict", "summary"];

const DEMO_PROFILE_PHOTOS = [
  "minio://instagram-archive/instagram/huseyinulukoylu54/20260411T073206Z/profile/profile_image/623138473_17870793696536194_5980835999520243314_n.jpg",
  "minio://instagram-archive/instagram/cdk.liege/20260408T141441Z/profile/profile_image/491468731_1209192290904030_3733353055987002229_n.jpg",
];

function normalizeAvatarSource(raw: unknown): string | undefined {
  if (typeof raw !== "string") return undefined;
  const value = raw.trim();
  if (!value) return undefined;
  if (value.startsWith("minio://")) return value;
  if (value.includes("instagram.com/") && !value.includes("fbcdn.net/")) return undefined;
  if (value.startsWith("http://") || value.startsWith("https://")) return value;
  return undefined;
}

function randomSeeded(seed: number) {
  let value = seed;
  return () => {
    value = (value * 1664525 + 1013904223) % 4294967296;
    return value / 4294967296;
  };
}

function aggregateEdges(edges: GraphEdge[]) {
  const map = new Map<string, SimEdge>();

  for (const edge of edges) {
    const key = `${edge.source}|${edge.target}|${edge.type}`;
    const current = map.get(key);
    if (current) {
      current.weight += 1;
      continue;
    }
    map.set(key, {
      id: key,
      source: edge.source,
      target: edge.target,
      type: edge.type,
      weight: 1,
      color: edge.type === "flagged_by" ? "#ff647c" : edge.type === "linked_to" ? "#e9c46a" : "#5aaefa",
      label: EDGE_LABEL[edge.type] || edge.type,
    });
  }

  return [...map.values()];
}

function makeGraphModel(data: GraphData): GraphModel {
  const aggregated = aggregateEdges(data.edges);
  const degree: Record<string, number> = {};

  for (const edge of aggregated) {
    degree[edge.source] = (degree[edge.source] || 0) + edge.weight;
    degree[edge.target] = (degree[edge.target] || 0) + edge.weight;
  }

  const seeded = randomSeeded(data.nodes.length * 1337 + aggregated.length * 97 + 11);
  const nodes: SimNode[] = data.nodes.map((node, index) => {
    const band = 180 + (index % 5) * 38;
    const angle = seeded() * Math.PI * 2;
    const elevation = (seeded() - 0.5) * 180;
    return {
      id: node.id,
      label: node.label,
      type: node.type,
      avatarUrl: normalizeAvatarSource(node.avatar_url) || pickDemoPhoto(node.label),
      baseWeight: Math.max(1, node.weight || 1),
      degree: degree[node.id] || 0,
      pinned: false,
      x: Math.cos(angle) * band,
      y: elevation,
      z: Math.sin(angle) * band,
      vx: 0,
      vy: 0,
      vz: 0,
    };
  });

  const nodeMap = Object.fromEntries(nodes.map((item) => [item.id, item]));
  const adjacency: GraphModel["adjacency"] = {};
  for (const node of nodes) adjacency[node.id] = [];

  for (const edge of aggregated) {
    if (!nodeMap[edge.source] || !nodeMap[edge.target]) continue;
    adjacency[edge.source].push({ id: edge.target, weight: edge.weight, edgeType: edge.type });
    adjacency[edge.target].push({ id: edge.source, weight: edge.weight, edgeType: edge.type });
  }

  const typeCounts: Record<string, number> = {};
  for (const node of nodes) typeCounts[node.type] = (typeCounts[node.type] || 0) + 1;

  const weights = aggregated.map((item) => item.weight);
  return {
    nodes,
    edges: aggregated,
    nodeMap,
    adjacency,
    typeCounts,
    edgeWeightRange: {
      min: weights.length ? Math.min(...weights) : 0,
      max: weights.length ? Math.max(...weights) : 0,
    },
  };
}

function toTypeList(counts: Record<string, number>) {
  return TYPE_ORDER.filter((type) => counts[type]).concat(
    Object.keys(counts).filter((type) => !TYPE_ORDER.includes(type)),
  );
}

function threatSeverity(label: string) {
  const normalized = label.trim().toLowerCase();
  if (normalized === "kritik" || normalized === "critical") return 4;
  if (normalized === "yuksek" || normalized === "high") return 3;
  if (normalized === "orta" || normalized === "medium") return 2;
  if (normalized === "dusuk" || normalized === "low") return 1;
  return 1.5;
}

function riskLevelFromScore(score: number): PersonRisk["level"] {
  if (score >= 16) return "critical";
  if (score >= 9) return "high";
  if (score >= 4) return "medium";
  return "low";
}

function normalizeUsername(value: string | null | undefined) {
  return (value || "").trim().toLowerCase();
}

function riskLevelLabel(level?: PersonRisk["level"]) {
  if (level === "critical") return "kritik";
  if (level === "high") return "yüksek";
  if (level === "medium") return "orta";
  if (level === "low") return "düşük";
  return "belirsiz";
}

function riskLevelRank(level: PersonRisk["level"]) {
  if (level === "critical") return 4;
  if (level === "high") return 3;
  if (level === "medium") return 2;
  return 1;
}

function riskPalette(score: number | undefined): RiskPalette {
  if ((score || 0) >= 7) {
    return {
      fill: "#b00020",
      glow: "rgba(176,0,32,0.96)",
      badgeClass: "border-[rgba(176,0,32,0.78)] bg-[rgba(176,0,32,0.3)] text-[#ffd4dc]",
    };
  }
  if ((score || 0) >= 5) {
    return {
      fill: "#f97316",
      glow: "rgba(249,115,22,0.95)",
      badgeClass: "border-[rgba(249,115,22,0.68)] bg-[rgba(249,115,22,0.22)] text-[#ffe2c9]",
    };
  }
  return {
    fill: "#9b8cff",
    glow: "rgba(155,140,255,0.9)",
    badgeClass: "border-[rgba(155,140,255,0.45)] bg-[rgba(155,140,255,0.16)] text-[#e4ddff]",
  };
}

function drawRoundedRect(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
  radius: number,
) {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + width - r, y);
  ctx.quadraticCurveTo(x + width, y, x + width, y + r);
  ctx.lineTo(x + width, y + height - r);
  ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
  ctx.lineTo(x + r, y + height);
  ctx.quadraticCurveTo(x, y + height, x, y + height - r);
  ctx.lineTo(x, y + r);
  ctx.quadraticCurveTo(x, y, x + r, y);
  ctx.closePath();
}

function proxyAvatarUrl(url: string | undefined) {
  if (!url) return "";
  return `/api/avatar?source=${encodeURIComponent(url)}`;
}

function pickDemoPhoto(username: string) {
  if (!DEMO_PROFILE_PHOTOS.length) return "";
  const seed = [...(username || "user")].reduce((acc, char) => acc + char.charCodeAt(0), 0);
  return DEMO_PROFILE_PHOTOS[seed % DEMO_PROFILE_PHOTOS.length];
}

function branchLeafLabel(username: string) {
  if (!username.startsWith("branch_")) return username;
  if (username.endsWith("_a")) return "L-1";
  if (username.endsWith("_b")) return "L-2";
  return "LEAF";
}

function makePersonCardModel(username: string): PersonCardModel {
  const clean = (username || "user").trim().replace(/^@+/, "") || "user";
  const parts = clean.split(/[._-]+/).filter(Boolean);
  const initials = (parts[0]?.[0] || clean[0] || "u").toUpperCase() + (parts[1]?.[0] || "").toUpperCase();
  const seed = [...clean].reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const hueA = seed % 360;
  const hueB = (seed * 1.7 + 73) % 360;
  return {
    username: clean,
    initials,
    gradient: `linear-gradient(135deg, hsl(${hueA} 72% 42%), hsl(${hueB} 78% 34%))`,
    avatarUrl: pickDemoPhoto(clean),
  };
}

function buildDangerNarrative(
  username: string,
  risk: PersonRisk | undefined,
  review: ReviewQueueItem | undefined,
): string {
  if (!risk) {
    return `@${username} için belirgin risk sinyali bulunamadı.`;
  }
  const reasons = (risk.reasons || []).slice(0, 2).join("; ");
  const trigger = review?.trigger_count ? `${review.trigger_count} kez` : "en az 1 kez";
  const lastReason = (review?.last_reason || "").trim();
  const summaryParts = [
    `@${username} bu soruşturmada ${risk.score.toFixed(1)} risk puanı ile ${riskLevelLabel(risk.level)} seviyede görünüyor.`,
    `Kuyruk tetiği: ${trigger}.`,
    reasons ? `Ana sinyaller: ${reasons}.` : "",
    lastReason ? `Son işaret nedeni: ${lastReason}` : "",
  ].filter(Boolean);
  return summaryParts.join(" ");
}

function buildPersonRiskRanking(model: GraphModel, edges: SimEdge[], allowedNodes: Set<string>) {
  const commenterIds = [...allowedNodes].filter((id) => model.nodeMap[id]?.type === "commenter");
  const result: PersonRisk[] = [];

  for (const commenterId of commenterIds) {
    let flaggedWeight = 0;
    let commentWeight = 0;
    let threatWeight = 0;
    let orgWeight = 0;
    const threatLabels = new Set<string>();

    for (const edge of edges) {
      const touches =
        (edge.source === commenterId && allowedNodes.has(edge.target)) ||
        (edge.target === commenterId && allowedNodes.has(edge.source));
      if (!touches) continue;

      const counterpartId = edge.source === commenterId ? edge.target : edge.source;
      const counterpart = model.nodeMap[counterpartId];
      if (!counterpart) continue;

      if (edge.type === "flagged_by") flaggedWeight += edge.weight;
      if (edge.type === "commented_on") commentWeight += edge.weight;
      if (counterpart.type === "threat") {
        threatWeight += edge.weight * threatSeverity(counterpart.label);
        threatLabels.add(counterpart.label);
      }
      if (counterpart.type === "organization") orgWeight += edge.weight;
    }

    const score = flaggedWeight * 4 + commentWeight * 1.3 + threatWeight * 1.8 + orgWeight * 1.2;
    if (score <= 0) continue;

    const reasons: string[] = [];
    if (flaggedWeight > 0) reasons.push(`Yorumlardan ${flaggedWeight} bayrak sinyali`);
    if (commentWeight > 0) reasons.push(`${commentWeight} yogun yorum etkilesimi`);
    if (threatLabels.size > 0) reasons.push(`Bagli tehdit kumeleri: ${[...threatLabels].join(", ")}`);
    if (orgWeight > 0) reasons.push(`Orgut baglam puani: ${orgWeight}`);
    if (!reasons.length) reasons.push("Yapisal olarak riskli profil algilandi");

    result.push({
      id: commenterId,
      score,
      level: riskLevelFromScore(score),
      reasons,
    });
  }

  return result.sort((a, b) => b.score - a.score);
}

function ForceGraphCanvas({
  model,
  visibleNodeIds,
  visibleEdges,
  selectedNodeId,
  highlightedPathEdges,
  clusterMode,
  riskyNodeLevels,
  riskyNodeScores,
  onNodeClick,
}: {
  model: GraphModel;
  visibleNodeIds: Set<string>;
  visibleEdges: SimEdge[];
  selectedNodeId?: string | null;
  highlightedPathEdges: Set<string>;
  clusterMode: boolean;
  riskyNodeLevels: Record<string, PersonRisk["level"]>;
  riskyNodeScores: Record<string, number>;
  onNodeClick: (nodeId: string) => void;
}) {
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const projectedRef = useRef<ProjectedNode[]>([]);
  const [size, setSize] = useState({ width: 1000, height: 600 });
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const rotationRef = useRef({ yaw: 0.12, pitch: -0.08, zoom: 1 });
  const dragRef = useRef<{ active: boolean; x: number; y: number }>({ active: false, x: 0, y: 0 });
  const nodeDragRef = useRef<{ id: string | null; x: number; y: number }>({ id: null, x: 0, y: 0 });
  const imageCacheRef = useRef<Record<string, HTMLImageElement>>({});
  const [, setImageTick] = useState(0);

  useEffect(() => {
    const node = wrapRef.current;
    if (!node) return undefined;

    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect) return;
      setSize({ width: Math.max(320, rect.width), height: Math.max(320, rect.height) });
    });

    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const nodesForPhysics = useMemo(() => {
    return model.nodes.filter((node) => visibleNodeIds.has(node.id));
  }, [model.nodes, visibleNodeIds]);

  const edgesForPhysics = useMemo(() => {
    const active = new Set(visibleNodeIds);
    return visibleEdges.filter((edge) => active.has(edge.source) && active.has(edge.target));
  }, [visibleEdges, visibleNodeIds]);

  useEffect(() => {
    const avatarUrls = nodesForPhysics
      .map((node) => proxyAvatarUrl(node.avatarUrl))
      .filter((url) => Boolean(url));
    for (const url of avatarUrls) {
      if (!url || imageCacheRef.current[url]) continue;
      const img = new Image();
      img.crossOrigin = "anonymous";
      img.onload = () => {
        imageCacheRef.current[url] = img;
        setImageTick((value) => value + 1);
      };
      img.onerror = () => {
        // fallback handled in draw phase by skipping image draw
      };
      img.src = url;
    }
  }, [nodesForPhysics]);

  useEffect(() => {
    let frame = 0;
    const canvas = canvasRef.current;
    if (!canvas) return undefined;
    const context = canvas.getContext("2d");
    if (!context) return undefined;

    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.floor(size.width * dpr);
    canvas.height = Math.floor(size.height * dpr);
    canvas.style.width = `${size.width}px`;
    canvas.style.height = `${size.height}px`;
    context.setTransform(dpr, 0, 0, dpr, 0, 0);

    const clusters = toTypeList(model.typeCounts);
    const clusterPositions = new Map<string, [number, number, number]>();
    clusters.forEach((type, index) => {
      const angle = (index / Math.max(1, clusters.length)) * Math.PI * 2;
      clusterPositions.set(type, [Math.cos(angle) * 210, Math.sin(angle * 1.7) * 110, Math.sin(angle) * 210]);
    });

    const physics = () => {
      const repulsion = 8200;
      const springStrength = 0.015;
      const damping = 0.86;
      const centerPull = 0.005;

      for (let i = 0; i < nodesForPhysics.length; i += 1) {
        const a = nodesForPhysics[i];

        for (let j = i + 1; j < nodesForPhysics.length; j += 1) {
          const b = nodesForPhysics[j];
          let dx = b.x - a.x;
          let dy = b.y - a.y;
          let dz = b.z - a.z;
          const distSq = dx * dx + dy * dy + dz * dz + 0.01;
          const dist = Math.sqrt(distSq);
          dx /= dist;
          dy /= dist;
          dz /= dist;
          const push = repulsion / distSq;
          if (!a.pinned) {
            a.vx -= dx * push;
            a.vy -= dy * push;
            a.vz -= dz * push;
          }
          if (!b.pinned) {
            b.vx += dx * push;
            b.vy += dy * push;
            b.vz += dz * push;
          }
        }
      }

      for (const edge of edgesForPhysics) {
        const source = model.nodeMap[edge.source];
        const target = model.nodeMap[edge.target];
        if (!source || !target) continue;

        const dx = target.x - source.x;
        const dy = target.y - source.y;
        const dz = target.z - source.z;
        const dist = Math.sqrt(dx * dx + dy * dy + dz * dz) || 0.0001;
        const desired = 120 - Math.min(40, edge.weight * 5);
        const stretch = dist - desired;
        const pull = springStrength * stretch * (1 + edge.weight * 0.06);

        const nx = dx / dist;
        const ny = dy / dist;
        const nz = dz / dist;

        if (!source.pinned) {
          source.vx += nx * pull;
          source.vy += ny * pull;
          source.vz += nz * pull;
        }
        if (!target.pinned) {
          target.vx -= nx * pull;
          target.vy -= ny * pull;
          target.vz -= nz * pull;
        }
      }

      for (const node of nodesForPhysics) {
        if (node.pinned) {
          node.vx = 0;
          node.vy = 0;
          node.vz = 0;
          continue;
        }
        node.vx -= node.x * centerPull;
        node.vy -= node.y * centerPull;
        node.vz -= node.z * centerPull;

        if (clusterMode) {
          const attractor = clusterPositions.get(node.type);
          if (attractor) {
            node.vx += (attractor[0] - node.x) * 0.006;
            node.vy += (attractor[1] - node.y) * 0.006;
            node.vz += (attractor[2] - node.z) * 0.006;
          }
        }

        if (selectedNodeId && node.id === selectedNodeId) {
          node.vx *= 0.82;
          node.vy *= 0.82;
          node.vz *= 0.82;
        }

        node.vx *= damping;
        node.vy *= damping;
        node.vz *= damping;

        node.x += node.vx * 0.12;
        node.y += node.vy * 0.12;
        node.z += node.vz * 0.12;
      }
    };

    const project = (x: number, y: number, z: number) => {
      const { yaw, pitch, zoom } = rotationRef.current;
      const cosY = Math.cos(yaw);
      const sinY = Math.sin(yaw);
      const cosP = Math.cos(pitch);
      const sinP = Math.sin(pitch);

      const rx = x * cosY - z * sinY;
      const rz = x * sinY + z * cosY;
      const ry = y * cosP - rz * sinP;
      const rz2 = y * sinP + rz * cosP;

      const camera = 620 / zoom;
      const perspective = camera / (camera - rz2);
      return {
        sx: size.width / 2 + rx * perspective,
        sy: size.height / 2 + ry * perspective,
        depth: rz2,
        scale: perspective,
      };
    };

    const edgeKey = (a: string, b: string) => `${a}|${b}`;

    const render = () => {
      context.clearRect(0, 0, size.width, size.height);

      const background = context.createRadialGradient(
        size.width * 0.3,
        size.height * 0.2,
        40,
        size.width * 0.5,
        size.height * 0.5,
        Math.max(size.width, size.height),
      );
      background.addColorStop(0, "rgba(18,30,45,0.88)");
      background.addColorStop(0.55, "rgba(8,13,19,0.96)");
      background.addColorStop(1, "rgba(4,7,11,1)");
      context.fillStyle = background;
      context.fillRect(0, 0, size.width, size.height);

      context.globalAlpha = 0.16;
      for (let i = 0; i < 180; i += 1) {
        const px = (i * 53) % size.width;
        const py = (i * 97) % size.height;
        context.fillStyle = "rgba(180,204,234,0.22)";
        context.fillRect(px, py, 1, 1);
      }
      context.globalAlpha = 1;

      const projections: Record<string, ProjectedNode> = {};
      const projectedNodes: ProjectedNode[] = [];

      for (const node of nodesForPhysics) {
        const p = project(node.x, node.y, node.z);
        let radius = Math.max(4, Math.min(15, 5 + Math.log2(1 + node.degree + node.baseWeight) * p.scale));
        if (node.type === "account") radius = Math.max(radius, 16);
        if (node.type === "commenter" && node.label.startsWith("branch_")) radius = Math.max(radius, 10);
        const out = { id: node.id, sx: p.sx, sy: p.sy, scale: p.scale, depth: p.depth, radius };
        projections[node.id] = out;
        projectedNodes.push(out);
      }

      projectedNodes.sort((a, b) => a.depth - b.depth);
      projectedRef.current = projectedNodes;

      for (const edge of edgesForPhysics) {
        const s = projections[edge.source];
        const t = projections[edge.target];
        if (!s || !t) continue;

        const meta = EDGE_LABEL[edge.type] ? edge.type : "grouped";
        const color =
          edge.type === "related_to"
            ? "#2dd4bf"
            : edge.type === "flagged_by"
              ? "#ff647c"
              : edge.type === "linked_to"
                ? "#e9c46a"
                : "#6fb9ff";
        const pathHit = highlightedPathEdges.has(edgeKey(edge.source, edge.target)) || highlightedPathEdges.has(edgeKey(edge.target, edge.source));

        context.beginPath();
        context.moveTo(s.sx, s.sy);
        context.lineTo(t.sx, t.sy);
        context.strokeStyle = pathHit ? "rgba(255,226,121,0.95)" : color;
        context.setLineDash(edge.type === "related_to" ? [4, 4] : []);
        context.globalAlpha = pathHit ? 1 : Math.max(0.2, Math.min(0.78, (s.scale + t.scale) * 0.3));
        context.lineWidth =
          pathHit
            ? 2.6
            : edge.type === "related_to"
              ? 1.4
              : Math.min(2.3, 0.7 + edge.weight * 0.34 + (meta === "flagged_by" ? 0.7 : 0));
        context.shadowBlur = pathHit ? 20 : 10;
        context.shadowColor = pathHit ? "rgba(255,218,113,0.82)" : color;
        context.stroke();
        context.setLineDash([]);
      }

      context.globalAlpha = 1;
      context.shadowBlur = 0;

      for (const item of projectedNodes) {
        const node = model.nodeMap[item.id];
        if (!node) continue;
        const meta = NODE_META[node.type] || NODE_META.summary;
        const riskyLevel = riskyNodeLevels[node.id];
        const palette = riskPalette(riskyNodeScores[node.id]);
        const nodeColor = riskyLevel ? palette.fill : meta.color;
        const selected = selectedNodeId === node.id;
        const hovered = hoveredId === node.id;
        const person = makePersonCardModel(node.label);
        const riskyScore = riskyNodeScores[node.id] || 0;

        if (node.type === "account") {
          const size = selected ? 86 : 58;
          const imageRadius = selected ? 31 : 21;
          if (selected) {
            context.beginPath();
            context.arc(item.sx, item.sy, size * 0.8, 0, Math.PI * 2);
            context.fillStyle = "rgba(130, 255, 246, 0.12)";
            context.fill();
          }
          drawRoundedRect(context, item.sx - size / 2, item.sy - size / 2, size, size, 8);
          context.fillStyle = "rgba(8,26,34,0.98)";
          context.shadowBlur = selected ? 46 : 24;
          context.shadowColor = "#26d7cf";
          context.fill();
          context.shadowBlur = 0;
          context.strokeStyle = selected ? "#9af6ff" : "rgba(52,235,224,0.9)";
          context.lineWidth = selected ? 3.4 : 2;
          context.stroke();
          context.fillStyle = "#74fff2";
          context.font = "bold 11px var(--font-jetbrains-mono), monospace";
          const accountImg = imageCacheRef.current[proxyAvatarUrl(node.avatarUrl)];
          if (accountImg) {
            context.save();
            context.beginPath();
            context.arc(item.sx, item.sy, imageRadius, 0, Math.PI * 2);
            context.closePath();
            context.clip();
            context.drawImage(accountImg, item.sx - imageRadius, item.sy - imageRadius, imageRadius * 2, imageRadius * 2);
            context.restore();
          } else {
            context.fillText(person.initials || "SRC", item.sx - 8, item.sy + 4);
          }

          drawRoundedRect(context, item.sx - 24, item.sy + size / 2 + 5, 48, 15, 5);
          context.fillStyle = "rgba(36,209,195,0.2)";
          context.fill();
          context.strokeStyle = "rgba(36,209,195,0.6)";
          context.lineWidth = 1;
          context.stroke();
          context.fillStyle = "rgba(123,247,237,0.95)";
          context.font = "bold 10px var(--font-jetbrains-mono), monospace";
          context.fillText("SOURCE", item.sx - 18, item.sy + size / 2 + 16);
        } else if (node.type === "commenter" && node.label.startsWith("branch_")) {
          const w = 26;
          const h = 18;
          drawRoundedRect(context, item.sx - w / 2, item.sy - h / 2, w, h, 6);
          context.fillStyle = "rgba(42,54,82,0.92)";
          context.fill();
          context.strokeStyle = "rgba(153,176,226,0.35)";
          context.lineWidth = 1;
          context.stroke();
          context.fillStyle = "rgba(201,218,248,0.95)";
          context.font = "bold 9px var(--font-jetbrains-mono), monospace";
          context.fillText(branchLeafLabel(node.label), item.sx - 10, item.sy + 3);
        } else if (node.type === "commenter" && riskyScore >= 5) {
          const w = selected ? 74 : 44;
          const h = selected ? 74 : 44;
          const imageRadius = selected ? 28 : 16;
          if (selected) {
            context.beginPath();
            context.arc(item.sx, item.sy, w * 0.88, 0, Math.PI * 2);
            context.fillStyle = riskyScore >= 7 ? "rgba(176,0,32,0.16)" : "rgba(249,115,22,0.16)";
            context.fill();
          }
          drawRoundedRect(context, item.sx - w / 2, item.sy - h / 2, w, h, 8);
          context.fillStyle = "rgba(11,18,34,0.96)";
          context.shadowBlur = selected ? 40 : 20;
          context.shadowColor = palette.glow;
          context.fill();
          context.shadowBlur = 0;
          context.strokeStyle = riskyScore >= 7 ? "rgba(255,120,135,0.95)" : "rgba(255,191,106,0.95)";
          context.lineWidth = selected ? 3.3 : 1.8;
          context.stroke();
          const commenterImg = imageCacheRef.current[proxyAvatarUrl(node.avatarUrl)];
          if (commenterImg) {
            context.save();
            context.beginPath();
            context.arc(item.sx, item.sy, imageRadius, 0, Math.PI * 2);
            context.closePath();
            context.clip();
            context.drawImage(commenterImg, item.sx - imageRadius, item.sy - imageRadius, imageRadius * 2, imageRadius * 2);
            context.restore();
          } else {
            context.fillStyle = riskyScore >= 7 ? "rgba(255,180,188,0.96)" : "rgba(255,222,171,0.96)";
            context.font = "bold 11px var(--font-jetbrains-mono), monospace";
            context.fillText(person.initials || "U", item.sx - 7, item.sy + 4);
          }
        } else {
          context.beginPath();
          context.arc(item.sx, item.sy, item.radius + 5, 0, Math.PI * 2);
          context.fillStyle = selected ? "rgba(255,232,144,0.26)" : hovered ? "rgba(160,198,255,0.22)" : "rgba(95,118,145,0.12)";
          context.fill();

          context.beginPath();
          context.arc(item.sx, item.sy, item.radius, 0, Math.PI * 2);
          const gradient = context.createRadialGradient(item.sx - item.radius * 0.3, item.sy - item.radius * 0.3, 1, item.sx, item.sy, item.radius);
          gradient.addColorStop(0, "#f6fbff");
          gradient.addColorStop(1, nodeColor);
          context.fillStyle = gradient;
          context.shadowBlur = selected ? 24 : 16;
          context.shadowColor = riskyLevel ? palette.glow : nodeColor;
          context.fill();
          context.shadowBlur = 0;
          context.strokeStyle = selected ? "#ffe383" : "rgba(214,230,248,0.5)";
          context.lineWidth = selected ? 2.1 : 1;
          context.stroke();
        }

        if (selected || hovered || node.type === "account" || (node.type === "commenter" && riskyScore >= 5)) {
          context.fillStyle = "rgba(226,235,246,0.95)";
          context.font = "11px var(--font-jetbrains-mono), monospace";
          context.fillText(node.label, item.sx + item.radius + 8, item.sy - item.radius - 4);
        }
      }

      frame = window.requestAnimationFrame(loop);
    };

    const loop = () => {
      physics();
      render();
    };

    loop();

    return () => {
      window.cancelAnimationFrame(frame);
    };
  }, [
    edgesForPhysics,
    highlightedPathEdges,
    hoveredId,
    model,
    nodesForPhysics,
    riskyNodeLevels,
    riskyNodeScores,
    selectedNodeId,
    size.height,
    size.width,
    clusterMode,
  ]);

  return (
    <div
      ref={wrapRef}
      className="relative h-full min-h-[520px] overflow-hidden rounded-[1.6rem] border border-[var(--border-default)]"
      onMouseDown={(event) => {
        const rect = (event.currentTarget as HTMLDivElement).getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        let picked: { id: string; d: number } | null = null;
        for (const node of projectedRef.current) {
          const dx = node.sx - x;
          const dy = node.sy - y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d <= node.radius + 8 && (!picked || d < picked.d)) {
            picked = { id: node.id, d };
          }
        }
        if (picked) {
          nodeDragRef.current = { id: picked.id, x: event.clientX, y: event.clientY };
          const node = nodesForPhysics.find((item) => item.id === picked.id);
          if (node) node.pinned = true;
          return;
        }
        dragRef.current = { active: true, x: event.clientX, y: event.clientY };
      }}
      onMouseUp={() => {
        dragRef.current.active = false;
        nodeDragRef.current.id = null;
      }}
      onMouseLeave={() => {
        dragRef.current.active = false;
        nodeDragRef.current.id = null;
        setHoveredId(null);
      }}
      onMouseMove={(event) => {
        const rect = (event.currentTarget as HTMLDivElement).getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;

        if (nodeDragRef.current.id) {
          const node = nodesForPhysics.find((item) => item.id === nodeDragRef.current.id);
          if (!node) return;
          const dx = event.clientX - nodeDragRef.current.x;
          const dy = event.clientY - nodeDragRef.current.y;
          const scale = 1 / Math.max(0.65, rotationRef.current.zoom);
          const yaw = rotationRef.current.yaw;
          const pitch = rotationRef.current.pitch;
          const rightX = Math.cos(yaw);
          const rightZ = -Math.sin(yaw);
          const forwardX = Math.sin(yaw);
          const forwardZ = Math.cos(yaw);
          const upY = Math.cos(pitch);
          const upForward = -Math.sin(pitch);
          const rightStep = dx * scale;
          const upStep = dy * scale;

          node.x += rightX * rightStep + forwardX * upForward * upStep;
          node.y += upY * upStep;
          node.z += rightZ * rightStep + forwardZ * upForward * upStep;
          node.vx = 0;
          node.vy = 0;
          node.vz = 0;
          node.pinned = true;
          nodeDragRef.current.x = event.clientX;
          nodeDragRef.current.y = event.clientY;
          return;
        }

        if (dragRef.current.active) {
          const dx = event.clientX - dragRef.current.x;
          const dy = event.clientY - dragRef.current.y;
          rotationRef.current.yaw += dx * 0.006;
          rotationRef.current.pitch = Math.max(-1.1, Math.min(1.1, rotationRef.current.pitch + dy * 0.004));
          dragRef.current.x = event.clientX;
          dragRef.current.y = event.clientY;
          return;
        }

        let nearest: { id: string; d: number } | null = null;
        for (const node of projectedRef.current) {
          const dx = node.sx - x;
          const dy = node.sy - y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d <= node.radius + 7 && (!nearest || d < nearest.d)) nearest = { id: node.id, d };
        }
        setHoveredId(nearest?.id || null);
      }}
      onClick={(event) => {
        const rect = (event.currentTarget as HTMLDivElement).getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;

        let picked: { id: string; d: number } | null = null;
        for (const node of projectedRef.current) {
          const dx = node.sx - x;
          const dy = node.sy - y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d <= node.radius + 8 && (!picked || d < picked.d)) {
            picked = { id: node.id, d };
          }
        }

        if (picked) onNodeClick(picked.id);
      }}
      onDoubleClick={(event) => {
        const rect = (event.currentTarget as HTMLDivElement).getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        let picked: { id: string; d: number } | null = null;
        for (const node of projectedRef.current) {
          const dx = node.sx - x;
          const dy = node.sy - y;
          const d = Math.sqrt(dx * dx + dy * dy);
          if (d <= node.radius + 8 && (!picked || d < picked.d)) {
            picked = { id: node.id, d };
          }
        }
        if (!picked) return;
        const simNode = nodesForPhysics.find((item) => item.id === picked.id);
        if (!simNode) return;
        simNode.pinned = !simNode.pinned;
        if (simNode.pinned) {
          simNode.vx = 0;
          simNode.vy = 0;
          simNode.vz = 0;
        }
      }}
      onWheel={(event) => {
        event.preventDefault();
        const zoom = Math.max(0.65, Math.min(1.85, rotationRef.current.zoom + event.deltaY * -0.0008));
        rotationRef.current.zoom = zoom;
      }}
    >
      <canvas ref={canvasRef} className="h-full w-full" />
    </div>
  );
}

export function RelationshipGraph({
  title,
  graph,
  reviewQueueItems = [],
  accountId,
  initialAnalysisModel,
  initialAnalysisUpdatedAt,
  initialCaptureUpdatedAt,
}: {
  title: string;
  graph: GraphData;
  reviewQueueItems?: ReviewQueueItem[];
  accountId?: number;
  initialAnalysisModel?: string | null;
  initialAnalysisUpdatedAt?: string | null;
  initialCaptureUpdatedAt?: string | null;
}) {
  const model = useMemo(() => makeGraphModel(graph), [graph]);
  const defaultAccountNodeId = useMemo(
    () => model.nodes.find((node) => node.type === "account")?.id || model.nodes[0]?.id || null,
    [model.nodes],
  );

  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(defaultAccountNodeId);
  const [overlayOpen, setOverlayOpen] = useState(false);
  const [riskFloor, setRiskFloor] = useState<"all" | PersonRisk["level"]>("medium");
  const [topSlice, setTopSlice] = useState<"all" | "top5" | "top10" | "top20">("top10");

  const [analysisModel, setAnalysisModel] = useState(initialAnalysisModel || "");
  const [analysisUpdatedAt, setAnalysisUpdatedAt] = useState(initialAnalysisUpdatedAt || "");
  const [analysisError, setAnalysisError] = useState("");
  const [isAnalyzing, startAnalysis] = useTransition();

  const [captureUpdatedAt, setCaptureUpdatedAt] = useState(initialCaptureUpdatedAt || "");
  const [captureError, setCaptureError] = useState("");
  const [isSavingCapture, startSavingCapture] = useTransition();

  const graphCaptureRef = useRef<HTMLDivElement | null>(null);

  const filteredEdges = useMemo(() => model.edges, [model.edges]);

  const connectedNodeIds = useMemo(() => {
    const active = new Set<string>();
    for (const edge of filteredEdges) {
      active.add(edge.source);
      active.add(edge.target);
    }
    for (const node of model.nodes) {
      if (model.typeCounts[node.type] === 1) active.add(node.id);
    }
    return active;
  }, [filteredEdges, model.nodes, model.typeCounts]);

  const personRiskRanking = useMemo(() => {
    return buildPersonRiskRanking(model, filteredEdges, connectedNodeIds);
  }, [connectedNodeIds, filteredEdges, model]);

  const openReviewQueueByUsername = useMemo(() => {
    const entries = reviewQueueItems.filter((item) => (item.status || "open").toLowerCase() === "open");
    return Object.fromEntries(entries.map((item) => [normalizeUsername(item.commenter_username), item])) as Record<
      string,
      ReviewQueueItem
    >;
  }, [reviewQueueItems]);

  const reviewQueueNodeIds = useMemo(() => {
    const ids = new Set<string>();
    for (const node of model.nodes) {
      if (node.type !== "commenter") continue;
      if (openReviewQueueByUsername[normalizeUsername(node.label)]) ids.add(node.id);
    }
    return ids;
  }, [model.nodes, openReviewQueueByUsername]);

  const reviewQueueRiskRanking = useMemo(() => {
    return personRiskRanking.filter((item) => reviewQueueNodeIds.has(item.id));
  }, [personRiskRanking, reviewQueueNodeIds]);

  const prioritizedReviewQueueRiskRanking = useMemo(() => {
    const floorRank = riskFloor === "all" ? 0 : riskLevelRank(riskFloor);
    let ranked = reviewQueueRiskRanking
      .filter((item) => item.score >= 5)
      .filter((item) => riskLevelRank(item.level) >= floorRank)
      .sort((a, b) => b.score - a.score);

    if (topSlice === "top5") ranked = ranked.slice(0, 5);
    else if (topSlice === "top10") ranked = ranked.slice(0, 10);
    else if (topSlice === "top20") ranked = ranked.slice(0, 20);

    return ranked;
  }, [reviewQueueRiskRanking, riskFloor, topSlice]);

  const relatedBranchAnchorRanking = useMemo(() => {
    const floorRank = riskFloor === "all" ? 0 : riskLevelRank(riskFloor);
    const relatedNodeIds = new Set<string>();
    for (const edge of filteredEdges) {
      if (edge.type !== "related_to") continue;
      relatedNodeIds.add(edge.source);
      relatedNodeIds.add(edge.target);
    }
    return personRiskRanking
      .filter((item) => item.score >= 5)
      .filter((item) => riskLevelRank(item.level) >= floorRank)
      .filter((item) => relatedNodeIds.has(item.id))
      .sort((a, b) => b.score - a.score);
  }, [filteredEdges, personRiskRanking, riskFloor]);

  const prioritizedVisibleRiskRanking = useMemo(() => {
    const merged = [...prioritizedReviewQueueRiskRanking];
    const seen = new Set(merged.map((item) => item.id));
    for (const item of relatedBranchAnchorRanking) {
      if (seen.has(item.id)) continue;
      merged.push(item);
      seen.add(item.id);
    }
    return merged.sort((a, b) => b.score - a.score);
  }, [prioritizedReviewQueueRiskRanking, relatedBranchAnchorRanking]);

  const personRiskMap = useMemo(() => {
    return Object.fromEntries(personRiskRanking.map((item) => [item.id, item])) as Record<string, PersonRisk>;
  }, [personRiskRanking]);

  const visibleNodeIds = useMemo(() => {
    const ids = new Set<string>();
    for (const node of model.nodes) {
      if (node.type === "account") ids.add(node.id);
    }
    const anchorIds = new Set<string>();
    for (const item of prioritizedVisibleRiskRanking) {
      ids.add(item.id);
      anchorIds.add(item.id);
    }
    for (const edge of filteredEdges) {
      if (edge.type !== "related_to") continue;
      if (anchorIds.has(edge.source)) ids.add(edge.target);
      if (anchorIds.has(edge.target)) ids.add(edge.source);
    }
    return ids;
  }, [model.nodes, prioritizedVisibleRiskRanking, filteredEdges]);

  const visibleEdges = useMemo(() => {
    return filteredEdges.filter(
      (edge) =>
        visibleNodeIds.has(edge.source) &&
        visibleNodeIds.has(edge.target) &&
        (edge.type === "commented_on" || edge.type === "related_to"),
    );
  }, [filteredEdges, visibleNodeIds]);

  const effectiveSelectedNodeId =
    selectedNodeId && visibleNodeIds.has(selectedNodeId)
      ? selectedNodeId
      : (prioritizedReviewQueueRiskRanking[0]?.id || defaultAccountNodeId || null);

  const selectedNode =
    effectiveSelectedNodeId && model.nodeMap[effectiveSelectedNodeId]
      ? model.nodeMap[effectiveSelectedNodeId]
      : undefined;
  const selectedPersonRisk = selectedNode ? personRiskMap[selectedNode.id] : undefined;
  const selectedReviewQueueItem = useMemo(() => {
    if (!selectedNode || selectedNode.type !== "commenter") return undefined;
    return openReviewQueueByUsername[normalizeUsername(selectedNode.label)];
  }, [openReviewQueueByUsername, selectedNode]);
  const selectedPersonCard = useMemo(() => {
    if (!selectedNode || selectedNode.type !== "commenter") return null;
    const modelCard = makePersonCardModel(selectedNode.label);
    const avatar = proxyAvatarUrl(model.nodeMap[selectedNode.id]?.avatarUrl) || modelCard.avatarUrl;
    return {
      ...modelCard,
      avatarUrl: avatar,
    };
  }, [selectedNode, model.nodeMap]);
  const selectedDangerNarrative = useMemo(() => {
    if (!selectedNode || selectedNode.type !== "commenter") return "";
    return buildDangerNarrative(selectedNode.label, selectedPersonRisk, selectedReviewQueueItem);
  }, [selectedNode, selectedPersonRisk, selectedReviewQueueItem]);

  const riskyVisiblePeople = useMemo(() => {
    return [...prioritizedVisibleRiskRanking].sort((a, b) => b.score - a.score);
  }, [prioritizedVisibleRiskRanking]);
  const riskyNodeLevels = useMemo(() => {
    return Object.fromEntries(riskyVisiblePeople.map((item) => [item.id, item.level])) as Record<string, PersonRisk["level"]>;
  }, [riskyVisiblePeople]);
  const riskyNodeScores = useMemo(() => {
    return Object.fromEntries(riskyVisiblePeople.map((item) => [item.id, item.score])) as Record<string, number>;
  }, [riskyVisiblePeople]);

  async function captureGraphImage() {
    const node = graphCaptureRef.current;
    if (!node) return "";
    const width = Math.max(node.clientWidth * 2.2, 2200);
    const height = Math.max(node.clientHeight * 2.2, 1400);
    return toPng(node, {
      cacheBust: true,
      pixelRatio: 2,
      backgroundColor: "#05080d",
      canvasWidth: width,
      canvasHeight: height,
      skipAutoScale: true,
      filter: (domNode) => {
        if (!(domNode instanceof HTMLElement)) return true;
        return !domNode.dataset.excludeCapture;
      },
    });
  }

  const graphStats = {
    nodes: visibleNodeIds.size,
    edges: visibleEdges.length,
    hiddenNodes: Math.max(0, model.nodes.length - visibleNodeIds.size),
    hiddenEdges: Math.max(0, model.edges.length - visibleEdges.length),
  };
  const highlightedPathEdges = useMemo(() => new Set<string>(), []);
  const showGraphPopup = Boolean(selectedNode && selectedNode.type === "commenter");

  return (
    <>
      <AppPanel className="h-full min-h-[760px] overflow-hidden">
        <div className="mb-4">
          <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">{title}</div>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[var(--text-secondary)]">
            Hesap, örgüt, tehdit ve yorumcu ilişkilerini canlı ağ üzerinde izleyin.
          </p>
        </div>

        <div className="grid min-h-[700px] gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
          <div ref={graphCaptureRef} className="relative min-h-[680px]">
            <button
              type="button"
              onClick={() => setOverlayOpen(true)}
              data-exclude-capture="true"
              className="absolute right-4 top-4 z-10 inline-flex h-10 items-center gap-2 rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.86)] px-3 text-xs font-medium text-[var(--text-primary)] transition hover:bg-[var(--bg-hover)]"
            >
              <Expand size={14} />
              tam ekran grafik
            </button>

            {showGraphPopup ? (
              <div
                data-exclude-capture="true"
                className="absolute left-4 top-4 z-10 w-[340px] max-w-[calc(100%-1rem)] rounded-2xl border border-[rgba(255,120,120,0.28)] bg-[rgba(8,14,22,0.92)] p-3 shadow-[0_18px_42px_rgba(0,0,0,0.45)] backdrop-blur-sm"
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-muted)]">Seçili Kişi</div>
                  {selectedPersonRisk ? (
                    <span
                      data-mono="true"
                      className={`inline-flex min-h-6 items-center rounded-[12px] border px-2.5 py-1 text-[12px] font-semibold tracking-[0.04em] ${
                        riskPalette(selectedPersonRisk.score).badgeClass
                      }`}
                    >
                      {selectedPersonRisk.score.toFixed(1)}
                    </span>
                  ) : null}
                </div>
                <div className="mt-2 flex items-center gap-3">
                  <div
                    className="relative h-11 w-11 overflow-hidden rounded-full border border-[rgba(255,255,255,0.22)]"
                    style={{ background: selectedPersonCard?.gradient || "linear-gradient(135deg,#4b5563,#111827)" }}
                  >
                    {selectedPersonCard?.avatarUrl ? (
                      // eslint-disable-next-line @next/next/no-img-element
                      <img
                        src={selectedPersonCard.avatarUrl}
                        alt={selectedPersonCard.username}
                        className="h-full w-full object-cover"
                        onError={(event) => {
                          event.currentTarget.src = proxyAvatarUrl(DEMO_PROFILE_PHOTOS[0]);
                        }}
                      />
                    ) : null}
                    <span className="pointer-events-none absolute inset-0 flex items-center justify-center text-sm font-bold text-white [text-shadow:0_1px_4px_rgba(0,0,0,0.65)]">
                      {selectedPersonCard?.initials || "U"}
                    </span>
                  </div>
                  <div>
                    <div className="text-sm font-semibold text-[var(--text-primary)]">@{selectedPersonCard?.username || selectedNode?.label}</div>
                    <div className="mt-1">
                      <StatusBadge
                        tone={
                          selectedPersonRisk?.level === "critical"
                            ? "danger"
                            : selectedPersonRisk?.level === "high"
                              ? "warning"
                              : "neutral"
                        }
                      >
                        {selectedPersonRisk ? riskLevelLabel(selectedPersonRisk.level) : "belirsiz"}
                      </StatusBadge>
                    </div>
                  </div>
                </div>
                <div className="mt-3 rounded-xl border border-[rgba(255,120,120,0.24)] bg-[rgba(255,70,70,0.08)] p-2.5 text-xs leading-5 text-[var(--text-primary)]">
                  {selectedDangerNarrative}
                </div>
              </div>
            ) : null}

            <ForceGraphCanvas
              model={model}
              visibleNodeIds={visibleNodeIds}
              visibleEdges={visibleEdges}
              selectedNodeId={selectedNode?.id}
              clusterMode={false}
              highlightedPathEdges={highlightedPathEdges}
              riskyNodeLevels={riskyNodeLevels}
              riskyNodeScores={riskyNodeScores}
              onNodeClick={setSelectedNodeId}
            />
          </div>

          <aside className="flex min-h-0 flex-col rounded-[1.6rem] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)]">
            <div className="border-b border-[var(--border-subtle)] p-5">
              <div className="text-xs uppercase tracking-[0.24em] text-[var(--text-muted)]">Genişletilmiş Soruşturma</div>
              <div className="mt-2 text-2xl font-semibold text-[var(--text-primary)]">{selectedNode?.label || "Düğüm seçilmedi"}</div>
              <div className="mt-4 flex flex-wrap gap-2">
                <StatusBadge tone="accent">{graphStats.nodes} düğüm</StatusBadge>
                <StatusBadge tone="warning">{graphStats.edges} bağ</StatusBadge>
                <StatusBadge tone="neutral">{graphStats.hiddenNodes} gizli düğüm</StatusBadge>
                <StatusBadge tone="danger">{graphStats.hiddenEdges} gizli bağ</StatusBadge>
              </div>
            </div>

            <div className="space-y-4 p-5">
              <div>
                <FieldLabel>Minimum risk</FieldLabel>
                <SelectControl value={riskFloor} onChange={(event) => setRiskFloor(event.target.value as "all" | PersonRisk["level"])}>
                  <option value="all">Tümü</option>
                  <option value="low">Düşük ve üstü</option>
                  <option value="medium">Orta ve üstü</option>
                  <option value="high">Yüksek ve üstü</option>
                  <option value="critical">Sadece kritik</option>
                </SelectControl>
              </div>
              <div>
                <FieldLabel>Puan filtresi</FieldLabel>
                <SelectControl value={topSlice} onChange={(event) => setTopSlice(event.target.value as "all" | "top5" | "top10" | "top20")}>
                  <option value="top5">En yüksek 5</option>
                  <option value="top10">En yüksek 10</option>
                  <option value="top20">En yüksek 20</option>
                  <option value="all">Hepsi</option>
                </SelectControl>
              </div>
              <div className="text-xs text-[var(--text-muted)]">Filtre sonrası {riskyVisiblePeople.length} kişi gösteriliyor.</div>
            </div>

            <div className="min-h-0 flex-1 space-y-2 overflow-y-auto overflow-x-hidden px-5 pb-5 pr-4">
              {riskyVisiblePeople.map((item) => (
                <button
                  key={`risk-${item.id}`}
                  type="button"
                  onClick={() => setSelectedNodeId(item.id)}
                  className="w-full rounded-xl border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.03)] px-3 py-2 text-left text-sm hover:border-[var(--accent-border)]"
                >
                  <div className="flex min-w-0 items-center justify-between gap-2">
                    <div className="flex min-w-0 flex-1 items-center gap-2">
                      <div className="h-7 w-7 overflow-hidden rounded-full border border-[rgba(255,255,255,0.2)]">
                        {/* eslint-disable-next-line @next/next/no-img-element */}
                        <img
                          src={proxyAvatarUrl(model.nodeMap[item.id]?.avatarUrl) || pickDemoPhoto(model.nodeMap[item.id]?.label || item.id)}
                          alt={model.nodeMap[item.id]?.label || item.id}
                          className="h-full w-full object-cover"
                          onError={(event) => {
                            event.currentTarget.src = proxyAvatarUrl(DEMO_PROFILE_PHOTOS[0]);
                          }}
                        />
                      </div>
                      <span className="block truncate text-[var(--text-primary)]">@{model.nodeMap[item.id]?.label || item.id}</span>
                    </div>
                    <div className="flex shrink-0 items-center gap-2">
                      <span
                        data-mono="true"
                        className={`inline-flex min-h-6 items-center rounded-[12px] border px-2.5 py-1 text-[12px] font-semibold tracking-[0.04em] ${
                          riskPalette(item.score).badgeClass
                        }`}
                      >
                        {item.score.toFixed(1)}
                      </span>
                      <StatusBadge tone={item.level === "critical" ? "danger" : item.level === "high" ? "warning" : "neutral"}>
                        {riskLevelLabel(item.level)}
                      </StatusBadge>
                    </div>
                  </div>
                </button>
              ))}
              {!riskyVisiblePeople.length ? (
                <div className="rounded-xl border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] px-3 py-3 text-xs text-[var(--text-muted)]">
                  Bu filtrede sonuç yok.
                </div>
              ) : null}
            </div>

            <div className="border-t border-[var(--border-subtle)] p-5">
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  disabled={isSavingCapture}
                  onClick={() => {
                    setCaptureError("");
                    startSavingCapture(async () => {
                      try {
                        const graphImageDataUrl = await captureGraphImage();
                        const result = await saveAccountGraphCapture(accountId || 0, { graph_image_data_url: graphImageDataUrl });
                        setCaptureUpdatedAt(result.updated_at || "");
                      } catch (error) {
                        setCaptureError(error instanceof Error ? error.message : "Grafik görüntüsü kaydedilemedi.");
                      }
                    });
                  }}
                >
                  {isSavingCapture ? "PNG kaydediliyor..." : "PNG Kaydet"}
                </Button>
                <Button
                  type="button"
                  disabled={isAnalyzing}
                  tone="primary"
                  onClick={() => {
                    setAnalysisError("");
                    startAnalysis(async () => {
                      try {
                        const graphImageDataUrl = await captureGraphImage();
                        const result = await runAccountGraphAnalysis(accountId || 0, {
                          graph_image_data_url: graphImageDataUrl || undefined,
                        });
                        setAnalysisModel(result.model);
                        setAnalysisUpdatedAt(result.updated_at || "");
                      } catch (error) {
                        setAnalysisError(error instanceof Error ? error.message : "Yapay zeka çözümlemesi başarısız oldu.");
                      }
                    });
                  }}
                >
                  {isAnalyzing ? "Çözümleme..." : "AI Çözümle"}
                </Button>
              </div>
              {(analysisError || captureError) ? (
                <div className="mt-3 rounded-xl border border-[rgba(255,100,124,0.28)] bg-[rgba(255,100,124,0.1)] px-3 py-2 text-xs text-[#ffc1cb]">
                  {analysisError || captureError}
                </div>
              ) : null}
              {(analysisUpdatedAt || captureUpdatedAt || analysisModel) ? (
                <div className="mt-3 flex flex-wrap gap-2">
                  {analysisModel ? <StatusBadge tone="accent">{analysisModel}</StatusBadge> : null}
                  {analysisUpdatedAt ? <StatusBadge tone="neutral">analiz {analysisUpdatedAt}</StatusBadge> : null}
                  {captureUpdatedAt ? <StatusBadge tone="neutral">png {captureUpdatedAt}</StatusBadge> : null}
                </div>
              ) : null}
            </div>
          </aside>
        </div>
      </AppPanel>

      {overlayOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-[rgba(3,6,10,0.86)] p-3 xl:p-5" onClick={() => setOverlayOpen(false)}>
          <div
            className="grid h-[94vh] w-full max-w-[98vw] gap-4 rounded-[2rem] border border-[var(--border-default)] bg-[var(--bg-base)] p-4 shadow-[var(--shadow-overlay)] xl:grid-cols-[minmax(0,1fr)_360px] xl:p-5"
            onClick={(event) => event.stopPropagation()}
          >
            <div className="relative">
              {showGraphPopup ? (
                <div
                  data-exclude-capture="true"
                  className="absolute left-4 top-4 z-10 w-[340px] max-w-[calc(100%-1rem)] rounded-2xl border border-[rgba(255,120,120,0.28)] bg-[rgba(8,14,22,0.92)] p-3 shadow-[0_18px_42px_rgba(0,0,0,0.45)] backdrop-blur-sm"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-[11px] uppercase tracking-[0.2em] text-[var(--text-muted)]">Seçili Kişi</div>
                    {selectedPersonRisk ? (
                      <span
                        data-mono="true"
                        className={`inline-flex min-h-6 items-center rounded-[12px] border px-2.5 py-1 text-[12px] font-semibold tracking-[0.04em] ${
                          riskPalette(selectedPersonRisk.score).badgeClass
                        }`}
                      >
                        {selectedPersonRisk.score.toFixed(1)}
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-2 flex items-center gap-3">
                    <div
                      className="relative h-11 w-11 overflow-hidden rounded-full border border-[rgba(255,255,255,0.22)]"
                      style={{ background: selectedPersonCard?.gradient || "linear-gradient(135deg,#4b5563,#111827)" }}
                    >
                      {selectedPersonCard?.avatarUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element
                        <img
                          src={selectedPersonCard.avatarUrl}
                          alt={selectedPersonCard.username}
                          className="h-full w-full object-cover"
                          onError={(event) => {
                            event.currentTarget.src = proxyAvatarUrl(DEMO_PROFILE_PHOTOS[0]);
                          }}
                        />
                      ) : null}
                      <span className="pointer-events-none absolute inset-0 flex items-center justify-center text-sm font-bold text-white [text-shadow:0_1px_4px_rgba(0,0,0,0.65)]">
                        {selectedPersonCard?.initials || "U"}
                      </span>
                    </div>
                    <div>
                      <div className="text-sm font-semibold text-[var(--text-primary)]">@{selectedPersonCard?.username || selectedNode?.label}</div>
                      <div className="mt-1">
                        <StatusBadge
                          tone={
                            selectedPersonRisk?.level === "critical"
                              ? "danger"
                              : selectedPersonRisk?.level === "high"
                                ? "warning"
                                : "neutral"
                          }
                        >
                          {selectedPersonRisk ? riskLevelLabel(selectedPersonRisk.level) : "belirsiz"}
                        </StatusBadge>
                      </div>
                    </div>
                  </div>
                  <div className="mt-3 rounded-xl border border-[rgba(255,120,120,0.24)] bg-[rgba(255,70,70,0.08)] p-2.5 text-xs leading-5 text-[var(--text-primary)]">
                    {selectedDangerNarrative}
                  </div>
                </div>
              ) : null}

              <ForceGraphCanvas
                model={model}
                visibleNodeIds={visibleNodeIds}
                visibleEdges={visibleEdges}
                selectedNodeId={selectedNode?.id}
                clusterMode={false}
                highlightedPathEdges={highlightedPathEdges}
                riskyNodeLevels={riskyNodeLevels}
                riskyNodeScores={riskyNodeScores}
                onNodeClick={setSelectedNodeId}
              />
            </div>

            <div className="flex min-h-0 flex-col rounded-[1.75rem] border border-[var(--border-default)] bg-[rgba(255,255,255,0.02)] p-5">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-xs uppercase tracking-[0.24em] text-[var(--text-muted)]">Genişletilmiş Soruşturma</div>
                  <div className="mt-2 text-2xl font-semibold text-[var(--text-primary)]">{selectedNode?.label || "Düğüm seçilmedi"}</div>
                </div>
                <button
                  type="button"
                  onClick={() => setOverlayOpen(false)}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.88)] text-[var(--text-primary)] transition hover:bg-[var(--bg-hover)]"
                >
                  <X size={16} />
                </button>
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                <StatusBadge tone="accent">{graphStats.nodes} düğüm</StatusBadge>
                <StatusBadge tone="warning">{graphStats.edges} bağ</StatusBadge>
                <StatusBadge tone="neutral">{graphStats.hiddenNodes} gizli düğüm</StatusBadge>
                <StatusBadge tone="danger">{graphStats.hiddenEdges} gizli bağ</StatusBadge>
              </div>

              <div className="mt-5 space-y-3">
                <div>
                  <FieldLabel>Minimum risk</FieldLabel>
                  <SelectControl value={riskFloor} onChange={(event) => setRiskFloor(event.target.value as "all" | PersonRisk["level"])}>
                    <option value="all">Tümü</option>
                    <option value="low">Düşük ve üstü</option>
                    <option value="medium">Orta ve üstü</option>
                    <option value="high">Yüksek ve üstü</option>
                    <option value="critical">Sadece kritik</option>
                  </SelectControl>
                </div>
                <div>
                  <FieldLabel>Puan filtresi</FieldLabel>
                  <SelectControl value={topSlice} onChange={(event) => setTopSlice(event.target.value as "all" | "top5" | "top10" | "top20")}>
                    <option value="top5">En yüksek 5</option>
                    <option value="top10">En yüksek 10</option>
                    <option value="top20">En yüksek 20</option>
                    <option value="all">Hepsi</option>
                  </SelectControl>
                </div>
                <div className="text-xs text-[var(--text-muted)]">Filtre sonrası {riskyVisiblePeople.length} kişi gösteriliyor.</div>
              </div>

              <div className="mt-4 min-h-0 flex-1 space-y-2 overflow-y-auto overflow-x-hidden pr-2">
                {riskyVisiblePeople.map((item) => (
                  <button
                    key={`overlay-risk-${item.id}`}
                    type="button"
                    onClick={() => setSelectedNodeId(item.id)}
                    className="w-full rounded-xl border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.03)] px-3 py-2 text-left text-sm hover:border-[var(--accent-border)]"
                  >
                    <div className="flex min-w-0 items-center justify-between gap-2">
                      <div className="flex min-w-0 flex-1 items-center gap-2">
                        <div className="h-7 w-7 overflow-hidden rounded-full border border-[rgba(255,255,255,0.2)]">
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img
                            src={proxyAvatarUrl(model.nodeMap[item.id]?.avatarUrl) || pickDemoPhoto(model.nodeMap[item.id]?.label || item.id)}
                            alt={model.nodeMap[item.id]?.label || item.id}
                            className="h-full w-full object-cover"
                            onError={(event) => {
                              event.currentTarget.src = proxyAvatarUrl(DEMO_PROFILE_PHOTOS[0]);
                            }}
                          />
                        </div>
                        <span className="block truncate text-[var(--text-primary)]">@{model.nodeMap[item.id]?.label || item.id}</span>
                      </div>
                      <div className="flex shrink-0 items-center gap-2">
                        <span
                          data-mono="true"
                          className={`inline-flex min-h-6 items-center rounded-[12px] border px-2.5 py-1 text-[12px] font-semibold tracking-[0.04em] ${
                            riskPalette(item.score).badgeClass
                          }`}
                        >
                          {item.score.toFixed(1)}
                        </span>
                        <StatusBadge tone={item.level === "critical" ? "danger" : item.level === "high" ? "warning" : "neutral"}>
                          {riskLevelLabel(item.level)}
                        </StatusBadge>
                      </div>
                    </div>
                  </button>
                ))}
                {!riskyVisiblePeople.length ? (
                  <div className="rounded-xl border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] px-3 py-3 text-xs text-[var(--text-muted)]">
                    Bu filtrede sonuç yok.
                  </div>
                ) : null}
              </div>

              <div className="mt-3 text-xs text-[var(--text-muted)]">
                Bir düğüme tıklayarak alttaki tehlike nedeni panelini güncelleyebilirsin.
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
