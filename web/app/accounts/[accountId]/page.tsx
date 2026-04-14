import { RelationshipGraph } from "@/components/relationship-graph";
import { PostTimeline } from "@/components/post-timeline";
import {
  AppPanel,
  DataRow,
  DetailSidebar,
  MetricCard,
  PageHeader,
  SectionTitle,
  StatusBadge,
  getThreatTone,
} from "@/components/ui";
import { getAccountDetail, getAccountGraph, getAccountPosts } from "@/lib/api";

function normalizeThreatLevel(value?: string | null): "kritik" | "yuksek" | "orta" | "dusuk" | "yok" | "belirsiz" {
  const threat = String(value || "").trim().toLowerCase();
  if (threat === "kritik") return "kritik";
  if (threat === "yuksek") return "yuksek";
  if (threat === "orta") return "orta";
  if (threat === "dusuk") return "dusuk";
  if (threat === "yok") return "yok";
  return "belirsiz";
}

function buildInvestigationOutcome(detail: {
  tehdit_seviyesi?: string | null;
  flagged_comment_count: number;
  ortalama_onem_skoru: number;
}) {
  const threat = normalizeThreatLevel(detail.tehdit_seviyesi);
  const flagged = Number(detail.flagged_comment_count || 0);
  const avgImportance = Number(detail.ortalama_onem_skoru || 0);

  if (threat === "kritik" || flagged >= 5 || avgImportance >= 7) {
    return {
      label: "Acil Takip",
      tone: "danger" as const,
      note: "Hesap yüksek risk grubundadır. Operasyonel takip ve hızlı teyit önerilir.",
    };
  }
  if (threat === "yuksek" || flagged >= 1 || avgImportance >= 4) {
    return {
      label: "Öncelikli Takip",
      tone: "warning" as const,
      note: "Hesapta anlamlı risk sinyali vardır. Düzenli izleme ve dönemsel güncelleme önerilir.",
    };
  }
  return {
    label: "Rutin İzleme",
    tone: "neutral" as const,
    note: "Mevcut bulgular düşük yoğunluktadır. Standart izleme periyodu yeterlidir.",
  };
}

export default async function AccountDetailPage({
  params,
}: {
  params: Promise<{ accountId: string }>;
}) {
  const { accountId } = await params;
  const [detail, postsResponse, graph] = await Promise.all([
    getAccountDetail(accountId),
    getAccountPosts(accountId),
    getAccountGraph(accountId),
  ]);
  const posts = postsResponse.items;
  const outcome = buildInvestigationOutcome(detail);

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="Vaka Panosu"
        title={`@${detail.instagram_username}`}
        description="Seçili hesabın profil özeti, post zaman çizgisi ve ilişki haritası aynı analist çalışma alanında toplanır."
        action={<StatusBadge tone={getThreatTone(detail.tehdit_seviyesi)}>{detail.tehdit_seviyesi || "belirsiz"}</StatusBadge>}
      />

      <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1.25fr)_320px]">
        <AppPanel className="self-start">
          <SectionTitle eyebrow="Identity Snapshot" title="Hesap Kimliği ve Risk Özeti" />
          <div className="mt-5 grid items-start gap-5 xl:grid-cols-[minmax(0,1fr)_260px]">
            <div>
              <div data-mono="true" className="text-[28px] font-bold tracking-[-0.04em] text-[var(--text-primary)]">
                @{detail.instagram_username}
              </div>
              <p className="mt-3 max-w-3xl text-[15px] leading-7 text-[var(--text-secondary)]">{detail.bio || "Biyografi yok."}</p>
              <div className="mt-4 flex flex-wrap gap-2">
                <StatusBadge tone={getThreatTone(detail.tehdit_seviyesi)}>{detail.tehdit_seviyesi || "belirsiz"} tehdit</StatusBadge>
                <StatusBadge tone="warning">{detail.tespit_edilen_orgut || "belirsiz"} örgüt</StatusBadge>
                <StatusBadge tone="neutral">{detail.baskin_kategori} kategori</StatusBadge>
              </div>

            </div>

            <div className="rounded-[var(--radius-card)] border border-[var(--border-default)] bg-[rgba(10,17,26,0.82)] p-4 self-start">
              <DataRow label="Baskın Kategori" value={detail.baskin_kategori} />
              <DataRow label="Tespit Edilen Örgüt" value={detail.tespit_edilen_orgut || "belirsiz"} />
              <DataRow label="Tehdit Seviyesi" value={detail.tehdit_seviyesi || "belirsiz"} />
              <DataRow label="Ortalama Önem" value={detail.ortalama_onem_skoru} mono />
            </div>
          </div>

          <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <MetricCard label="Toplam Post" value={detail.post_count} />
            <MetricCard label="Toplam Yorum" value={detail.comment_count} />
            <MetricCard label="Kritik Yorum" value={detail.flagged_comment_count} tone="danger" />
            <MetricCard label="Ort. Önem" value={detail.ortalama_onem_skoru} tone="warning" />
          </div>
        </AppPanel>

        <DetailSidebar title="Kritik Sonuç" eyebrow="İstihbarat Çıktısı">
          <div className="rounded-[var(--radius-card)] border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] p-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">Risk Özeti</div>
            <div className="mt-3 space-y-2">
              <div className="flex items-center justify-between gap-3">
                <span className="text-[13px] text-[var(--text-secondary)]">Tehdit</span>
                <StatusBadge tone={getThreatTone(detail.tehdit_seviyesi)}>{detail.tehdit_seviyesi || "belirsiz"}</StatusBadge>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-[13px] text-[var(--text-secondary)]">Örgüt</span>
                <StatusBadge tone="warning">{detail.tespit_edilen_orgut || "belirsiz"}</StatusBadge>
              </div>
              <div className="flex items-center justify-between gap-3">
                <span className="text-[13px] text-[var(--text-secondary)]">Kategori</span>
                <StatusBadge tone="neutral">{detail.baskin_kategori}</StatusBadge>
              </div>
            </div>
          </div>
          <div className="rounded-[var(--radius-card)] border border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] p-4">
            <div className="flex items-center justify-between gap-3">
              <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-[var(--text-muted)]">Nihai Sonuç</div>
              <StatusBadge tone={outcome.tone}>{outcome.label}</StatusBadge>
            </div>
            <p className="mt-3 text-[14px] leading-7 text-[var(--text-secondary)]">{outcome.note}</p>
          </div>
        </DetailSidebar>
      </div>

      <AppPanel>
        <SectionTitle eyebrow="İstihbarat Özeti" title="İstihbarat Özeti" />
        <div className="mt-5 max-w-5xl">
          <p className="text-[15px] leading-8 text-[var(--text-secondary)]">{detail.account_profile_summary || "Profil özeti yok."}</p>
        </div>
      </AppPanel>

      <RelationshipGraph
        key={detail.id}
        title="İlişki Ağı"
        graph={graph}
        accountId={detail.id}
        initialAnalysis={detail.graph_ai_analysis}
        initialAnalysisModel={detail.graph_ai_analysis_model}
        initialAnalysisUpdatedAt={detail.graph_ai_analysis_updated_at}
        initialCapturePath={detail.graph_capture_path}
        initialCaptureUpdatedAt={detail.graph_capture_updated_at}
      />

      <PostTimeline posts={posts} />
    </div>
  );
}
