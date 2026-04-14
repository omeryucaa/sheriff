import { ArrowUpRight, ShieldAlert } from "lucide-react";

import { BarPanel, PiePanel } from "@/components/charts";
import { AnalystListItem, AppPanel, MetricCard, PageHeader, SectionTitle, StatusBadge, getThreatTone } from "@/components/ui";
import { getDashboardSummary } from "@/lib/api";

export default async function HomePage() {
  const summary = await getDashboardSummary();

  return (
    <div className="min-w-0 space-y-5">
      <PageHeader
        eyebrow="Komuta Paneli"
        title="RedKid İstihbarat Merkezi"
        description="İncelenen hesaplar, tehdit sinyalleri ve ilişki yoğunluğu operasyonel öncelik sırasına göre tek sahnede toplanır."
        action={
          <div className="inline-flex h-11 items-center gap-3 rounded-[var(--radius-control)] border border-[var(--border-default)] bg-[rgba(255,255,255,0.03)] px-4 text-[13px] text-[var(--text-secondary)]">
            <span className="h-2.5 w-2.5 rounded-full bg-[var(--success)]" />
            Sistem aktif
          </div>
        }
      />

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <MetricCard label="İncelenen Hesap" value={summary.kpis.incelenen_hesap} hint="Aktif kapsam içindeki hesap sayısı" />
        <MetricCard label="İncelenen Post" value={summary.kpis.incelenen_post} hint="Toplanan ve özetlenen içerik" />
        <MetricCard label="İncelenen Yorum" value={summary.kpis.incelenen_yorum} hint="Analiz edilen yorum hacmi" />
        <MetricCard label="Açık İnceleme Kuyruğu" value={summary.kpis.acik_review_queue} hint="Analist aksiyonu bekleyen kayıtlar" tone="danger" />
        <MetricCard label="Baskın Örgüt" value={summary.kpis.baskin_orgut} hint="En yoğun sinyal kümesi" tone="warning" />
      </div>

      <div className="grid min-w-0 gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(420px,0.65fr)]">
        <AppPanel>
          <SectionTitle
            eyebrow="Öncelikli İnceleme"
            title="Öncelikli İnceleme Listesi"
            description="Yüksek tehdit, yorum yoğunluğu ve önem skoruna göre sıralanan hesaplar."
            action={<ShieldAlert className="h-5 w-5 text-[var(--warning)]" />}
          />
          <div className="mt-5 space-y-3">
            {summary.riskli_hesaplar.map((account, index) => (
              <AnalystListItem
                key={account.id}
                href={`/accounts/${account.id}`}
                selected={index === 0}
                title={<span data-mono="true">@{account.instagram_username}</span>}
                subtitle={`${account.tehdit_seviyesi || "belirsiz"} tehdit`}
                summary={account.profil_ozeti || account.account_profile_summary || "Profil özeti bulunmuyor."}
                status={<StatusBadge tone={getThreatTone(account.tehdit_seviyesi)}>önem {account.max_onem_skoru || 0}</StatusBadge>}
                meta={
                  <div className="space-y-1">
                    <div data-mono="true">{account.flagged_comment_count || 0} bayraklı yorum</div>
                    <div className="flex items-center justify-end gap-1 text-[var(--text-dim)]">
                      <ArrowUpRight className="h-3.5 w-3.5" />
                      vaka panosu
                    </div>
                  </div>
                }
              />
            ))}
          </div>
        </AppPanel>

        <div className="grid min-w-0 gap-6">
          <BarPanel title="Tehdit Dağılımı" data={summary.tehdit_dagilimi} />
          <PiePanel title="Kategori Dağılımı" data={summary.kategori_dagilimi} />
        </div>
      </div>

    </div>
  );
}
