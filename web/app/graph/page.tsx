import { RelationshipGraph } from "@/components/relationship-graph";
import { AppPanel, PageHeader, StatusBadge, getThreatTone } from "@/components/ui";
import { getAccounts, getAccountGraph, getReviewQueue } from "@/lib/api";

export default async function GraphPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) || {};
  const accounts = await getAccounts();
  const selectedAccount =
    typeof params.accountId === "string"
      ? accounts.items.find((item) => String(item.id) === params.accountId)
      : accounts.items[0];
  const graph = selectedAccount ? await getAccountGraph(selectedAccount.id) : { nodes: [], edges: [] };
  const reviewQueue = await getReviewQueue();

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="İlişki Grafiği"
        title="Bağlantı ve Etki Haritası"
        description="Hesap, örgüt, kategori, tehdit ve yorumcu bağlarını üç panelli inceleme düzeninde izleyin."
      />

      <AppPanel className="p-3">
        <div className="flex items-center justify-between gap-3">
          <div className="text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">Hesap Seçimi</div>
          {selectedAccount ? (
            <StatusBadge tone="accent">@{selectedAccount.instagram_username}</StatusBadge>
          ) : null}
        </div>
        <div className="mt-3 flex gap-2 overflow-x-auto pb-1">
          {accounts.items.map((account) => (
            <a
              key={account.id}
              href={`/graph?accountId=${account.id}`}
              className={`shrink-0 rounded-[var(--radius-control)] border px-3 py-2 text-sm transition ${
                selectedAccount?.id === account.id
                  ? "border-[var(--accent-border)] bg-[var(--accent-soft)] text-[var(--text-primary)] shadow-[var(--glow-selection)]"
                  : "border-[var(--border-subtle)] bg-[rgba(255,255,255,0.02)] text-[var(--text-secondary)] hover:bg-[rgba(255,255,255,0.03)]"
              }`}
            >
              <div data-mono="true" className="font-semibold leading-5">
                @{account.instagram_username}
              </div>
              <div className="mt-1.5 flex gap-1.5">
                <StatusBadge tone="warning">{account.tespit_edilen_orgut || "belirsiz"}</StatusBadge>
                <StatusBadge tone={getThreatTone(account.tehdit_seviyesi)}>{account.tehdit_seviyesi || "belirsiz"}</StatusBadge>
              </div>
            </a>
          ))}
        </div>
      </AppPanel>

      <div>
        <RelationshipGraph
          key={selectedAccount?.id || "graph"}
          title={`Graf Görünümü${selectedAccount ? ` · @${selectedAccount.instagram_username}` : ""}`}
          graph={graph}
          reviewQueueItems={reviewQueue.items}
          accountId={selectedAccount?.id}
          initialCapturePath={undefined}
          initialCaptureUpdatedAt={undefined}
        />
      </div>
    </div>
  );
}
