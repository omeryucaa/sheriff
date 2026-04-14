import { FilterBar, FilterField, FilterInput } from "@/components/filter-bar";
import { DenseTable, PageHeader, StatusBadge, getThreatTone } from "@/components/ui";
import { getReviewQueue } from "@/lib/api";

export default async function ReviewQueuePage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) || {};
  const queue = await getReviewQueue(typeof params.search === "string" ? params.search : undefined);

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="İnceleme Kuyruğu"
        title="Bayraklı Kullanıcılar"
        description="Tekrarlı tetiklenme, son gerekçe ve verdict tipine göre önceliklendirilen triage kuyruğu."
      />

      <FilterBar autoSubmit>
        <FilterField label="Kullanıcı / Gerekçe Ara">
          <FilterInput
            name="search"
            defaultValue={typeof params.search === "string" ? params.search : ""}
            placeholder="yorumcu adı veya gerekçe"
          />
        </FilterField>
      </FilterBar>

      <DenseTable
        items={queue.items}
        getKey={(item) => item.commenter_username}
        columns={[
          {
            key: "user",
            header: "Kullanıcı",
            render: (item) => (
              <div>
                <div data-mono="true" className="font-semibold text-[var(--text-primary)]">
                  @{item.commenter_username}
                </div>
                <div data-mono="true" className="mt-2 text-[12px] text-[var(--text-muted)]">
                  {item.last_triggered_at}
                </div>
              </div>
            ),
          },
          {
            key: "count",
            header: "Tetik",
            render: (item) => (
              <span data-mono="true" className="text-[13px] text-[var(--text-primary)]">
                {item.trigger_count}
              </span>
            ),
          },
          {
            key: "reason",
            header: "Karar / Tip",
            render: (item) => <StatusBadge tone="warning">{item.flag_reason_type || "belirsiz"}</StatusBadge>,
          },
          {
            key: "summary",
            header: "Son Gerekçe",
            render: (item) => <div className="text-[14px] leading-6 text-[var(--text-secondary)]">{item.last_reason || "-"}</div>,
          },
          {
            key: "status",
            header: "Durum",
            align: "right",
            render: (item) => <StatusBadge tone={getThreatTone(item.status)}>{item.status}</StatusBadge>,
          },
        ]}
      />
    </div>
  );
}
