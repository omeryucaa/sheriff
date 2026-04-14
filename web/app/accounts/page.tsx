import Link from "next/link";

import { FilterBar, FilterField, FilterInput, FilterSelect } from "@/components/filter-bar";
import { AppPanel, PageHeader, StatusBadge, getThreatTone } from "@/components/ui";
import { getAccounts } from "@/lib/api";

function resolveDetectedOrg(account: {
  tespit_edilen_orgut?: string | null;
  account_profile_summary?: string | null;
  profil_ozeti?: string | null;
}) {
  const direct = String(account.tespit_edilen_orgut || "").trim();
  if (direct && direct !== "belirsiz") return direct;

  const text = `${account.account_profile_summary || ""} ${account.profil_ozeti || ""}`.toUpperCase();
  const matches: string[] = [];
  if (text.includes("PKK/KCK") || text.includes(" PKK ") || text.includes(" KCK ")) matches.push("PKK/KCK");
  if (text.includes("DHKP-C") || text.includes("DHKPC")) matches.push("DHKP-C");
  if (text.includes("FETÖ") || text.includes("FETO")) matches.push("FETÖ");
  if (text.includes("DEAŞ") || text.includes("DEAS") || text.includes("IŞİD") || text.includes("ISID") || text.includes("ISIS")) {
    matches.push("DEAŞ/IŞİD");
  }

  return matches.length ? matches.join(", ") : "belirsiz";
}

export default async function AccountsPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = (await searchParams) || {};
  const query = new URLSearchParams();
  if (typeof params.search === "string") query.set("search", params.search);
  if (typeof params.orgut === "string") query.set("orgut", params.orgut);
  if (typeof params.threat === "string") query.set("threat", params.threat);
  if (params.flagged_only === "true") query.set("flagged_only", "true");
  const response = await getAccounts(query.toString());

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="Hesap İndeksi"
        title="İncelenen Hesaplar"
        description="Risk, örgüt bağı, hacim ve özet sinyallerine göre hesapları kıyaslamak için kullanılan yoğun operasyon indeksi."
      />

      <FilterBar resetHref="/accounts">
        <FilterField label="Arama">
          <FilterInput
            name="search"
            defaultValue={typeof params.search === "string" ? params.search : ""}
            placeholder="@hesap, bio veya özet ara"
          />
        </FilterField>
        <FilterField label="Örgüt">
          <FilterSelect name="orgut" defaultValue={typeof params.orgut === "string" ? params.orgut : ""}>
            <option value="">Tümü</option>
            <option value="PKK/KCK">PKK/KCK</option>
            <option value="DHKP-C">DHKP-C</option>
            <option value="FETÖ">FETÖ</option>
            <option value="DEAŞ/IŞİD">DEAŞ/IŞİD</option>
            <option value="belirsiz">Belirsiz</option>
          </FilterSelect>
        </FilterField>
        <FilterField label="Tehdit">
          <FilterSelect name="threat" defaultValue={typeof params.threat === "string" ? params.threat : ""}>
            <option value="">Tümü</option>
            <option value="yok">Yok</option>
            <option value="dusuk">Düşük</option>
            <option value="orta">Orta</option>
            <option value="yuksek">Yüksek</option>
            <option value="kritik">Kritik</option>
          </FilterSelect>
        </FilterField>
        <FilterField label="Bayrak Durumu">
          <FilterSelect name="flagged_only" defaultValue={params.flagged_only === "true" ? "true" : ""}>
            <option value="">Tümü</option>
            <option value="true">Sadece bayraklı</option>
          </FilterSelect>
        </FilterField>
      </FilterBar>

      <AppPanel className="overflow-hidden p-0">
        <div className="grid grid-cols-[minmax(220px,0.95fr)_minmax(0,1.9fr)_220px_120px] border-b border-[var(--border-default)] px-4 py-4 text-[11px] font-semibold uppercase tracking-[0.22em] text-[var(--text-muted)]">
          <div>Hesap</div>
          <div>Analist Özeti</div>
          <div>Örgüt / Tehdit</div>
          <div>Hacim</div>
        </div>

        <div>
          {response.items.map((account) => {
            const detectedOrg = resolveDetectedOrg(account);
            return (
              <Link
                key={account.id}
                href={`/accounts/${account.id}`}
                className="grid grid-cols-[minmax(220px,0.95fr)_minmax(0,1.9fr)_220px_120px] gap-4 border-b border-[var(--border-subtle)] px-4 py-4 transition-colors hover:bg-[rgba(255,255,255,0.03)] focus:bg-[rgba(255,255,255,0.03)] last:border-b-0"
              >
              <div className="min-w-0">
                <div data-mono="true" className="text-[15px] font-semibold text-[var(--text-primary)]">
                  @{account.instagram_username}
                </div>
                <div className="mt-2 line-clamp-2 text-[13px] leading-6 text-[var(--text-muted)]">{account.bio || "-"}</div>
              </div>

              <div className="min-w-0">
                <div className="line-clamp-3 text-[14px] leading-7 text-[var(--text-secondary)]">
                  {account.account_profile_summary || account.profil_ozeti || "Profil özeti yok."}
                </div>
              </div>

              <div className="space-y-2">
                <StatusBadge tone={detectedOrg !== "belirsiz" ? "warning" : "neutral"}>
                  {detectedOrg}
                </StatusBadge>
                <div>
                  <StatusBadge tone={getThreatTone(account.tehdit_seviyesi)}>{account.tehdit_seviyesi || "belirsiz"}</StatusBadge>
                </div>
              </div>

              <div data-mono="true" className="space-y-1 text-[13px] leading-6 text-[var(--text-secondary)]">
                <div>{account.post_count || 0} post</div>
                <div>{account.comment_count || 0} yorum</div>
                <div>{account.flagged_comment_count || 0} bayrak</div>
              </div>
              </Link>
            );
          })}
        </div>
      </AppPanel>
    </div>
  );
}
