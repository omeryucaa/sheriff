import { IngestConsole } from "@/components/ingest-console";
import { PageHeader } from "@/components/ui";
import { getIngestTrace, getJobsOverview } from "@/lib/api";

export default async function OperationsPage() {
  const [trace, overview] = await Promise.all([getIngestTrace(), getJobsOverview({ limit: 30 })]);

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="Operasyon"
        title="Batch Orkestrasyon ve İzleme"
        description="Hedef toplu işi açın, işleyici dalgalarını çalıştırın, kuyrukta-çalışıyor-tamamlandı zincirini ve yorumlardan bulunan takip araştırma adaylarını tek panelde izleyin."
      />
      <IngestConsole initialTrace={trace.content} initialOverview={overview} />
    </div>
  );
}
