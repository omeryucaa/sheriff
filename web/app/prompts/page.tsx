import { PromptManager } from "@/components/prompt-manager";
import { PageHeader } from "@/components/ui";
import { getPromptTemplates } from "@/lib/api";

export default async function PromptsPage() {
  const prompts = await getPromptTemplates();

  return (
    <div className="space-y-8">
      <PageHeader
        eyebrow="İstem Yönetimi"
        title="Prompt Yönetim Merkezi"
        description="Medya, gönderi, yorum ve profil özeti promptlarını kontrollü bir iç araç ekranı üzerinden yönetin."
      />
      <PromptManager initialItems={prompts.items} />
    </div>
  );
}
