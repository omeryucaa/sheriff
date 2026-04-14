# Support + 0 Examples

Bu dosya, LLM'in `comment_type="support"` verdiği ama `organization_link_score=0` bıraktığı örnekleri toplar.

## Example 1

Commenter: `bunyaminkacak65`  
Comment: `🔥`

### Prompt excerpt sent to LLM

```text
Comment:
- Comment owner: bunyaminkacak65
- Comment text: 🔥
- Öncelikli inceleme odağı: Daltons

Rules:
- If a comment is supportive toward the investigated account/post, a low non-zero score is usually appropriate even when the support is short, symbolic, emotional, or low-detail.
- Use score 0 only when the comment is neutral, unrelated, oppositional, or has no meaningful supportive/aligned signal.

Scoring:
- 0 = neutral, unrelated, oppositional, or no meaningful supportive/aligned signal
- 1-3 = weak but real support, praise, approval, or symbolic support
```

### Raw LLM response excerpt

```json
{
  "comment_type": "support",
  "content_summary_tr": "Kullanıcı, hedef hesap tarafından paylaşılan kişisel fotoğrafa 'ateş' emojisi ile tepki vererek onay/beğeni göstermiştir.",
  "flags": {
    "active_supporter": {
      "flag": false,
      "reason_tr": "Tek bir emoji ile verilen tepki, aktif destekçilik için yetersiz kanıttır."
    }
  }
}
```

### Final parsed result

```text
comment_type='support'
organization_link_score=0
reason='Yalnızca tek bir emoji kullanımı organizasyonel bağ kurmak için yetersizdir.'
```

## Example 2

Commenter: `canlcx47`  
Comment: `🔥❤️`

### Prompt excerpt sent to LLM

```text
Comment:
- Comment owner: canlcx47
- Comment text: 🔥❤️
- Öncelikli inceleme odağı: Daltons

Rules:
- If the comment expresses praise, admiration, approval, encouragement, solidarity, celebration, or symbolic positive support toward the investigated account/post, treat it as a support signal unless it is clearly critical, mocking, or unrelated.
- If a comment is supportive toward the investigated account/post, a low non-zero score is usually appropriate even when the support is short, symbolic, emotional, or low-detail.
```

### Raw LLM response excerpt

```json
{
  "comment_type": "support",
  "content_summary_tr": "Yorum sahibi, ateş ve kalp emojileri kullanarak paylaşım sahibine yönelik sembolik bir destek ve beğeni göstermiştir.",
  "flags": {
    "active_supporter": {
      "flag": true,
      "reason_tr": "İnceleme odağındaki hesapla ilişkili kişiye yönelik sembolik destek (emoji) paylaşımı."
    }
  }
}
```

### Final parsed result

```text
comment_type='support'
active_supporter_flag=True
organization_link_score=0
reason='Sadece sembolik destek içeren emojiler kullanılmıştır; kurumsal bir bağ kanıtlanamamıştır.'
```

## Example 3

Commenter: `salih.oztk65`  
Comment: `Varlın yeter kral 👑`

### Prompt excerpt sent to LLM

```text
Comment:
- Comment owner: salih.oztk65
- Comment text: Varlın yeter kral 👑
- Öncelikli inceleme odağı: Daltons

Rules:
- If the comment expresses praise, admiration, approval, encouragement, solidarity, celebration, or symbolic positive support toward the investigated account/post, treat it as a support signal unless it is clearly critical, mocking, or unrelated.
- Lack of strong evidence for formal organizational connection does not require score 0.
```

### Raw LLM response excerpt

```json
{
  "comment_type": "support",
  "content_summary_tr": "Kullanıcı, hesap sahibine yönelik 'Varlığın yeter kral' ifadesiyle hayranlık ve destek belirten bir yorum yapmıştır.",
  "flags": {
    "active_supporter": {
      "flag": true,
      "reason_tr": "İnceleme odağındaki hesap sahibine yönelik açık destek ve övgü ifadesi kullanılmıştır."
    }
  }
}
```

### Final parsed result

```text
comment_type='support'
active_supporter_flag=True
organization_link_score=0
reason='Yalnızca bireysel destek ifadesi bulunmaktadır; kurumsal bir bağa dair kanıt yoktur.'
```

## Reading

Ortak pattern şu:

- Model `support` sınıfını veriyor.
- Bazı örneklerde `active_supporter_flag=true` bile veriyor.
- Ama gerekçede tekrar tekrar `kurumsal/organizasyonel bağ kanıtı yok` diyerek skoru `0` bırakıyor.

Bu da modelin `organization_link_score` alanını hâlâ "formal organizational proof" gibi yorumladığını gösteriyor.
