# RedKid Media Analyzer (MinIO -> vLLM)

Bu servis, MinIO'ya yüklenmiş medya dosyasını (fotoğraf veya video) presigned URL ile alır ve `vLLM /v1/chat/completions` endpointine açıklama metniyle birlikte gönderir.

## Ne yapar?

- Tek endpoint: `POST /analyze-media`
- Model varsayılanı: `gemma-4-31b-it`
- Fotoğraf için `image_url`, video için `video_url` gönderir.
- vLLM cevabını `choices[0].message.content` alanından döndürür.

## Kurulum

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Ortam Değişkenleri

- `VLLM_BASE_URL` (default: `http://10.21.6.145:8007`)
- `VLLM_MODEL` (default: `gemma-4-31b-it`)
- `VLLM_TIMEOUT_SECONDS` (default: `120`)
- `MINIO_ENDPOINT` (default: `10.21.6.126:9000`)
- `MINIO_ACCESS_KEY` (default: `minioadmin`)
- `MINIO_SECRET_KEY` (default: `TurkAI2026_minio`)
- `MINIO_SECURE` (default: `false`)
- `MINIO_BUCKET_DEFAULT` (default: `instagram-archive`)
- `MINIO_BUCKET_FALLBACK` (default: `instagram_archive`)
- `SQLITE_DB_PATH` (default: `./data/redkid.db`)

## Çalıştırma

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

Geliştirme için tek komut:

```bash
./scripts/dev-backend.sh
```

Frontend:

```bash
./scripts/dev-frontend.sh
```

## İstek Örneği

Fotoğraf:

```bash
curl -X POST http://localhost:8080/analyze-media \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "uploads",
    "object_key": "images/sample.png",
    "description": "Bu görselde ne var, kısa anlat.",
    "media_type": "image",
    "expires_seconds": 900,
    "max_tokens": 256
  }'
```

Video:

```bash
curl -X POST http://localhost:8080/analyze-media \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "uploads",
    "object_key": "videos/sample.mp4",
    "description": "Videoda ne görüyorsun, kısa anlat.",
    "media_type": "video",
    "expires_seconds": 900,
    "max_tokens": 256
  }'
```

## Post + Yorum Analizi (Yeni)

Bu endpoint, gelen JSON içindeki kullanıcı/bio/açıklama/medya/yorumları iki aşamada işler:

1. Gönderiyi VLM ile analiz eder.
2. Her yorumu `supportive | opposing | irrelevant | unclear` olarak sınıflandırır.
3. Sonucu SQLite tablolara yazar (`persons`, `instagram_accounts`, `instagram_posts`, `instagram_comments`).

Endpoint:

```bash
POST /analyze-post-and-comments
```

Örnek istek (`media_url` ile):

```bash
curl -X POST http://localhost:8080/analyze-post-and-comments \
  -H "Content-Type: application/json" \
  -d '{
    "username": "kullanici_adi",
    "instagram_username": "insta_adi",
    "profile_photo_url": "https://cdn.example.com/p.jpg",
    "bio": "Kullanıcının bio metni",
    "caption": "Gönderi açıklaması",
    "media_type": "video",
    "media_url": "https://example.com/video.mp4",
    "comments": [
      {"commenter_username": "u1", "text": "Helal olsun"},
      {"commenter_username": "u2", "text": "Bu ne alaka?"}
    ]
  }'
```

Örnek response alanları:

- `person_id`
- `instagram_account_id`
- `post_id`
- `comment_ids`

## Instagram Arşiv Batch Ingestion (Yeni)

Bu endpoint MinIO arşivinden (`instagram/{username}/{run_id}/...`) en güncel run'ı bulur, postları sırayla VLM'e gönderir, yorumları sınıflandırır ve DB'ye yazar.

Endpoint:

```bash
POST /ingest-instagram-account-latest
```

Örnek istek:

```bash
curl -X POST http://localhost:8080/ingest-instagram-account-latest \
  -H "Content-Type: application/json" \
  -d '{
    "target_username": "kurdistan24tv.official",
    "max_posts": 20,
    "max_comments_per_post": 50,
    "analyze_comments": true
  }'
```

Örnek response alanları:

- `run_id`
- `processed_posts` / `created_posts` / `updated_posts`
- `processed_comments` / `created_comments` / `skipped_comments`
- `flagged_users` / `flagged_usernames`
- `errors`

Not:

- `9001` MinIO console portudur; S3 API istekleri için `9000` kullanılır.
- Bir yorum `supportive` ise yorum sahibi `review_queue` tablosuna (inceleme listesi) alınır.

Örnek istek (`bucket + object_key` ile, presigned URL üretir):

```bash
curl -X POST http://localhost:8080/analyze-post-and-comments \
  -H "Content-Type: application/json" \
  -d '{
    "username": "kullanici_adi",
    "instagram_username": "insta_adi",
    "bio": "Kullanıcının bio metni",
    "caption": "Gönderi açıklaması",
    "media_type": "image",
    "bucket": "uploads",
    "object_key": "images/post.png",
    "comments": [
      {"commenter_username": "u1", "text": "Destekliyorum"}
    ]
  }'
```

## Test

```bash
pytest -q
```
