from datetime import timedelta

from minio import Minio
from minio.error import S3Error


class MinioService:
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool) -> None:
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def presigned_get_object(self, bucket: str, object_key: str, expires_seconds: int) -> str:
        return self.client.presigned_get_object(
            bucket_name=bucket,
            object_name=object_key,
            expires=timedelta(seconds=expires_seconds),
        )

    def list_object_names(self, bucket: str, prefix: str, recursive: bool = True) -> list[str]:
        return [obj.object_name for obj in self.client.list_objects(bucket, prefix=prefix, recursive=recursive)]

    def read_object_text(self, bucket: str, object_key: str, encoding: str = "utf-8") -> str:
        obj = self.client.get_object(bucket, object_key)
        try:
            return obj.read().decode(encoding, errors="replace")
        finally:
            obj.close()
            obj.release_conn()

    def read_object_bytes(self, bucket: str, object_key: str) -> bytes:
        obj = self.client.get_object(bucket, object_key)
        try:
            return obj.read()
        finally:
            obj.close()
            obj.release_conn()

    def object_content_type(self, bucket: str, object_key: str) -> str | None:
        try:
            meta = self.client.stat_object(bucket, object_key)
        except S3Error:
            return None
        return getattr(meta, "content_type", None)

    def object_exists(self, bucket: str, object_key: str) -> bool:
        try:
            self.client.stat_object(bucket, object_key)
            return True
        except S3Error:
            return False

    def bucket_exists(self, bucket: str) -> bool:
        return self.client.bucket_exists(bucket)
