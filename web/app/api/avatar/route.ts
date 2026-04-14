import { NextRequest } from "next/server";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8080";

export async function GET(request: NextRequest) {
  const source = request.nextUrl.searchParams.get("source") || request.nextUrl.searchParams.get("url");
  if (!source) {
    return new Response("missing source", { status: 400 });
  }

  const upstream = await fetch(`${API_BASE_URL}/media/avatar?source=${encodeURIComponent(source)}`, {
    cache: "no-store",
  });

  if (!upstream.ok) {
    return new Response("avatar fetch failed", { status: upstream.status });
  }

  const contentType = upstream.headers.get("content-type") || "image/jpeg";
  const payload = await upstream.arrayBuffer();
  return new Response(payload, {
    headers: {
      "content-type": contentType,
      "cache-control": "public, max-age=3600",
    },
  });
}
