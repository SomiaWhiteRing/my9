import { NextResponse } from "next/server";
import {
  buildCustomSearchResponse,
  parseCustomSearchSource,
  searchCustomSource,
} from "@/lib/custom/search";
import { normalizeSearchQuery } from "@/lib/search/query";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const source = parseCustomSearchSource(searchParams.get("source"));
  const query = normalizeSearchQuery(searchParams.get("q"));

  if (!source) {
    return NextResponse.json(
      {
        ok: false,
        error: "source 参数无效",
        code: "invalid_source",
      },
      { status: 400 }
    );
  }

  if (!query) {
    return NextResponse.json(buildCustomSearchResponse({ source, query: "", items: [] }));
  }

  try {
    const items = await searchCustomSource({ source, query });
    return NextResponse.json(buildCustomSearchResponse({ source, query, items }));
  } catch (error) {
    return NextResponse.json(
      {
        ...buildCustomSearchResponse({ source, query, items: [] }),
        ok: false,
        error: error instanceof Error ? error.message : "搜索失败",
      },
      { status: 500 }
    );
  }
}
