import { NextResponse } from "next/server";
import { DEFAULT_SUBJECT_KIND, SubjectKind, parseSubjectKind } from "@/lib/subject-kind";
import { buildBangumiSearchResponse, searchBangumiSubjects } from "@/lib/bangumi/search";

export async function handleBangumiSearchRequest(
  request: Request,
  options?: {
    forcedKind?: SubjectKind;
  }
) {
  const { searchParams } = new URL(request.url);
  const query = (searchParams.get("q") || "").trim();
  const requestedKind = parseSubjectKind(searchParams.get("kind"));
  const kind = options?.forcedKind ?? requestedKind ?? DEFAULT_SUBJECT_KIND;

  if (!query) {
    return NextResponse.json(buildBangumiSearchResponse({ query: "", kind, items: [] }));
  }

  if (query.length < 2) {
    const payload = buildBangumiSearchResponse({ query, kind, items: [] });
    return NextResponse.json(
      {
        ...payload,
        ok: false,
        error: "至少输入 2 个字符",
      },
      { status: 400 }
    );
  }

  try {
    const items = await searchBangumiSubjects({ query, kind });
    return NextResponse.json(buildBangumiSearchResponse({ query, kind, items }));
  } catch (error) {
    const payload = buildBangumiSearchResponse({ query, kind, items: [] });
    return NextResponse.json(
      {
        ...payload,
        ok: false,
        error: error instanceof Error ? error.message : "搜索失败",
      },
      { status: 500 }
    );
  }
}
