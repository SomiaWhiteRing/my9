import { normalizeShareId } from "@/lib/share/id";
import { getShare } from "@/lib/share/storage";

const SHARE_GET_CDN_TTL_SECONDS = 3600;
const SHARE_GET_STALE_TTL_SECONDS = 86400;
const SHARE_GET_CACHE_CONTROL_VALUE = `public, max-age=0, s-maxage=${SHARE_GET_CDN_TTL_SECONDS}, stale-while-revalidate=${SHARE_GET_STALE_TTL_SECONDS}`;

function createShareGetCacheHeaders() {
  return {
    "Cache-Control": SHARE_GET_CACHE_CONTROL_VALUE,
    "CDN-Cache-Control": SHARE_GET_CACHE_CONTROL_VALUE,
  };
}

export async function handleShareGetRequest(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const id = normalizeShareId(searchParams.get("id"));
    if (!id) {
      return Response.json(
        {
          ok: false,
          error: "无效的分享 ID",
        },
        { status: 400 }
      );
    }

    const share = await getShare(id);
    if (!share) {
      return Response.json(
        {
          ok: false,
          error: "分享不存在",
        },
        { status: 404 }
      );
    }

    return Response.json(
      {
        ok: true,
        shareId: share.shareId,
        kind: share.kind,
        creatorName: share.creatorName,
        games: share.games,
      },
      {
        headers: createShareGetCacheHeaders(),
      }
    );
  } catch (error) {
    return Response.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "读取失败",
      },
      { status: 500 }
    );
  }
}
