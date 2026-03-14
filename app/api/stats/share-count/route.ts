import { NextResponse } from "next/server";
import { countAllShares } from "@/lib/share/storage";

const SHARE_COUNT_CDN_TTL_SECONDS = 300;
const SHARE_COUNT_STALE_TTL_SECONDS = 600;
const SHARE_COUNT_CACHE_CONTROL_VALUE = `public, max-age=0, s-maxage=${SHARE_COUNT_CDN_TTL_SECONDS}, stale-while-revalidate=${SHARE_COUNT_STALE_TTL_SECONDS}`;

function createShareCountCacheHeaders() {
  return {
    "Cache-Control": SHARE_COUNT_CACHE_CONTROL_VALUE,
    "CDN-Cache-Control": SHARE_COUNT_CACHE_CONTROL_VALUE,
  };
}

export async function GET() {
  try {
    const totalCount = await countAllShares();
    return NextResponse.json(
      {
        ok: true,
        totalCount,
      },
      {
        headers: createShareCountCacheHeaders(),
      }
    );
  } catch {
    return NextResponse.json(
      {
        ok: false,
        error: "总数加载失败",
      },
      {
        status: 500,
      }
    );
  }
}
