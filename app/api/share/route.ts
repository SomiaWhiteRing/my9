import { NextResponse } from "next/server";
import { createShareId } from "@/lib/share/id";
import { saveShare } from "@/lib/share/storage";
import { ShareGame, StoredShareV1 } from "@/lib/share/types";
import { parseSubjectKind } from "@/lib/subject-kind";

const MAX_CREATOR_LENGTH = 40;
const MAX_COMMENT_LENGTH = 140;
function sanitizeString(value: unknown): string {
  if (typeof value !== "string") return "";
  return value.trim();
}

function sanitizeHttpUrl(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (!trimmed) return null;

  try {
    const parsed = new URL(trimmed);
    const protocol = parsed.protocol.toLowerCase();
    if (protocol !== "http:" && protocol !== "https:") {
      return null;
    }
    return parsed.toString();
  } catch {
    return null;
  }
}

function toRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object") return null;
  return value as Record<string, unknown>;
}

function sanitizeGame(input: unknown): ShareGame | null {
  const game = toRecord(input);
  if (!game) return null;

  const name = sanitizeString(game.name);
  if (!name) return null;

  const id =
    typeof game.id === "number" || typeof game.id === "string"
      ? game.id
      : String(name);
  const coverRaw = game.cover;
  const cover = typeof coverRaw === "string" && coverRaw.trim() ? coverRaw.trim() : null;

  const commentRaw = sanitizeString(game.comment);
  const comment = commentRaw ? commentRaw.slice(0, MAX_COMMENT_LENGTH) : undefined;
  const spoiler = Boolean(game.spoiler);

  const releaseYear =
    typeof game.releaseYear === "number" && Number.isFinite(game.releaseYear)
      ? Math.trunc(game.releaseYear)
      : undefined;

  const localizedName = sanitizeString(game.localizedName) || undefined;
  const genres = Array.isArray(game.genres)
    ? game.genres
        .map((item: unknown) => sanitizeString(item))
        .filter((item: string) => Boolean(item))
        .slice(0, 5)
    : undefined;

  const storeUrlsRaw = game.storeUrls;
  const storeUrls =
    storeUrlsRaw && typeof storeUrlsRaw === "object"
      ? (Object.fromEntries(
          Object.entries(storeUrlsRaw)
            .filter(([k]) => typeof k === "string")
            .map(([k, v]) => [k, sanitizeHttpUrl(v)])
            .filter((entry): entry is [string, string] => Boolean(entry[1]))
        ) as Record<string, string>)
      : undefined;

  return {
    id,
    name,
    localizedName,
    cover,
    releaseYear,
    genres,
    storeUrls: storeUrls && Object.keys(storeUrls).length > 0 ? storeUrls : undefined,
    comment,
    spoiler,
  };
}

function parseGames(input: unknown): Array<ShareGame | null> | null {
  if (!Array.isArray(input) || input.length !== 9) return null;
  return input.map((item) => sanitizeGame(item));
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const kind = parseSubjectKind(body?.kind);
    if (!kind) {
      return NextResponse.json(
        {
          ok: false,
          error: "kind 参数无效",
          code: "invalid_kind",
        },
        { status: 400 }
      );
    }

    const creatorNameRaw = sanitizeString(body?.creatorName);
    const creatorName = creatorNameRaw ? creatorNameRaw.slice(0, MAX_CREATOR_LENGTH) : null;
    const games = parseGames(body?.games);

    if (!games) {
      return NextResponse.json(
        {
          ok: false,
          error: "games 参数必须是长度为 9 的数组",
          code: "invalid_games",
        },
        { status: 400 }
      );
    }

    const shareId = createShareId();
    const now = Date.now();
    const record: StoredShareV1 = {
      shareId,
      kind,
      creatorName,
      games,
      createdAt: now,
      updatedAt: now,
      lastViewedAt: now,
    };

    const saveResult = await saveShare(record);
    const finalShareId = saveResult.shareId;
    const origin = new URL(request.url).origin;
    const shareUrl = `${origin}/${kind}/s/${finalShareId}`;

    return NextResponse.json({
      ok: true,
      shareId: finalShareId,
      kind,
      shareUrl,
      deduped: saveResult.deduped,
    });
  } catch (error) {
    console.error("[share] save failed", error);
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "保存失败",
      },
      { status: 500 }
    );
  }
}

export { handleShareGetRequest as GET } from "@/lib/share/read-route";
