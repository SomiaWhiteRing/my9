import { NextResponse } from "next/server";
import { runShareArchive } from "@/lib/share/archive";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

function isAuthorized(request: Request): boolean {
  const cronSecret = process.env.CRON_SECRET?.trim();
  if (!cronSecret) {
    return process.env.NODE_ENV !== "production";
  }

  const authorization = request.headers.get("authorization");
  return authorization === `Bearer ${cronSecret}`;
}

export async function GET(request: Request) {
  if (!isAuthorized(request)) {
    return NextResponse.json(
      {
        ok: false,
        error: "unauthorized",
      },
      { status: 401 }
    );
  }

  try {
    const { config, result } = await runShareArchive();

    return NextResponse.json({
      ok: true,
      ...result,
      config,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "archive failed",
      },
      { status: 500 }
    );
  }
}

