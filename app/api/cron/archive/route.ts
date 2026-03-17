import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET() {
  return NextResponse.json(
    {
      ok: false,
      error: "gone",
      message: "The cold-storage archive endpoint has been removed.",
    },
    { status: 410 }
  );
}
