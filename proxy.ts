import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";

const STATIC_TOP_LEVEL_PATHS = new Set([
  "agreement",
  "commercial-disclosure",
  "custom",
  "privacy-policy",
  "trends",
]);

const SUBJECT_KINDS = new Set([
  "game",
  "anime",
  "tv",
  "movie",
  "manga",
  "lightnovel",
  "work",
  "song",
  "album",
  "character",
  "person",
]);

function redirectHome(request: NextRequest) {
  const url = request.nextUrl.clone();
  url.pathname = "/";
  url.search = "";
  return NextResponse.redirect(url);
}

export function proxy(request: NextRequest) {
  const { pathname } = request.nextUrl;
  const segments = pathname.split("/").filter(Boolean);

  if (segments.length === 0) {
    return NextResponse.next();
  }

  const [firstSegment, secondSegment, thirdSegment] = segments;

  if (STATIC_TOP_LEVEL_PATHS.has(firstSegment)) {
    return segments.length === 1 ? NextResponse.next() : redirectHome(request);
  }

  if (!SUBJECT_KINDS.has(firstSegment)) {
    return redirectHome(request);
  }

  if (segments.length === 1) {
    return NextResponse.next();
  }

  if (segments.length === 3 && secondSegment === "s" && thirdSegment.trim()) {
    return NextResponse.next();
  }

  return redirectHome(request);
}

export const config = {
  matcher: [
    "/((?!api|_next/static|_next/image|.*\\..*).*)",
  ],
};
