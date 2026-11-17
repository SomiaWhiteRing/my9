import Resonance from "@/app/[kind]/s/preview/page";
import Discover from "@/app/[kind]/find-preview/page";
import Detail from "@/app/[kind]/s/find-preview/[shareId]/page";
import Trends from "@/app/trends/preview/page";
import { parseSubjectKind } from "@/lib/subject-kind";
import { DISCOVERY_PREVIEW_ITEMS } from "@/lib/dev/share-discovery-preview";

export function previewPage(url: URL) {
  const [, kind, segment, detail, shareId] = url.pathname.split("/");
  const searchParams = Object.fromEntries(url.searchParams);
  if (url.pathname === "/trends/preview") return <Trends searchParams={searchParams} />;
  if (url.pathname === "/game/s/preview") return <Resonance params={{ kind }} searchParams={searchParams} />;
  if (!parseSubjectKind(kind)) return null;
  if (url.pathname === `/${kind}/find-preview`) return <Discover params={{ kind }} searchParams={searchParams} />;
  if (segment === "s" && detail === "find-preview" && DISCOVERY_PREVIEW_ITEMS.some(i => i.kind === kind && i.shareId === shareId))
    return <Detail params={{ kind, shareId }} searchParams={searchParams} />;
  return null;
}
