import type { Metadata } from "next";
import { notFound, permanentRedirect, redirect } from "next/navigation";
import My9ReadonlyApp from "@/app/components/My9ReadonlyApp";
import My9ReadonlyPage, { type InitialReadonlyShareData } from "@/app/components/My9ReadonlyPage";
import { normalizeShareId } from "@/lib/share/id";
import { resolveSharePage } from "@/lib/share/page-data";
import { getShareSelectionStats } from "@/lib/share/storage";
import { createPageMetadata } from "@/lib/page-metadata";
import { getSubjectKindShareTitle, parseSubjectKind } from "@/lib/subject-kind";

type ShareReadonlyPageParams = {
  kind: string;
  shareId: string;
};

type ShareReadonlyPageProps = {
  params: Promise<ShareReadonlyPageParams>;
};

export async function generateMetadata({
  params,
}: ShareReadonlyPageProps): Promise<Metadata> {
  const { kind: rawKind, shareId: rawShareId } = await params;
  const kind = parseSubjectKind(rawKind);
  const shareId = normalizeShareId(rawShareId);
  if (!kind || !shareId) {
    return { title: "页面不存在" };
  }

  const shareTitle = getSubjectKindShareTitle(kind);
  return createPageMetadata(
    `${shareTitle}分享页`,
    `查看这份「${shareTitle}」的选择与评论，也可以创建属于自己的构成。`,
    `/${kind}/s/${shareId}`,
  );
}

export default async function ShareReadonlyPage({
  params,
}: ShareReadonlyPageProps) {
  const { kind: rawKind, shareId: rawShareId } = await params;
  const page = await resolveSharePage(rawKind, rawShareId);
  if (page.type === "not-found") notFound();
  if (page.type === "redirect") {
    if (page.status === 308) permanentRedirect(page.location);
    redirect(page.location);
  }
  const { kind, shareId } = page;
  if (page.type === "fallback") {
    return <My9ReadonlyApp kind={kind} initialShareId={shareId} initialShareData={null} />;
  }
  const { share } = page;
  const initialShareData: InitialReadonlyShareData = {
    shareId: share.shareId,
    kind,
    creatorName: share.creatorName,
    games: share.games,
    selectionStats: await getShareSelectionStats(share),
  };

  return <My9ReadonlyPage kind={kind} shareId={shareId} initialShareData={initialShareData} />;
}
