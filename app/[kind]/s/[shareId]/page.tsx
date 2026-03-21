import type { Metadata } from "next";
import { notFound, permanentRedirect, redirect } from "next/navigation";
import My9ReadonlyApp from "@/app/components/My9ReadonlyApp";
import My9ReadonlyPage, { type InitialReadonlyShareData } from "@/app/components/My9ReadonlyPage";
import { isCanonicalShareId, normalizeShareId } from "@/lib/share/id";
import { getShare } from "@/lib/share/storage";
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
  const { kind: rawKind } = await params;
  const kind = parseSubjectKind(rawKind);
  if (!kind) {
    return { title: "页面不存在" };
  }

  return {
    title: `${getSubjectKindShareTitle(kind)}分享页`,
  };
}

export default async function ShareReadonlyPage({
  params,
}: ShareReadonlyPageProps) {
  const { kind: rawKind, shareId: rawShareId } = await params;
  const kind = parseSubjectKind(rawKind);
  const shareId = normalizeShareId(rawShareId);
  if (!kind || !shareId) {
    notFound();
  }

  if (!isCanonicalShareId(rawShareId) || rawShareId.trim().toLowerCase() !== shareId) {
    permanentRedirect(`/${kind}/s/${shareId}`);
  }

  let initialShareData: InitialReadonlyShareData | null = null;

  try {
    const share = await getShare(shareId);
    if (share) {
      const shareKind = parseSubjectKind(share.kind) ?? kind;
      if (shareKind !== kind) {
        redirect(`/${shareKind}/s/${share.shareId}`);
      }

      initialShareData = {
        shareId: share.shareId,
        kind: shareKind,
        creatorName: share.creatorName,
        games: share.games,
      };
    }
  } catch {
    initialShareData = null;
  }

  if (!initialShareData) {
    return <My9ReadonlyApp kind={kind} initialShareId={shareId} initialShareData={null} />;
  }

  return <My9ReadonlyPage kind={kind} shareId={shareId} initialShareData={initialShareData} />;
}
