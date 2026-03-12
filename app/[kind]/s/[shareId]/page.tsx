import type { Metadata } from "next";
import { notFound, permanentRedirect, redirect } from "next/navigation";
import My9ReadonlyApp from "@/app/components/My9ReadonlyApp";
import My9ReadonlyPage, { type InitialReadonlyShareData } from "@/app/components/My9ReadonlyPage";
import { isCanonicalShareId, normalizeShareId } from "@/lib/share/id";
import { getShare } from "@/lib/share/storage";
import { getSubjectKindShareTitle, parseSubjectKind } from "@/lib/subject-kind";

export function generateMetadata({
  params,
}: {
  params: { kind: string; shareId: string };
}): Metadata {
  const kind = parseSubjectKind(params.kind);
  if (!kind) {
    return { title: "页面不存在" };
  }

  return {
    title: `${getSubjectKindShareTitle(kind)}分享页`,
  };
}

export default async function ShareReadonlyPage({
  params,
}: {
  params: { kind: string; shareId: string };
}) {
  const kind = parseSubjectKind(params.kind);
  const shareId = normalizeShareId(params.shareId);
  if (!kind || !shareId) {
    notFound();
  }

  if (!isCanonicalShareId(params.shareId) || params.shareId.trim().toLowerCase() !== shareId) {
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
