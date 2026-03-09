import type { Metadata } from "next";
import { notFound, redirect } from "next/navigation";
import My9V3App from "@/app/components/My9V3App";
import { normalizeShareId } from "@/lib/share/id";
import { getShare } from "@/lib/share/storage";
import type { ShareGame } from "@/lib/share/types";
import { getSubjectKindMeta, parseSubjectKind, type SubjectKind } from "@/lib/subject-kind";

export function generateMetadata({
  params,
}: {
  params: { kind: string; shareId: string };
}): Metadata {
  const kind = parseSubjectKind(params.kind);
  if (!kind) {
    return { title: "页面不存在" };
  }

  const meta = getSubjectKindMeta(kind);
  return {
    title: `${meta.shareTitle}分享页`,
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

  let initialShareData: {
    shareId: string;
    kind: SubjectKind;
    creatorName: string | null;
    games: Array<ShareGame | null>;
  } | null = null;

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

  return (
    <My9V3App
      kind={kind}
      initialShareId={shareId}
      initialShareData={initialShareData}
      readOnlyShare
    />
  );
}
