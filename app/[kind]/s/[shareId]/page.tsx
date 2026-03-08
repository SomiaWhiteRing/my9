import type { Metadata } from "next";
import { notFound } from "next/navigation";
import My9V3App from "@/app/components/My9V3App";
import { normalizeShareId } from "@/lib/share/id";
import { getSubjectKindMeta, parseSubjectKind } from "@/lib/subject-kind";

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

export default function ShareReadonlyPage({
  params,
}: {
  params: { kind: string; shareId: string };
}) {
  const kind = parseSubjectKind(params.kind);
  const shareId = normalizeShareId(params.shareId);
  if (!kind || !shareId) {
    notFound();
  }

  return <My9V3App kind={kind} initialShareId={shareId} readOnlyShare />;
}
