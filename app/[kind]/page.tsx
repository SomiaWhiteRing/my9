import type { Metadata } from "next";
import { notFound } from "next/navigation";
import My9V3App from "@/app/components/My9V3App";
import { getSubjectKindMeta, parseSubjectKind } from "@/lib/subject-kind";

export function generateMetadata({
  params,
}: {
  params: { kind: string };
}): Metadata {
  const kind = parseSubjectKind(params.kind);
  if (!kind) {
    return { title: "页面不存在" };
  }

  const meta = getSubjectKindMeta(kind);
  return {
    title: `构成我的${meta.longLabel}`,
  };
}

export default function SubjectKindPage({
  params,
}: {
  params: { kind: string };
}) {
  const kind = parseSubjectKind(params.kind);
  if (!kind) {
    notFound();
  }

  return <My9V3App kind={kind} />;
}
