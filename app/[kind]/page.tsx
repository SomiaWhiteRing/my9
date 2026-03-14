import type { Metadata } from "next";
import { notFound } from "next/navigation";
import My9V3App from "@/app/components/My9V3App";
import { getSubjectKindMeta, parseSubjectKind } from "@/lib/subject-kind";

export const dynamic = "force-dynamic";

type SubjectKindPageParams = {
  kind: string;
};

type SubjectKindPageProps = {
  params: Promise<SubjectKindPageParams>;
};

export async function generateMetadata({
  params,
}: SubjectKindPageProps): Promise<Metadata> {
  const { kind: rawKind } = await params;
  const kind = parseSubjectKind(rawKind);
  if (!kind) {
    return { title: "页面不存在" };
  }

  const meta = getSubjectKindMeta(kind);
  return {
    title: `构成我的${meta.longLabel}`,
  };
}

export default async function SubjectKindPage({
  params,
}: SubjectKindPageProps) {
  const { kind: rawKind } = await params;
  const kind = parseSubjectKind(rawKind);
  if (!kind) {
    notFound();
  }

  return <My9V3App kind={kind} />;
}
