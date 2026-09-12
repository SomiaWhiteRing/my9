import type { Metadata } from "next";
import { notFound } from "next/navigation";
import My9V3App from "@/app/components/My9V3App";
import { createPageMetadata } from "@/lib/page-metadata";
import { SUBJECT_KIND_ORDER, getSubjectKindMeta, parseSubjectKind } from "@/lib/subject-kind";

export const dynamicParams = false;

type SubjectKindPageParams = {
  kind: string;
};

type SubjectKindPageProps = {
  params: Promise<SubjectKindPageParams>;
};

export function generateStaticParams() {
  return SUBJECT_KIND_ORDER.map((kind) => ({ kind }));
}

export async function generateMetadata({
  params,
}: SubjectKindPageProps): Promise<Metadata> {
  const { kind: rawKind } = await params;
  const kind = parseSubjectKind(rawKind);
  if (!kind) {
    return { title: "页面不存在" };
  }

  const meta = getSubjectKindMeta(kind);
  return createPageMetadata(
    `构成我的${meta.longLabel}`,
    `制作「构成我的${meta.longLabel}」九宫格：搜索并挑选最能代表你的${meta.label}，添加评论，生成分享页面与图片，向世界传达你所爱的${meta.label}。`,
    `/${kind}`,
  );
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
