import type { SubjectKind } from "@/lib/subject-kind";
import { getSubjectKindShareTitle } from "@/lib/subject-kind";
import { createPageMetadata } from "@/lib/page-metadata";
import My9ReadonlyPage, { type InitialReadonlyShareData } from "@/app/components/My9ReadonlyPage";

export type ShareProps = { kind: SubjectKind; shareId: string; data: InitialReadonlyShareData };
export function shareMetadata(kind: SubjectKind, shareId: string) {
  const title = getSubjectKindShareTitle(kind);
  return createPageMetadata(`${title}分享页`, `查看这份「${title}」的选择与评论，也可以创建属于自己的构成。`, `/${kind}/s/${shareId}`);
}

export function ShareView({ kind, shareId, data }: ShareProps) {
  return <My9ReadonlyPage kind={kind} shareId={shareId} initialShareData={data} />;
}
