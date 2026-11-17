import My9V3App from "@/app/components/My9V3App";
import { createPageMetadata } from "@/lib/page-metadata";
import { getSubjectKindMeta, type SubjectKind } from "@/lib/subject-kind";

export function kindMetadata(kind: SubjectKind) {
  const meta = getSubjectKindMeta(kind);
  return createPageMetadata(
    `构成我的${meta.longLabel}`,
    `制作「构成我的${meta.longLabel}」九宫格：搜索并挑选最能代表你的${meta.label}，添加评论，生成分享页面与图片，向世界传达你所爱的${meta.label}。`,
    `/${kind}`,
  );
}

export default My9V3App;
