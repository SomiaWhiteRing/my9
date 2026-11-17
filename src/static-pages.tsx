import Home, { metadata as home } from "@/app/page";
import Editor, { kindMetadata } from "@/app/[kind]/page";
import Custom, { metadata as custom } from "@/app/custom/page";
import Trends, { metadata as trends } from "@/app/trends/page";
import Agreement, { metadata as agreement } from "@/app/agreement/page";
import Privacy, { metadata as privacy } from "@/app/privacy-policy/page";
import Disclosure, { metadata as disclosure } from "@/app/commercial-disclosure/page";
import { parseSubjectKind, SUBJECT_KIND_ORDER } from "@/lib/subject-kind";

export const staticPaths = ["/", ...SUBJECT_KIND_ORDER.map(kind => "/" + kind), "/custom", "/trends",
  "/agreement", "/privacy-policy", "/commercial-disclosure"];

export function staticPage(path: string) {
  switch (path) {
    case "/": return { meta: home, view: <Home />, page: "home" };
    case "/custom": return { meta: custom, view: <Custom />, page: "custom" };
    case "/trends": return { meta: trends, view: <Trends />, page: "trends" };
    case "/agreement": return { meta: agreement, view: <Agreement />, page: null };
    case "/privacy-policy": return { meta: privacy, view: <Privacy />, page: null };
    case "/commercial-disclosure": return { meta: disclosure, view: <Disclosure />, page: null };
  }
  const kind = parseSubjectKind(path.slice(1));
  if (kind && path === "/" + kind) return { meta: kindMetadata(kind), view: <Editor kind={kind} />, page: "editor", kind };
  return null;
}
