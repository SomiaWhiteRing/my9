import { staticPage } from "./static-pages";
import { renderDocument } from "./document";
import { previewPage } from "./preview-pages";

export function renderDevPage(url: URL) {
  const entry = staticPage(url.pathname);
  if (entry) {
    const { meta, view, page, ...params } = entry;
    return renderDocument(meta, view, page ? { page, ...params } : null);
  }
  const view = previewPage(url);
  return view ? renderDocument({ title: "本地预览", description: "", path: url.pathname }, view, { page: "preview" }) : null;
}
