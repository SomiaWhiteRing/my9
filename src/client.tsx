import { hydrateRoot } from "react-dom/client";
import type { SubjectKind } from "@/lib/subject-kind";
import type { ShareProps } from "./share-view";
import "@/app/globals.css";

type Bootstrap =
  | { page: "home" | "trends" | "custom" | "preview" }
  | { page: "editor"; kind: SubjectKind }
  | { page: "share" } & ShareProps
  | { page: "retry"; kind: SubjectKind; shareId: string };

async function start(state: Bootstrap) {
  const root = document.getElementById("app")!;
  switch (state.page) {
    case "home": {
      const { default: Home } = await import("@/app/page");
      hydrateRoot(root, <Home />);
      break;
    }
    case "editor": {
      const { default: Editor } = await import("@/app/components/My9V3App");
      const app = hydrateRoot(root, <Editor kind={state.kind} onSaved={async (kind, id) => {
        const [{ ShareView, shareMetadata }, response] = await Promise.all([
          import("./share-view"), fetch(`/api/share?id=${encodeURIComponent(id)}&includeSelectionStats=1`)
        ]);
        if (!response.ok) throw new Error("共享页面加载失败");
        const data = await response.json() as ShareProps["data"];
        const meta = shareMetadata(data.kind ?? kind, data.shareId);
        history.replaceState(null, "", meta.path);
        document.title = meta.title;
        const canonical = new URL(meta.path, location.origin).href;
        document.querySelector('link[rel="canonical"]')?.setAttribute("href", canonical);
        for (const [selector, content] of [
          ['meta[name="description"]', meta.description], ['meta[property="og:url"]', canonical],
          ['meta[property="og:title"]', meta.title], ['meta[property="og:description"]', meta.description],
          ['meta[name="twitter:title"]', meta.title], ['meta[name="twitter:description"]', meta.description],
        ]) document.querySelector(selector)?.setAttribute("content", content);
        app.render(<ShareView kind={data.kind} shareId={data.shareId} data={data} />);
        window.scrollTo(0, 0);
      }} />);
      break;
    }
    case "custom": {
      const { default: Custom } = await import("@/app/custom/page");
      hydrateRoot(root, <Custom />);
      break;
    }
    case "trends": {
      const { default: Trends } = await import("@/app/trends/page");
      hydrateRoot(root, <Trends />);
      break;
    }
    case "share": {
      const { ShareView } = await import("./share-view");
      hydrateRoot(root, <ShareView {...state} />);
      break;
    }
    case "retry": {
      const { default: ReadonlyApp } = await import("@/app/components/My9ReadonlyApp");
      hydrateRoot(root, <ReadonlyApp kind={state.kind} initialShareId={state.shareId} />);
      break;
    }
    case "preview": {
      if (import.meta.env.DEV) {
        const { previewPage } = await import("./preview-pages");
        const view = previewPage(new URL(location.href));
        if (view) hydrateRoot(root, view);
      }
    }
  }
}

void start(JSON.parse(document.getElementById("my9-data")!.textContent!));
