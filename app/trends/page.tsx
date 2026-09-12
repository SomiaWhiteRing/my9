import { Suspense } from "react";
import TrendsClientPage from "@/app/components/TrendsClientPage";
import { createPageMetadata } from "@/lib/page-metadata";

export const dynamic = "force-static";

export const metadata = createPageMetadata(
  "构成大家的作品",
  "查看 My9「大家的构成」作品排行榜，了解大家选择的游戏、动画、电影、音乐等内容。",
  "/trends",
);

function TrendsPageFallback() {
  return (
    <main className="min-h-screen bg-background text-foreground">
      <div className="mx-auto flex min-h-screen w-full max-w-6xl items-center justify-center px-4 py-16 sm:px-6">
        <p className="text-sm text-muted-foreground">趋势数据加载中...</p>
      </div>
    </main>
  );
}

export default function TrendsPage() {
  return (
    <Suspense fallback={<TrendsPageFallback />}>
      <TrendsClientPage />
    </Suspense>
  );
}
