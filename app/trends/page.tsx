import type { Metadata } from "next";
import { Suspense } from "react";
import TrendsClientPage from "@/app/components/TrendsClientPage";

export const dynamic = "force-static";

export const metadata: Metadata = {
  title: "构成大家的作品",
};

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
