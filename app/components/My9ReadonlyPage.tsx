import Link from "next/link";
import { SiteFooter } from "@/components/layout/SiteFooter";
import { SharePlatformActions } from "@/components/share/SharePlatformActions";
import { ReadonlyNineGridBoard } from "@/app/components/v3/ReadonlyNineGridBoard";
import { ReadonlySelectedGamesList } from "@/app/components/v3/ReadonlySelectedGamesList";
import { SubjectKind, getSubjectKindMeta, getSubjectKindShareTitle } from "@/lib/subject-kind";
import { ShareGame } from "@/lib/share/types";

export type InitialReadonlyShareData = {
  shareId: string;
  kind: SubjectKind;
  creatorName: string | null;
  games: Array<ShareGame | null>;
};

interface My9ReadonlyPageProps {
  kind: SubjectKind;
  shareId: string;
  initialShareData: InitialReadonlyShareData;
}

export default function My9ReadonlyPage({ kind, shareId, initialShareData }: My9ReadonlyPageProps) {
  const kindMeta = getSubjectKindMeta(kind);
  const shareTitle = getSubjectKindShareTitle(kind);
  const games = initialShareData.games;
  const creatorName = initialShareData.creatorName || "";
  const finalShareId = initialShareData.shareId || shareId;

  return (
    <main className="min-h-screen bg-background px-4 py-16 text-foreground">
      <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4">
        <header className="space-y-3 text-center">
          <h1 className="whitespace-nowrap text-3xl font-bold leading-tight tracking-tight text-foreground sm:text-4xl">
            {shareTitle}
          </h1>
          <p className="text-sm text-muted-foreground">{kindMeta.subtitle}</p>
          <Link
            href={`/trends?kind=${kind}`}
            className="inline-flex items-center justify-center gap-1 rounded-full border border-sky-200 bg-sky-50 px-4 py-1.5 text-xs font-semibold text-sky-700 transition-colors hover:bg-sky-100 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200 dark:hover:bg-sky-900/60"
          >
            大家的构成
            <span className="text-red-500">(New!)</span>
          </Link>
        </header>

        <div className="flex flex-col items-center gap-2">
          <p className="rounded-full border border-amber-200 bg-amber-50 px-4 py-1.5 text-xs font-semibold text-amber-700 dark:border-amber-900 dark:bg-amber-950/50 dark:text-amber-300">
            这是共享页面（只读）
          </p>
          <p className="text-sm text-muted-foreground">创作者: {creatorName.trim() || "匿名"}</p>
          <Link
            href={`/${kind}`}
            className="inline-flex items-center justify-center gap-2 rounded-full border border-border bg-card px-5 py-2 text-sm font-bold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            前往填写页面
          </Link>
        </div>

        <div className="mx-auto w-full rounded-xl border-4 border-background bg-card p-1 shadow-2xl ring-1 ring-border/70 sm:p-4">
          <ReadonlyNineGridBoard games={games} subjectLabel={kindMeta.label} />
        </div>

        <div className="flex w-full flex-col items-center gap-3">
          <SharePlatformActions
            kind={kind}
            shareId={finalShareId}
            games={games}
            creatorName={creatorName}
          />
        </div>

        <ReadonlySelectedGamesList
          games={games}
          subjectLabel={kindMeta.label}
          bangumiSearchCat={kindMeta.search.bangumiSearchCat}
          kind={kind}
        />

        <SiteFooter className="w-full" kind={kind} />
      </div>
    </main>
  );
}
