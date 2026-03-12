"use client";

import dynamic from "next/dynamic";
import { useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { SubjectKind, getSubjectKindShareTitle } from "@/lib/subject-kind";
import { ShareGame } from "@/lib/share/types";

const ShareImagePreviewDialog = dynamic(
  () => import("@/components/share/ShareImagePreviewDialog").then((mod) => mod.ShareImagePreviewDialog)
);
const ShareLinkDialog = dynamic(
  () => import("@/components/share/ShareLinkDialog").then((mod) => mod.ShareLinkDialog)
);

type NoticeKind = "success" | "error" | "info";

interface SharePlatformActionsProps {
  kind: SubjectKind;
  shareId: string | null;
  games: Array<ShareGame | null>;
  creatorName?: string | null;
  onNotice?: (kind: NoticeKind, message: string) => void;
}

export function SharePlatformActions({
  kind,
  shareId,
  games,
  creatorName,
  onNotice,
}: SharePlatformActionsProps) {
  const [previewOpen, setPreviewOpen] = useState(false);
  const [linkDialogOpen, setLinkDialogOpen] = useState(false);
  const shareUrl = useMemo(() => {
    if (!shareId) return null;
    if (typeof window === "undefined") return null;
    return `${window.location.origin}/${kind}/s/${shareId}`;
  }, [kind, shareId]);

  const shareTitle = useMemo(() => {
    const defaultTitle = getSubjectKindShareTitle(kind);
    const name = creatorName?.trim();
    if (!name) return defaultTitle;
    return defaultTitle.replace("我", name);
  }, [creatorName, kind]);

  function handleNotice(kindValue: NoticeKind, message: string) {
    onNotice?.(kindValue, message);
  }

  const disabled = !shareId;

  const baseClass =
    "inline-flex items-center justify-center gap-2 rounded-full border border-border bg-card px-6 py-3 font-bold text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground disabled:cursor-not-allowed disabled:opacity-45";

  return (
    <div className="grid w-full max-w-[42rem] grid-cols-1 gap-3 sm:grid-cols-2">
      <Button
        variant="default"
        className="order-1 inline-flex items-center justify-center gap-2 rounded-full bg-foreground px-6 py-3 font-bold text-background transition-colors hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-45 sm:order-2"
        data-testid="share-generate-image"
        disabled={disabled}
        onClick={() => {
          if (!shareId) return;
          setPreviewOpen(true);
        }}
      >
        生成分享图片
      </Button>

      <Button
        variant="outline"
        className={`${baseClass} order-2 sm:order-1`}
        data-testid="share-generate-link"
        disabled={disabled}
        onClick={() => {
          if (!shareUrl) return;
          setLinkDialogOpen(true);
        }}
      >
        生成分享链接
      </Button>

      {shareId && previewOpen ? (
        <ShareImagePreviewDialog
          open={previewOpen}
          onOpenChange={setPreviewOpen}
          kind={kind}
          shareId={shareId}
          title={shareTitle}
          games={games}
          creatorName={creatorName}
          onNotice={handleNotice}
        />
      ) : null}
      {shareUrl && linkDialogOpen ? (
        <ShareLinkDialog
          open={linkDialogOpen}
          onOpenChange={setLinkDialogOpen}
          shareUrl={shareUrl}
          onNotice={handleNotice}
        />
      ) : null}
    </div>
  );
}
