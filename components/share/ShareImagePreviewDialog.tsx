"use client";

import Image from "next/image";
import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { ShareGame } from "@/lib/share/types";
import {
  buildDefaultShareImageHeaderSubtitle,
  downloadBlob,
  generateShareImageBlob,
} from "@/utils/image/exportShareImage";
import { SubjectKind } from "@/lib/subject-kind";

type NoticeKind = "success" | "error" | "info";

interface ShareImagePreviewDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  kind: SubjectKind;
  shareId: string;
  title: string;
  games: Array<ShareGame | null>;
  creatorName?: string | null;
  onNotice: (kind: NoticeKind, message: string) => void;
}

function buildFileName(title: string) {
  const fileName = `${title}.png`;
  return fileName;
}

export function ShareImagePreviewDialog({
  open,
  onOpenChange,
  kind,
  shareId,
  title,
  games,
  creatorName,
  onNotice,
}: ShareImagePreviewDialogProps) {
  const [showHeaderBlock, setShowHeaderBlock] = useState(true);
  const [showCustomHeaderSubtitle, setShowCustomHeaderSubtitle] = useState(false);
  const [headerSubtitle, setHeaderSubtitle] = useState(() =>
    buildDefaultShareImageHeaderSubtitle(kind, creatorName, games.filter((game) => Boolean(game?.comment?.trim())).length)
  );
  const [showComments, setShowComments] = useState(false);
  const [showNames, setShowNames] = useState(true);
  const [loading, setLoading] = useState(false);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [previewBlob, setPreviewBlob] = useState<Blob | null>(null);
  const [previewError, setPreviewError] = useState("");
  const requestIdRef = useRef(0);
  const previewUrlRef = useRef<string | null>(null);
  const reviewCount = games.filter((game) => Boolean(game?.comment?.trim())).length;
  const defaultHeaderSubtitle = buildDefaultShareImageHeaderSubtitle(kind, creatorName, reviewCount);
  const resolvedHeaderSubtitle = showCustomHeaderSubtitle ? headerSubtitle : defaultHeaderSubtitle;

  useEffect(() => {
    return () => {
      if (previewUrlRef.current) {
        URL.revokeObjectURL(previewUrlRef.current);
        previewUrlRef.current = null;
      }
    };
  }, []);

  useEffect(() => {
    if (!open) {
      setShowHeaderBlock(true);
      setShowCustomHeaderSubtitle(false);
      setHeaderSubtitle(defaultHeaderSubtitle);
      setShowComments(false);
      setShowNames(true);
      setLoading(false);
      setPreviewBlob(null);
      setPreviewError("");
      setPreviewUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev);
        previewUrlRef.current = null;
        return null;
      });
      return;
    }
  }, [defaultHeaderSubtitle, open]);

  useEffect(() => {
    if (reviewCount === 0 && showComments) {
      setShowComments(false);
    }
  }, [reviewCount, showComments]);

  useEffect(() => {
    if (!open) return;
    const requestId = ++requestIdRef.current;

    async function loadPreview() {
      setLoading(true);
      setPreviewError("");
      try {
        const blob = await generateShareImageBlob({
          kind,
          shareId,
          title,
          games,
          creatorName,
          showNames,
          showHeaderBlock,
          showHeaderQr: showHeaderBlock,
          headerSubtitle: resolvedHeaderSubtitle,
          showComments,
        });

        if (requestId !== requestIdRef.current) return;
        const nextUrl = URL.createObjectURL(blob);
        setPreviewBlob(blob);
        setPreviewUrl((prev) => {
          if (prev) URL.revokeObjectURL(prev);
          previewUrlRef.current = nextUrl;
          return nextUrl;
        });
      } catch {
        if (requestId !== requestIdRef.current) return;
        setPreviewBlob(null);
        setPreviewError("图片生成失败，请稍后重试");
        setPreviewUrl((prev) => {
          if (prev) URL.revokeObjectURL(prev);
          previewUrlRef.current = null;
          return null;
        });
      } finally {
        if (requestId === requestIdRef.current) {
          setLoading(false);
        }
      }
    }

    loadPreview();
  }, [
    creatorName,
    games,
    kind,
    open,
    resolvedHeaderSubtitle,
    shareId,
    showComments,
    showHeaderBlock,
    showCustomHeaderSubtitle,
    showNames,
    title,
  ]);

  async function handleDownload() {
    try {
      const blob =
        previewBlob ||
        (await generateShareImageBlob({
          kind,
          shareId,
          title,
          games,
          creatorName,
          showNames,
          showHeaderBlock,
          showHeaderQr: showHeaderBlock,
          headerSubtitle: resolvedHeaderSubtitle,
          showComments,
        }));
      downloadBlob(blob, buildFileName(title));
    } catch {
      onNotice("info", "下载失败，请长按预览图保存");
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[95vw] max-h-[90vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>生成分享图片</DialogTitle>
        </DialogHeader>

        <div className="space-y-3">
          <div className="overflow-hidden rounded-xl border border-border bg-muted">
            {loading ? (
              <div className="flex h-[46vh] min-h-[300px] items-center justify-center text-sm text-muted-foreground">
                正在生成图片...
              </div>
            ) : previewUrl ? (
              <div className="relative h-[46vh] min-h-[300px]">
                <Image
                  src={previewUrl}
                  alt="分享图片预览"
                  fill
                  unoptimized
                  className="mx-auto object-contain"
                  sizes="(max-width: 768px) 95vw, 768px"
                />
              </div>
            ) : (
              <div className="flex h-[46vh] min-h-[300px] items-center justify-center text-sm text-rose-500">
                {previewError || "预览图加载失败"}
              </div>
            )}
          </div>

          <div className="flex items-center justify-between rounded-xl border border-border bg-muted px-3 py-2.5">
            <div className="pr-3">
              <p className="text-sm font-semibold text-foreground">显示名称</p>
            </div>
            <button
              type="button"
              role="switch"
              aria-checked={showNames}
              aria-label="显示名称"
              onClick={() => setShowNames((value) => !value)}
              className={cn(
                "relative inline-flex h-6 w-11 items-center rounded-full transition-colors",
                showNames ? "bg-sky-600" : "bg-muted-foreground/40"
              )}
            >
              <span
                className={cn(
                  "inline-block h-5 w-5 transform rounded-full bg-white transition-transform",
                  showNames ? "translate-x-5" : "translate-x-1"
                )}
              />
            </button>
          </div>

          <div className="overflow-hidden rounded-xl border border-border bg-muted">
            <div className="flex items-center justify-between px-3 py-2.5">
              <div className="pr-3">
                <p className="text-sm font-semibold text-foreground">显示标题</p>
              </div>
              <button
                type="button"
                role="switch"
                aria-checked={showHeaderBlock}
                aria-label="显示标题"
                onClick={() => setShowHeaderBlock((value) => !value)}
                className={cn(
                  "relative inline-flex h-6 w-11 items-center rounded-full transition-colors",
                  showHeaderBlock ? "bg-sky-600" : "bg-muted-foreground/40"
                )}
              >
                <span
                  className={cn(
                    "inline-block h-5 w-5 transform rounded-full bg-white transition-transform",
                    showHeaderBlock ? "translate-x-5" : "translate-x-1"
                  )}
                />
              </button>
            </div>

            {showHeaderBlock ? (
              <div className="space-y-2.5 border-t border-border px-3 py-3">
                <div className="flex items-center justify-between">
                  <div className="pr-3">
                    <p className="text-xs font-medium text-foreground">自定义介绍</p>
                  </div>
                  <button
                    type="button"
                    role="switch"
                    aria-checked={showCustomHeaderSubtitle}
                    aria-label="自定义介绍"
                    onClick={() => setShowCustomHeaderSubtitle((value) => !value)}
                    className={cn(
                      "relative inline-flex h-5 w-9 items-center rounded-full transition-colors",
                      showCustomHeaderSubtitle ? "bg-sky-600" : "bg-muted-foreground/40"
                    )}
                  >
                    <span
                      className={cn(
                        "inline-block h-4 w-4 transform rounded-full bg-white transition-transform",
                        showCustomHeaderSubtitle ? "translate-x-4" : "translate-x-0.5"
                      )}
                    />
                  </button>
                </div>

                {showCustomHeaderSubtitle ? (
                  <Input
                    aria-label="自定义介绍输入框"
                    value={headerSubtitle}
                    onChange={(event) => setHeaderSubtitle(event.target.value.slice(0, 60))}
                    className="h-8 bg-background text-sm md:text-xs"
                  />
                ) : null}
              </div>
            ) : null}
          </div>

          <div className="flex items-center justify-between rounded-xl border border-border bg-muted px-3 py-2.5">
            <div className="pr-3">
              <p className="text-sm font-semibold text-foreground">显示评价</p>
              <p className="text-xs text-muted-foreground">
                {reviewCount === 0 ? "至少存在一条评价才可开启" : null}
              </p>
            </div>
            <button
              type="button"
              role="switch"
              aria-checked={showComments}
              aria-label="显示评价"
              disabled={reviewCount === 0}
              onClick={() => setShowComments((value) => !value)}
              className={cn(
                "relative inline-flex h-6 w-11 items-center rounded-full transition-colors disabled:cursor-not-allowed disabled:opacity-45",
                showComments ? "bg-sky-600" : "bg-muted-foreground/40"
              )}
            >
              <span
                className={cn(
                  "inline-block h-5 w-5 transform rounded-full bg-white transition-transform",
                  showComments ? "translate-x-5" : "translate-x-1"
                )}
              />
            </button>
          </div>
        </div>

        <DialogFooter className="sm:justify-between">
          <p className="text-xs text-muted-foreground">如果下载失败，可以尝试长按预览图保存，或使用“生成分享链接”功能在系统浏览器打开下载。</p>
          <Button
            type="button"
            onClick={handleDownload}
            disabled={loading}
            className="bg-foreground text-background hover:opacity-90"
          >
            保存图片
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
