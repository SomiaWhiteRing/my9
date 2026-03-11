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
import { cn } from "@/lib/utils";
import { ShareGame } from "@/lib/share/types";
import {
  downloadBlob,
  generateStandardShareImageBlob,
  generateEnhancedShareImageBlob,
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
  return `${title || "构成我的九部"}.png`;
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
  const [withQr, setWithQr] = useState(true);
  const [showNames, setShowNames] = useState(true);
  const [loading, setLoading] = useState(false);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [previewBlob, setPreviewBlob] = useState<Blob | null>(null);
  const [previewError, setPreviewError] = useState("");
  const requestIdRef = useRef(0);

  useEffect(() => {
    if (!open) {
      setWithQr(true);
      setShowNames(true);
      setLoading(false);
      setPreviewBlob(null);
      setPreviewError("");
      setPreviewUrl((prev) => {
        if (prev) URL.revokeObjectURL(prev);
        return null;
      });
      return;
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const requestId = ++requestIdRef.current;

    async function loadPreview() {
      setLoading(true);
      setPreviewError("");
      try {
        const blob = withQr
          ? await generateEnhancedShareImageBlob({
              kind,
              shareId,
              title,
              games,
              creatorName,
              showNames,
            })
          : await generateStandardShareImageBlob({ games, creatorName, showNames });

        if (requestId !== requestIdRef.current) return;
        const nextUrl = URL.createObjectURL(blob);
        setPreviewBlob(blob);
        setPreviewUrl((prev) => {
          if (prev) URL.revokeObjectURL(prev);
          return nextUrl;
        });
      } catch {
        if (requestId !== requestIdRef.current) return;
        setPreviewBlob(null);
        setPreviewError("图片生成失败，请稍后重试");
        setPreviewUrl((prev) => {
          if (prev) URL.revokeObjectURL(prev);
          return null;
        });
      } finally {
        if (requestId === requestIdRef.current) {
          setLoading(false);
        }
      }
    }

    loadPreview();
  }, [creatorName, games, kind, open, shareId, showNames, title, withQr]);

  async function handleDownload() {
    try {
      const blob =
        previewBlob ||
        (withQr
          ? await generateEnhancedShareImageBlob({
              kind,
              shareId,
              title,
              games,
              creatorName,
              showNames,
            })
          : await generateStandardShareImageBlob({ games, creatorName, showNames }));
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
              <p className="text-sm font-semibold text-foreground">附带分享链接</p>
            </div>
            <button
              type="button"
              role="switch"
              aria-checked={withQr}
              aria-label="附带分享链接"
              onClick={() => setWithQr((value) => !value)}
              className={cn(
                "relative inline-flex h-6 w-11 items-center rounded-full transition-colors",
                withQr ? "bg-sky-600" : "bg-muted-foreground/40"
              )}
            >
              <span
                className={cn(
                  "inline-block h-5 w-5 transform rounded-full bg-white transition-transform",
                  withQr ? "translate-x-5" : "translate-x-1"
                )}
              />
            </button>
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
        </div>

        <DialogFooter className="sm:justify-between">
          <p className="text-xs text-muted-foreground">如果下载失败，可以尝试长按预览图保存。</p>
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
