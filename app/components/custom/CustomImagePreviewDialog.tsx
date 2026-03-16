"use client";

import Image from "next/image";
import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { CustomEntry, getDefaultCustomExportTitle } from "@/lib/custom/types";
import { getPublicSiteUrl } from "@/lib/site-url";
import { cn } from "@/lib/utils";
import {
  downloadBlob,
  generateStandardCustomShareImageBlob,
  generateCustomShareImageBlob,
} from "@/utils/image/exportShareImage";

type NoticeKind = "success" | "error" | "info";

interface CustomImagePreviewDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  entries: Array<CustomEntry | null>;
  creatorName?: string | null;
  onNotice: (kind: NoticeKind, message: string) => void;
}

function buildFileName(title: string) {
  const sanitized = title
    .trim()
    .replace(/[\\/:*?"<>|]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return sanitized ? `${sanitized}.png` : "my9-custom.png";
}

function resolveCustomModeUrl() {
  if (typeof window === "undefined") {
    return `${getPublicSiteUrl()}/custom`;
  }

  const localHosts = new Set(["localhost", "127.0.0.1", "0.0.0.0"]);
  if (localHosts.has(window.location.hostname)) {
    return `${window.location.origin}/custom`;
  }
  return `${getPublicSiteUrl()}/custom`;
}

export function CustomImagePreviewDialog({
  open,
  onOpenChange,
  entries,
  creatorName,
  onNotice,
}: CustomImagePreviewDialogProps) {
  const defaultTitle = getDefaultCustomExportTitle(creatorName);
  const [title, setTitle] = useState("");
  const [withQr, setWithQr] = useState(true);
  const [showNames, setShowNames] = useState(true);
  const [loading, setLoading] = useState(false);
  const [previewUrl, setPreviewUrl] = useState<string | null>(null);
  const [previewBlob, setPreviewBlob] = useState<Blob | null>(null);
  const [previewError, setPreviewError] = useState("");
  const requestIdRef = useRef(0);
  const previewUrlRef = useRef<string | null>(null);

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
      setTitle("");
      setWithQr(true);
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
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const requestId = ++requestIdRef.current;
    const resolvedTitle = title.trim() || defaultTitle;

    async function loadPreview() {
      setLoading(true);
      setPreviewError("");
      try {
        const blob = withQr
          ? await generateCustomShareImageBlob({
              title: resolvedTitle,
              qrUrl: resolveCustomModeUrl(),
              entries,
              showNames,
            })
          : await generateStandardCustomShareImageBlob({
              entries,
              showNames,
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
  }, [defaultTitle, entries, open, showNames, title, withQr]);

  async function handleDownload() {
    const resolvedTitle = title.trim() || defaultTitle;

    try {
      const blob =
        previewBlob ||
        (withQr
          ? await generateCustomShareImageBlob({
              title: resolvedTitle,
              qrUrl: resolveCustomModeUrl(),
              entries,
              showNames,
            })
          : await generateStandardCustomShareImageBlob({
              entries,
              showNames,
            }));
      downloadBlob(blob, buildFileName(withQr ? resolvedTitle : ""));
    } catch {
      onNotice("info", "下载失败，请长按预览图保存，或改用系统浏览器。");
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[95vw] max-h-[90vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>生成图片</DialogTitle>
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
                  alt="自定义图片预览"
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

          <div className="overflow-hidden rounded-xl border border-border bg-muted">
            <div className="flex items-center justify-between px-3 py-2.5">
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

            {withQr ? (
              <div className="space-y-2 border-t border-border px-3 py-3">
                <p className="text-xs font-medium text-muted-foreground">自定义标题</p>
                <Input
                  id="custom-export-title"
                  aria-label="自定义标题"
                  value={title}
                  onChange={(event) => setTitle(event.target.value.slice(0, 60))}
                  placeholder={defaultTitle}
                  className="bg-background"
                />
              </div>
            ) : null}
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
