"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

type NoticeKind = "success" | "error" | "info";

interface ShareLinkDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  shareUrl: string;
  onNotice: (kind: NoticeKind, message: string) => void;
}

async function copyText(value: string) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  document.execCommand("copy");
  document.body.removeChild(textarea);
}

export function ShareLinkDialog({
  open,
  onOpenChange,
  shareUrl,
  onNotice,
}: ShareLinkDialogProps) {
  const [copyStatus, setCopyStatus] = useState<"idle" | "success" | "error">("idle");

  useEffect(() => {
    if (!open) {
      setCopyStatus("idle");
    }
  }, [open]);

  async function handleCopy() {
    try {
      await copyText(shareUrl);
      setCopyStatus("success");
      onNotice("success", "已生成并复制分享链接");
    } catch {
      setCopyStatus("error");
      onNotice("error", "生成分享链接失败，请手动复制");
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[95vw] max-h-[90vh] overflow-y-auto sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>生成分享链接</DialogTitle>
        </DialogHeader>

        <div className="space-y-3">
          <div className="flex items-center gap-2 rounded-full border border-border bg-muted px-2 py-1.5">
            <input
              readOnly
              value={shareUrl}
              aria-label="当前分享链接"
              className="h-8 w-full min-w-0 flex-1 bg-transparent px-2 text-xs text-foreground outline-none sm:text-sm"
            />
            <Button
              type="button"
              onClick={handleCopy}
              className="rounded-full bg-foreground px-4 py-2 text-xs font-bold text-background hover:opacity-90 sm:text-sm"
            >
              复制链接
            </Button>
          </div>

          <p className="text-xs text-muted-foreground">若未自动复制成功，可手动复制上方链接。</p>
          {copyStatus === "success" ? (
            <p className="text-xs font-semibold text-emerald-600">复制成功</p>
          ) : null}
          {copyStatus === "error" ? (
            <p className="text-xs font-semibold text-rose-600">复制失败，请手动复制上方链接。</p>
          ) : null}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
