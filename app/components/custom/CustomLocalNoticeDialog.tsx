"use client";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ToastKind } from "@/app/components/v3/InlineToast";
import { cn } from "@/lib/utils";

interface CustomLocalNoticeDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  testing: boolean;
  status: { kind: ToastKind; message: string } | null;
  onTestDownload: () => void;
  onCopyLink: () => void;
}

const statusTone: Record<ToastKind, string> = {
  success: "border-emerald-200 bg-emerald-50 text-emerald-700",
  error: "border-rose-200 bg-rose-50 text-rose-700",
  info: "border-sky-200 bg-sky-50 text-sky-700",
};

export function CustomLocalNoticeDialog({
  open,
  onOpenChange,
  testing,
  status,
  onTestDownload,
  onCopyLink,
}: CustomLocalNoticeDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[95vw] max-w-lg rounded-2xl">
        <DialogHeader>
          <DialogTitle>使用须知</DialogTitle>
          <DialogDescription className="space-y-2 pt-2 text-left">
            <p>当有实在无法找到的条目时，可以使用自定义模式来自行上传图片。</p>
            <p>自定义模式下，上传的图片不会提交到云端，也不会生成共享页面。</p>
            <p>请先测试当前浏览器能否正常保存图片；如果失败，建议复制链接到系统浏览器继续，以防填写结果最后无法保存。</p>
          </DialogDescription>
        </DialogHeader>

        {status ? (
          <div className={cn("rounded-xl border px-3 py-2 text-sm font-medium", statusTone[status.kind])}>
            {status.message}
          </div>
        ) : null}

        <DialogFooter className="block">
          <div className="grid gap-2 sm:hidden">
            <div className="grid grid-cols-2 gap-2">
              <Button type="button" variant="outline" onClick={onCopyLink} className="w-full">
                复制当前页面链接
              </Button>
              <Button type="button" onClick={onTestDownload} disabled={testing} className="w-full">
                {testing ? "测试中..." : "测试保存图片"}
              </Button>
            </div>
            <Button type="button" variant="secondary" onClick={() => onOpenChange(false)} className="w-full">
              我知道了
            </Button>
          </div>

          <div className="hidden items-center justify-between sm:flex">
            <Button type="button" variant="outline" onClick={onCopyLink}>
              复制当前页面链接
            </Button>
            <div className="flex gap-2">
              <Button type="button" onClick={onTestDownload} disabled={testing}>
                {testing ? "测试中..." : "测试保存图片"}
              </Button>
              <Button type="button" variant="secondary" onClick={() => onOpenChange(false)}>
                我知道了
              </Button>
            </div>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
