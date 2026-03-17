"use client";

import { useEffect, useState } from "react";
import Cropper, { type Area } from "react-easy-crop";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";

interface CustomImageCropDialogProps {
  open: boolean;
  imageSrc: string | null;
  fileName: string;
  onOpenChange: (open: boolean) => void;
  onConfirm: (coverData: string, titleSuggestion: string) => void;
}

async function createImage(src: string): Promise<HTMLImageElement> {
  const image = new Image();
  image.src = src;
  await new Promise<void>((resolve, reject) => {
    image.onload = () => resolve();
    image.onerror = () => reject(new Error("图片加载失败"));
  });
  return image;
}

async function cropImageToDataUrl(imageSrc: string, croppedAreaPixels: Area): Promise<string> {
  const image = await createImage(imageSrc);
  const canvas = document.createElement("canvas");
  canvas.width = 720;
  canvas.height = 960;

  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("无法创建裁切画布");
  }

  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.drawImage(
    image,
    croppedAreaPixels.x,
    croppedAreaPixels.y,
    croppedAreaPixels.width,
    croppedAreaPixels.height,
    0,
    0,
    canvas.width,
    canvas.height
  );

  return canvas.toDataURL("image/jpeg", 0.92);
}

function stripExtension(fileName: string) {
  return fileName.replace(/\.[^.]+$/, "").trim();
}

export function CustomImageCropDialog({
  open,
  imageSrc,
  fileName,
  onOpenChange,
  onConfirm,
}: CustomImageCropDialogProps) {
  const [crop, setCrop] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [croppedAreaPixels, setCroppedAreaPixels] = useState<Area | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setCrop({ x: 0, y: 0 });
    setZoom(1);
    setCroppedAreaPixels(null);
    setSaving(false);
  }, [open, imageSrc]);

  async function handleConfirm() {
    if (!imageSrc || !croppedAreaPixels) return;
    setSaving(true);
    try {
      const coverData = await cropImageToDataUrl(imageSrc, croppedAreaPixels);
      onConfirm(coverData, stripExtension(fileName));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[96vw] max-w-3xl rounded-2xl p-4 data-[state=open]:zoom-in-100 data-[state=closed]:zoom-out-100 sm:p-6">
        <DialogHeader>
          <DialogTitle>裁切上传图片</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <div className="relative h-[52vh] min-h-[320px] overflow-hidden rounded-2xl bg-slate-950">
            {imageSrc ? (
              <Cropper
                image={imageSrc}
                crop={crop}
                zoom={zoom}
                aspect={3 / 4}
                showGrid
                objectFit="contain"
                onCropChange={setCrop}
                onZoomChange={setZoom}
                onCropComplete={(_, croppedPixels) => setCroppedAreaPixels(croppedPixels)}
              />
            ) : (
              <div className="flex h-full items-center justify-center text-sm text-slate-300">图片载入中...</div>
            )}
          </div>

          <div className="space-y-2 rounded-2xl border border-border bg-muted/50 px-4 py-3">
            <div className="flex items-center justify-between text-sm font-medium text-foreground">
              <span>缩放</span>
              <span>{zoom.toFixed(2)}x</span>
            </div>
            <input
              type="range"
              min={1}
              max={3}
              step={0.01}
              value={zoom}
              onChange={(event) => setZoom(Number(event.target.value))}
              className="h-2 w-full cursor-pointer accent-sky-600"
            />
          </div>
        </div>

        <DialogFooter className="gap-2 sm:justify-between">
          <div className="flex gap-2">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              取消
            </Button>
            <Button
              type="button"
              onClick={handleConfirm}
              disabled={!imageSrc || !croppedAreaPixels || saving}
              className="bg-foreground text-background hover:opacity-90"
            >
              {saving ? "处理中..." : "确认使用"}
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
