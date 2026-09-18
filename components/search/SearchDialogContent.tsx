"use client";

import { forwardRef, type ComponentPropsWithoutRef, type ElementRef } from "react";
import { DialogContent } from "@/components/ui/dialog";
import { cn } from "@/lib/utils";

export const SearchDialogContent = forwardRef<
  ElementRef<typeof DialogContent>,
  ComponentPropsWithoutRef<typeof DialogContent>
>(function SearchDialogContent({ className, ...props }, ref) {
  // Preserve the former full-results height: 40vh/300/350/400px plus dialog controls.
  return (
    <DialogContent
      {...props}
      ref={ref}
      className={cn(
        "w-[95vw] grid-rows-[auto_auto_minmax(0,1fr)_auto] overflow-hidden sm:max-w-md md:max-w-lg lg:max-w-xl",
        "h-[min(90dvh,calc(40dvh+209px))] sm:h-[min(90dvh,517px)] md:h-[min(90dvh,567px)] lg:h-[min(90dvh,617px)]",
        className,
      )}
    />
  );
});
