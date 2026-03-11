"use client";

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";
import { FaWeibo, FaGithub } from "react-icons/fa";
import { SiBilibili } from "react-icons/si";
import { SupportButton } from "@/components/SupportButton";

interface SiteFooterProps {
  className?: string;
}

function buildTallyEmbedUrl(value: string): string {
  try {
    const url = new URL(value);
    url.searchParams.set("transparentBackground", "1");
    url.searchParams.set("hideTitle", "1");
    url.searchParams.set("dynamicHeight", "1");
    return url.toString();
  } catch {
    return value;
  }
}

export function SiteFooter({ className }: SiteFooterProps) {
  const tallyFormUrl =
    process.env.NEXT_PUBLIC_TALLY_FORM_URL?.trim() ||
    process.env.NEXT_PUBLIC_FEEDBACK_TALLY_URL?.trim();
  const tallyEmbedUrl = tallyFormUrl ? buildTallyEmbedUrl(tallyFormUrl) : "";

  return (
    <footer
      className={cn(
        "mx-auto w-full max-w-2xl border-t border-border pt-8 text-center text-xs text-muted-foreground",
        className
      )}
    >
      <p>
        由{" "}
        <a
          href="https://bangumi.tv/"
          target="_blank"
          rel="noreferrer"
          className="font-semibold text-sky-600 hover:underline"
        >
          Bangumi
        </a>
        {" "}强力驱动
      </p>
      <p className="mt-2">
        开发者：苍旻白轮
      </p>
      <div className="mt-2 flex items-center justify-center gap-4">
        <a href="https://weibo.com/u/6571509464" target="_blank" rel="noreferrer" aria-label="微博" className="text-muted-foreground transition-colors hover:text-foreground">
          <FaWeibo className="h-5 w-5" />
        </a>
        <a href="https://space.bilibili.com/808024" target="_blank" rel="noreferrer" aria-label="哔哩哔哩" className="text-muted-foreground transition-colors hover:text-foreground">
          <SiBilibili className="h-5 w-5" />
        </a>
        <a href="https://github.com/SomiaWhiteRing" target="_blank" rel="noreferrer" aria-label="GitHub" className="text-muted-foreground transition-colors hover:text-foreground">
          <FaGithub className="h-5 w-5" />
        </a>
        <a href="https://bangumi.tv/user/whitering" target="_blank" rel="noreferrer" aria-label="Bangumi" className="text-muted-foreground transition-colors hover:text-foreground">
          <svg
            viewBox="0 0 1024 1024"
            xmlns="http://www.w3.org/2000/svg"
            className="h-5 w-5 fill-current"
            aria-hidden="true"
          >
            <path d="M228.115014 615.39997a12.299999 12.299999 0 0 0 11.354999 7.569 12.470999 12.470999 0 0 0 4.75-0.965l147.609993-61.882997a12.299999 12.299999 0 0 0 0.264-22.556999l-147.609993-66.234997a12.299999 12.299999 0 1 0-10.066999 22.443999l121.739994 54.633997-121.455994 50.906998a12.299999 12.299999 0 0 0-6.586 16.084999z m170.905992 12.564999H239.470013a12.299999 12.299999 0 0 0 0 24.601999h159.549993a12.299999 12.299999 0 0 0 0-24.601999z m0 39.494998H239.470013a12.299999 12.299999 0 0 0 0 24.601999h159.549993a12.299999 12.299999 0 0 0 0-24.601999z m473.919976-190.56799l-133.282993 58.381997a12.299999 12.299999 0 0 0-0.397 22.349999l133.301993 64.057997a12.073999 12.073999 0 0 0 5.318 1.23 12.299999 12.299999 0 0 0 5.337-23.389999l-109.155995-52.419998 108.833995-47.632997a12.299999 12.299999 0 1 0-9.954-22.576999z m4.94 151.072992H729.779989a12.299999 12.299999 0 0 0 0 24.601999H877.879982a12.299999 12.299999 0 0 0 0-24.601999z m0 39.494998H729.779989a12.299999 12.299999 0 0 0 0 24.601999H877.879982a12.299999 12.299999 0 0 0 0-24.601999zM644.865994 537.127974h-162.919993a12.281999 12.281999 0 0 0-10.709999 18.319999l81.373996 145.129993a12.299999 12.299999 0 0 0 21.459999 0l81.374996-145.129993a12.299999 12.299999 0 0 0-10.729999-18.319999z m-81.373997 132.299993L503.047 561.729973h120.888995z" />
            <path d="M891.411981 334.959984H648.404993c-6.813-15.139999-19.813999-28.385999-36.863998-38.018998L803.091986 19.283999a12.299999 12.299999 0 0 0-20.248999-13.965999L588.565996 286.872986a147.722993 147.722993 0 0 0-45.417998-7.002 151.507993 151.507993 0 0 0-31.886998 3.369L239.980013 4.712a12.299999 12.299999 0 0 0-17.542999 17.163999L485.164001 291.679986c-22.140999 9.822-39.115998 25.112999-47.309997 43.241998H132.547019a91.763996 91.763996 0 0 0-91.782996 91.782995v414.44198a91.763996 91.763996 0 0 0 91.782996 91.820995h268.023986l-19.907999 46.988998c-12.640999 29.880999 22.614999 57.094997 48.294998 37.299998l109.514995-84.288996h352.937982a91.763996 91.763996 0 0 0 91.782996-91.782995V426.742979a91.763996 91.763996 0 0 0-91.782996-91.782995z m34.839999 463.815977a60.709997 60.709997 0 0 1-60.709997 60.708997H585.670996l-97.799995 73.482996-77.003996 57.851998 24.412999-57.851998 31.016998-73.482996H198.082015a60.727997 60.727997 0 0 1-60.802997-60.746997V440.329978a60.727997 60.727997 0 0 1 60.727997-60.727997h667.459968a60.709997 60.709997 0 0 1 60.708997 60.727997z" />
          </svg>
        </a>
        <a
          href="https://github.com/SomiaWhiteRing/my9"
          target="_blank"
          rel="noreferrer"
          aria-label="GitHub Stars"
        >
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src="https://img.shields.io/github/stars/SomiaWhiteRing/my9?style=social&label=GitHub%20Stars"
            alt="GitHub Stars badge"
          />
        </a>
      </div>
      <div className="mt-3 flex flex-nowrap items-center justify-center gap-2 text-xs text-muted-foreground">
        <a
          href="https://hits.sh/my9.shatranj.space/"
          target="_blank"
          rel="noreferrer"
          aria-label="hitsh"
          className="shrink-0"
        >
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src="https://hits.sh/my9.shatranj.space.svg?style=flat-square&label=visitors"
            alt="hitsh badge"
          />
        </a>
        <span aria-hidden="true">|</span>
        <Dialog>
          <DialogTrigger asChild>
            <button
              type="button"
              className="bg-transparent p-0 text-muted-foreground transition-colors hover:text-foreground hover:underline"
            >
              吐槽反馈
            </button>
          </DialogTrigger>
          <DialogContent className="w-[96vw] max-w-3xl gap-0 overflow-hidden rounded-2xl p-0">
            <DialogHeader className="sr-only">
              <DialogTitle>吐槽反馈</DialogTitle>
              <DialogDescription>Tally 反馈表单</DialogDescription>
            </DialogHeader>
            {tallyFormUrl ? (
              <iframe
                src={tallyEmbedUrl}
                title="Tally 反馈表单"
                className="h-[78vh] min-h-[520px] w-full border-0"
                loading="lazy"
              />
            ) : (
              <p className="p-6 text-sm text-muted-foreground">
                暂未配置 Tally 表单。请在 <code>.env.local</code> 设置
                <code> NEXT_PUBLIC_TALLY_FORM_URL</code>。
              </p>
            )}
          </DialogContent>
        </Dialog>
        <span aria-hidden="true">|</span>
        <SupportButton/>
      </div>
    </footer>
  );
}
