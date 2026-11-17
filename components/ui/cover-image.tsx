import type { ImgHTMLAttributes } from "react";

type Props = ImgHTMLAttributes<HTMLImageElement> & { fill?: boolean; alt: string };

export function CoverImage({ fill, style, loading = "lazy", ...props }: Props) {
  return <img {...props} loading={loading} decoding="async" style={fill
    ? { position: "absolute", inset: 0, height: "100%", width: "100%", ...style }
    : style} />;
}
