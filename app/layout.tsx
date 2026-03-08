import type React from "react";
import type { Metadata } from "next";
import { Noto_Sans_SC } from "next/font/google";
import { Analytics } from "@vercel/analytics/react";
import GoogleAnalytics from "@/components/GoogleAnalytics";
import "./globals.css";

const notoSansSC = Noto_Sans_SC({ subsets: ["latin"], weight: ["400", "500", "700", "900"] });

export const metadata: Metadata = {
  metadataBase: new URL("https://my9.shatranj.space"),
  title: "构成我的九部",
  description: "用 Bangumi 搜索挑选 9 部最能代表你的作品，生成并分享你的「构成我的九部」页面。",
  alternates: {
    canonical: "/",
  },
  openGraph: {
    type: "website",
    locale: "zh_CN",
    title: "构成我的九部",
    description: "用 Bangumi 搜索挑选 9 部最能代表你的作品，生成并分享你的「构成我的九部」页面。",
    url: "/",
    siteName: "构成我的九部",
  },
  twitter: {
    card: "summary_large_image",
    title: "构成我的九部",
    description: "用 Bangumi 搜索挑选 9 部最能代表你的作品，生成并分享你的「构成我的九部」页面。",
  },
  verification: {
    google: "swtOMxSQC6Dfn-w4YtMQ3OFH4SZz00Blcd6FI0qMgJc",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <head>
        <GoogleAnalytics />
      </head>
      <body className={notoSansSC.className}>
        <Analytics />
        {children}
      </body>
    </html>
  );
}
