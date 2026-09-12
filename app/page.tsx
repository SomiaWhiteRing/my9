import HomeKindEntry from "@/app/components/HomeKindEntry";
import { createPageMetadata } from "@/lib/page-metadata";
import { getServerSiteUrl } from "@/lib/site-url";

export const metadata = createPageMetadata(
  "构成我的九部游戏",
  "My9「构成我的」九宫格生成器：挑选代表你的游戏、动画、电影、漫画、音乐等作品，添加评论，生成分享页面与图片；也支持角色、人物和自定义模式。",
  "/",
);

export default function HomePage() {
  const siteUrl = getServerSiteUrl();
  const website = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    "@id": `${siteUrl}/#website`,
    url: `${siteUrl}/`,
    name: "构成我的九部作品",
    alternateName: "My9",
    inLanguage: "zh-CN",
  };

  return (
    <>
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(website).replace(/</g, "\\u003c") }}
      />
      <HomeKindEntry />
    </>
  );
}
