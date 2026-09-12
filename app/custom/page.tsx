import My9CustomApp from "@/app/components/My9CustomApp";
import { createPageMetadata } from "@/lib/page-metadata";

export const metadata = createPageMetadata(
  "自定义模式",
  "使用 My9 自定义模式创建属于你自己的构成，选择内容、添加评论，生成并分享你的作品页面与图片。",
  "/custom",
);

export default function CustomPage() {
  return <My9CustomApp />;
}
