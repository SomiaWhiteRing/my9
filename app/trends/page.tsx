import TrendsClientPage from "@/app/components/TrendsClientPage";
import { createPageMetadata } from "@/lib/page-metadata";



export const metadata = createPageMetadata(
  "构成大家的作品",
  "查看 My9「大家的构成」作品排行榜，了解大家选择的游戏、动画、电影、音乐等内容。",
  "/trends",
);

export default TrendsClientPage;
