import type { Metadata } from "next";
import HomeKindEntry from "@/app/components/HomeKindEntry";

export const metadata: Metadata = {
  title: "构成我的九部游戏",
};

export default function HomePage() {
  return <HomeKindEntry />;
}
