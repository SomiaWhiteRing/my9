import type { Metadata } from "next";
import My9CustomApp from "@/app/components/My9CustomApp";

export const metadata: Metadata = {
  title: "自定义模式",
};

export default function CustomPage() {
  return <My9CustomApp />;
}
