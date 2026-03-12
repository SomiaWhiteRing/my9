"use client";

import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { SHARE_COUNT_SNAPSHOT } from "@/lib/generated/share-count-snapshot";

const donationAcknowledgements: Array<{
  date: string;
  name: string;
  amount: string;
  message: string;
}> = [
  { date: "2026-03-12", name: "游戏者幻游", amount: "3", message: "6" },
  { date: "2026-03-12", name: "Mason", amount: "3", message: "" },
  { date: "2026-03-12", name: "匿名", amount: "6", message: "用来做记录真的太好了🥹" },
  { date: "2026-03-12", name: "J3rry", amount: "88", message: "" },
  { date: "2026-03-12", name: "Raving", amount: "6", message: "希望多加些经典小说和电影，我也让我爸填填" },
  { date: "2026-03-12", name: "World Collide", amount: "6", message: "感谢这么好的网站！可不可以再加个音乐剧Hamilton哇" },
  { date: "2026-03-12", name: "不想起床bot", amount: "10", message: "居然有妄想症赏10元" },
  { date: "2026-03-12", name: "💿", amount: "6", message: "加油呀" },
  { date: "2026-03-12", name: "17", amount: "5.2", message: "感谢老师带我回忆了一次童年过往的美好" },
  { date: "2026-03-12", name: "匿名", amount: "50", message: "太棒啦宝宝" },
  { date: "2026-03-12", name: "许喆隆", amount: "3", message: "" },
  { date: "2026-03-12", name: "太阳即为正義", amount: "3", message: "DITF国家队天下第一" },
  { date: "2026-03-12", name: "妍子", amount: "10", message: "" },
  { date: "2026-03-12", name: "Ying", amount: "100", message: "请加微信号*** 谢谢" },
  { date: "2026-03-12", name: "匿名", amount: "20", message: "老大谢谢喵" },
  { date: "2026-03-12", name: "梦游到老地方", amount: "10", message: "好全的库 非常感谢" },
  { date: "2026-03-12", name: "匿名", amount: "3", message: "" },
  { date: "2026-03-12", name: "森", amount: "3", message: "" },
  { date: "2026-03-12", name: "匿名", amount: "6", message: "怎么没有新破天一剑啊啊啊 究极老mmo了（开发者注：已在bangumi添加条目）" },
  { date: "2026-03-12", name: "榴莲千层", amount: "6", message: "" },
  { date: "2026-03-12", name: "Olympics🍭", amount: "6", message: "谢谢！" },
  { date: "2026-03-12", name: "DreamHe", amount: "100", message: "" },
  { date: "2026-03-12", name: "贺胖晨", amount: "3", message: "谢谢大兄弟" },
  { date: "2026-03-12", name: "匿名", amount: "6", message: "" },
  { date: "2026-03-12", name: "csan", amount: "10", message: "好全的作品库！" },
  { date: "2026-03-12", name: "梁亮", amount: "1", message: "" },
  { date: "2026-03-11", name: "鸽", amount: "1", message: "" },
  { date: "2026-03-11", name: "NW.哈托客", amount: "20", message: "回想作品时，与之相关的点滴就接连浮现在脑海中，令人怀念……" },
  { date: "2026-03-11", name: "晚漪", amount: "10", message: "助力！" },
  { date: "2026-03-11", name: "🍤FishFry酥脆版", amount: "1", message: "一下子推给了好多朋友玩 感谢你呀" },
  { date: "2026-03-11", name: "鞋子", amount: "5", message: "谢谢你" },
  { date: "2026-03-11", name: "BK", amount: "1", message: "" },
  { date: "2026-03-11", name: "匿名", amount: "5", message: "" },
  { date: "2026-03-11", name: "Eren", amount: "5", message: "2026加油" },
  { date: "2026-03-11", name: "my", amount: "1", message: "天才来的吧！想法太棒了！" },
  { date: "2026-03-11", name: "一哥酱", amount: "1", message: "能不能加一个电影hhh超爱的" },
  { date: "2026-03-11", name: "匿名", amount: "20", message: "🔥旻🔥" },
  { date: "2026-03-11", name: "匿名", amount: "5.2", message: "" },
  { date: "2026-03-11", name: "小木", amount: "2000", message: "（开发者注：我永恒的爱与感谢……）" },
  { date: "2026-03-11", name: "匿名", amount: "1", message: "" },
  { date: "2026-03-11", name: "金水168", amount: "50", message: "旻，屹立于大地之上！" },
  { date: "2026-03-11", name: "孙虎虎的🐯【收🦴版", amount: "20", message: "喝个奶茶再接再厉🤣" },
  { date: "2026-03-11", name: "无铭", amount: "10", message: "" },
  { date: "2026-03-11", name: "地球重置中", amount: "5.2", message: "老大加油！想问问网站搜索动画能加一个炫斗战轮吗？（开发者注：已在bangumi添加条目）​" },
  { date: "2026-03-11", name: "カノープス", amount: "100", message: "坏了我微信基本没存啥钱，下次加个支付宝的吧.jpg ​​​" },
  { date: "2026-03-11", name: "谷般", amount: "50", message: "请阿旻老师吃kfc" },
  { date: "2026-03-11", name: "狂怒使者MK1", amount: "20", message: "可惜保存图片失效了" },
  { date: "2026-03-11", name: "Star-t", amount: "52", message: "Love explosions！" },
  { date: "2026-03-11", name: "AL-1S", amount: "100", message: "希望旻妈妈增加拖拽功能！" },
  { date: "2026-03-11", name: "🥔🥔🥔🥔", amount: "1", message: "加油" },
  { date: "2026-03-10", name: "Veige", amount: "50", message: "虽然今天不是星期四但是看起来已经足够疯狂了" },
  { date: "2026-03-10", name: "国栋", amount: "79.2", message: "" },
  { date: "2026-03-10", name: "Jackpot", amount: "100", message: "加油啊旻妈妈……" },
];

const collectedCountFromEnv = process.env.NEXT_PUBLIC_SHARE_COUNT
  ? parseInt(process.env.NEXT_PUBLIC_SHARE_COUNT, 10)
  : null;
const collectedCount = SHARE_COUNT_SNAPSHOT > 0 ? SHARE_COUNT_SNAPSHOT : collectedCountFromEnv;

export function SupportButton() {
  const wechatPayQrUrl = process.env.NEXT_PUBLIC_WECHAT_PAY_QR_URL?.trim();
  const fallbackWechatPayQrUrl = "/wechatpay.png";
  const [wechatPayQrSrc, setWechatPayQrSrc] = useState(wechatPayQrUrl ?? fallbackWechatPayQrUrl);

  return (
    <Dialog>
      <DialogTrigger asChild>
        <button
          type="button"
          className="bg-transparent p-0 text-sm text-red-600 transition-colors hover:text-red-500 dark:text-red-400 dark:hover:text-red-300 hover:underline"
        >
          支援开发者
        </button>
      </DialogTrigger>
      <DialogContent className="w-[calc(100vw-1rem)] max-w-xl max-h-[88dvh] overflow-y-auto rounded-2xl p-4 md:w-[92vw] md:max-h-[85vh] md:p-5">
        <DialogHeader className="text-left">
          <DialogTitle>感谢支持</DialogTitle>
          <DialogDescription className="space-y-1.5 text-muted-foreground">
            <span className="block">
              本项目上线至今已经建构了{" "}
              <span className="font-semibold text-sky-600">
                {collectedCount === null ? "..." : collectedCount.toLocaleString("zh-CN")}
              </span>{" "}
              份大家的构成！可喜可贺（啪叽啪叽）
            </span>
            <span className="block">
              但与此同时，意料之外的流行也让服务器开始不堪重负……<span style={{ textDecoration: "line-through" }}>（我草怎么真的炸了）</span>
            </span>
            <span className="block">
              虽然在努力想办法自己解决，如果有谁愿意帮忙就太好了呢。
            </span>
            <span className="block">
              也非常欢迎通过
              <a
                href="https://github.com/SomiaWhiteRing/my9"
                target="_blank"
                rel="noreferrer"
                className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700 dark:text-sky-400 dark:decoration-sky-500 dark:hover:text-sky-300"
              >
                在 GitHub 点 Star
              </a>
              提供精神支持！
            </span>
          </DialogDescription>
        </DialogHeader>
        {wechatPayQrUrl ? (
          <div className="mt-3 flex justify-center">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src={wechatPayQrSrc}
              alt="微信赞赏码"
              className="h-48 w-48 rounded-lg border border-border object-contain md:h-60 md:w-60"
              onError={() => {
                setWechatPayQrSrc((current) =>
                  current === fallbackWechatPayQrUrl ? current : fallbackWechatPayQrUrl
                );
              }}
            />
          </div>
        ) : (
          <p className="mt-3 text-left text-sm text-muted-foreground">
            暂未配置微信赞赏码。请在 <code>.env.local</code> 设置
            <code> NEXT_PUBLIC_WECHAT_PAY_QR_URL</code>。
          </p>
        )}
        <section className="mt-5 border-t border-border pt-4">
          <h3 className="text-sm font-semibold text-foreground">鸣谢名单</h3>
          <p className="mt-1 text-sm text-muted-foreground">
            非常非常非常感谢以下各位的支持让站点能够运营下来……（排序从新到旧）
          </p>
          <p className="mt-1 text-sm text-muted-foreground">
            各位的支持会成为站点存续的基石和我更新维护的动力！
          </p>
          <div className="mt-3 space-y-2 md:hidden">
            {donationAcknowledgements.map((item, index) => (
              <article
                key={`${item.date}-${item.amount}-${index}`}
                className="rounded-lg border border-border bg-card p-3"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-semibold text-card-foreground">{item.name}</p>
                    <p className="mt-0.5 text-xs text-muted-foreground">{item.date}</p>
                  </div>
                  <p className="shrink-0 text-sm font-bold text-foreground">¥{item.amount}</p>
                </div>
                {item.message ? (
                  <p className="mt-2 text-xs leading-5 text-muted-foreground break-words">{item.message}</p>
                ) : null}
              </article>
            ))}
          </div>

          <div className="mt-3 hidden overflow-hidden rounded-lg border border-border md:block">
            <table className="w-full table-fixed text-left text-xs text-muted-foreground">
              <thead className="bg-muted text-[11px] font-semibold text-muted-foreground">
                <tr>
                  <th className="w-20 px-3 py-2">打赏日期</th>
                  <th className="w-32 px-3 py-2">打赏人</th>
                  <th className="w-20 px-3 py-2">打赏金额</th>
                  <th className="px-3 py-2">附言</th>
                </tr>
              </thead>
              <tbody>
                {donationAcknowledgements.map((item, index) => (
                  <tr
                    key={`${item.date}-${item.amount}-${index}`}
                    className="border-t border-border/70"
                  >
                    <td className="px-3 py-2 align-top whitespace-nowrap font-medium">
                      {item.date}
                    </td>
                    <td className="px-3 py-2 align-top whitespace-nowrap">
                      {item.name}
                    </td>
                    <td className="px-3 py-2 align-top whitespace-nowrap font-semibold">
                      {item.amount}
                    </td>
                    <td className="px-3 py-2 align-top break-words">
                      {item.message}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </DialogContent>
    </Dialog>
  );
}
