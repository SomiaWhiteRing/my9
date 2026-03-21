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
  { date: "2026-03-21", name: "CheungYeung", amount: "50", message: "title能加修饰词自定义就好了（开发者注：刚好又想做又缺动力……！）" },
  { date: "2026-03-21", name: "R.M.T.", amount: "3", message: "但搜出来好像和电视剧似的，我看它那个图标类型是书籍啊（开发者注：现实人物和书籍都缺少好用的API呢……）" },
  { date: "2026-03-21", name: "R.M.T.", amount: "3", message: "却搜不到沈从文和叶圣陶。作品也是，想搜查拉图斯特拉和朝花夕拾" },
  { date: "2026-03-21", name: "R.M.T.", amount: "3", message: "希望后面的人物和作品的搜索范围再大一点，能搜到郑渊洁和鲁迅，" },
  { date: "2026-03-21", name: "匿名", amount: "20", message: "涉猎真广₍₍Ϡ(੭•̀ω•́)੭✧⃛" },
  { date: "2026-03-21", name: "匿名", amount: "1.88", message: "很有意思的网站！" },
  { date: "2026-03-21", name: "Raistlin", amount: "3", message: "" },
  { date: "2026-03-21", name: "匿名", amount: "2.33", message: "加个查询作品选定数的功能+1。作者加油呀" },
  { date: "2026-03-21", name: "酷狗_🎶", amount: "6", message: "很有趣的想法💡" },
  { date: "2026-03-21", name: "酷狗_🎶", amount: "6", message: "" },
  { date: "2026-03-21", name: "酷狗_🎶", amount: "6", message: "" },
  { date: "2026-03-21", name: "匿名", amount: "3", message: "快修快修快把分享按钮端上来" },
  { date: "2026-03-21", name: "wabbajack16", amount: "6", message: "" },
  { date: "2026-03-20", name: "学海无涯", amount: "2", message: "作者加油啊，路过随两块钱份子" },
  { date: "2026-03-19", name: "匿名", amount: "3", message: "" },
  { date: "2026-03-19", name: "匿名", amount: "6", message: "可以加个查询作品选定数的功能吗，想看看自己选的作品能有多冷（开发者注：最近在优化数据库，之后可能会考虑……）" },
  { date: "2026-03-19", name: "烂神植旭富", amount: "10", message: "继续支持一下" },
  { date: "2026-03-19", name: "知不可乎骤得", amount: "6", message: "希望能坚持下去" },
  { date: "2026-03-19", name: "纷飞的蒲公英", amount: "6", message: "" },
  { date: "2026-03-18", name: "Recroaed", amount: "3", message: "" },
  { date: "2026-03-17", name: "匿名", amount: "10", message: "好多专辑搜不到……TT" },
  { date: "2026-03-17", name: "匿名", amount: "10", message: "想看我的英雄學院小众角色" },
  { date: "2026-03-17", name: "z13", amount: "3", message: "轻小说想找再生勇士没找到有点可惜，但是已经很好了，加油！" },
  { date: "2026-03-17", name: "匿名", amount: "1", message: "ww感谢" },
  { date: "2026-03-17", name: "Felix🔥", amount: "3", message: "" },
  { date: "2026-03-17", name: "烂神植旭富", amount: "3", message: "为爱发电，支持一下" },
  { date: "2026-03-16", name: "匿名", amount: "1", message: "" },
  { date: "2026-03-16", name: "釿緸", amount: "10", message: "👍" },
  { date: "2026-03-16", name: "早", amount: "3", message: "" },
  { date: "2026-03-16", name: "Crybaby", amount: "3", message: "" },
  { date: "2026-03-15", name: "匿名", amount: "1", message: "" },
  { date: "2026-03-15", name: "Zeek🤗", amount: "6", message: "希望能挽救真实的评价体系！" },
  { date: "2026-03-15", name: "明下", amount: "6", message: "希望能有书籍功能T T感谢作者" },
  { date: "2026-03-15", name: "Miki", amount: "3", message: "" },
  { date: "2026-03-15", name: "ay4ne", amount: "1", message: "干巴爹" },
  { date: "2026-03-15", name: "匿名", amount: "0.5", message: "" },
  { date: "2026-03-15", name: "匿名", amount: "10", message: "有点走马灯了" },
  { date: "2026-03-15", name: "我要看かぐや！", amount: "3", message: "这样对大家分享喜爱的事物或许也不错呢" },
  { date: "2026-03-15", name: "海月", amount: "3", message: "加油哦！" },
  { date: "2026-03-14", name: "陌离ﾟ", amount: "0.88", message: "" },
  { date: "2026-03-14", name: "*偷摸亲(｡•́__ก̀｡)*", amount: "20", message: "感谢制作(≧∀≦)ゞ🎉" },
  { date: "2026-03-14", name: "汐", amount: "20", message: "老大老大真的不能自定义图片吗喵" },
  { date: "2026-03-14", name: "惊蟄正中央｡:.ﾟ", amount: "3", message: "辛苦了！" },
  { date: "2026-03-14", name: "夷蹴而就", amount: "50", message: "成功的经营，希望有gal专项" },
  { date: "2026-03-14", name: "黛雪若兮", amount: "20", message: "感谢制作，希望有更多的书籍，专辑，电影！祝你开心^_^" },
  { date: "2026-03-14", name: "Euphoria", amount: "10", message: "作者加油鸭" },
  { date: "2026-03-14", name: "CC(´-ι_-｀)", amount: "6", message: "" },
  { date: "2026-03-14", name: "咒怨", amount: "10", message: "" },
  { date: "2026-03-14", name: "蜃気楼", amount: "5.2", message: "" },
  { date: "2026-03-13", name: "孔睦瑞", amount: "1", message: "云吸猫" },
  { date: "2026-03-13", name: "γοj1pR", amount: "10", message: "辛苦了——" },
  { date: "2026-03-13", name: "匿名", amount: "3", message: "音乐很多小众专辑缺失，后续可以支持让用户自行添加图片和文字" },
  { date: "2026-03-13", name: "官官", amount: "3", message: "阿巴阿巴阿巴" },
  { date: "2026-03-13", name: "匿名", amount: "1", message: "" },
  { date: "2026-03-13", name: "r1chard", amount: "10", message: "能不能出一个构成我的9本本子（（开发者注：商业本的话已经有了（小声）" },
  { date: "2026-03-13", name: "羽喵", amount: "20", message: "好活当赏" },
  { date: "2026-03-13", name: "春华宜照灼", amount: "3", message: "" },
  { date: "2026-03-13", name: "匿名", amount: "10", message: "" },
  { date: "2026-03-13", name: "匿名", amount: "1", message: "" },
  { date: "2026-03-13", name: "公子小白", amount: "3", message: "保存完居然不知道要分享给谁了" },
  { date: "2026-03-13", name: "圆圆", amount: "3", message: "加油" },
  { date: "2026-03-13", name: "麦高芬", amount: "10", message: "好厉害" },
  { date: "2026-03-13", name: "最脆弱之人", amount: "10", message: "玩的很开心捏 许愿一个构成的角色" },
  { date: "2026-03-13", name: "玮鸿", amount: "8.88", message: "感谢作者赞美作者！前两天刚留言说想要电影和音乐的九宫格" },
  { date: "2026-03-12", name: "游戏者幻游", amount: "3", message: "6" },
  { date: "2026-03-12", name: "Mason", amount: "3", message: "" },
  { date: "2026-03-12", name: "匿名", amount: "6", message: "用来做记录真的太好了🥹" },
  { date: "2026-03-12", name: "J3rry", amount: "88", message: "" },
  { date: "2026-03-12", name: "Raving", amount: "6", message: "希望多加些经典小说和电影，我也让我爸填填" },
  { date: "2026-03-12", name: "World Collide", amount: "6", message: "感谢这么好的网站！可不可以再加个音乐剧Hamilton哇" },
  { date: "2026-03-12", name: "不想起床bot", amount: "10", message: "居然有妄想症赏10元" },
  { date: "2026-03-12", name: "💽", amount: "6", message: "加油呀" },
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
          <div className="mt-3 flex justify-center">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src={fallbackWechatPayQrUrl}
              alt="微信赞赏码"
              className="h-48 w-48 rounded-lg border border-border object-contain md:h-60 md:w-60"
            />
          </div>
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

          <div className="mt-4 border-t border-border pt-4">
            <h4 className="text-sm font-semibold text-foreground">特别鸣谢</h4>
            <p className="mt-1 text-sm text-muted-foreground">感谢以下开发者对核心功能演进的贡献：</p>
            <ul className="mt-2 space-y-1.5 text-sm text-muted-foreground">
              <li>
                感谢{" "}
                <a
                  href="https://github.com/maxchang3"
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700 dark:text-sky-400 dark:decoration-sky-500 dark:hover:text-sky-300"
                >
                  @maxchang3
                </a>{" "}
                贡献卡片拖拽功能。
              </li>
              <li>
                感谢{" "}
                <a
                  href="https://github.com/MiQieR"
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700 dark:text-sky-400 dark:decoration-sky-500 dark:hover:text-sky-300"
                >
                  @MiQieR
                </a>{" "}
                贡献电影/电视剧查询功能。
              </li>
              <li>
                感谢{" "}
                <a
                  href="https://github.com/wanshuai12138"
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700 dark:text-sky-400 dark:decoration-sky-500 dark:hover:text-sky-300"
                >
                  @wanshuai12138
                </a>{" "}
                贡献单曲/专辑查询功能。
              </li>
              <li>
                感谢{" "}
                <a
                  href="https://github.com/AlanWanco"
                  target="_blank"
                  rel="noreferrer"
                  className="font-semibold text-sky-600 underline decoration-sky-300 underline-offset-2 hover:text-sky-700 dark:text-sky-400 dark:decoration-sky-500 dark:hover:text-sky-300"
                >
                  @AlanWanco
                </a>{" "}
                贡献角色/人物查询功能。
              </li>
            </ul>
          </div>
        </section>
      </DialogContent>
    </Dialog>
  );
}
