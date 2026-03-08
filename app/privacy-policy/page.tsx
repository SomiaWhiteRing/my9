import { LegalDocumentPage } from "@/components/legal/LegalDocumentPage";

export const metadata = {
  title: "隐私政策",
};

export default function PrivacyPolicyPage() {
  const paragraphs = [
    "本站仅提供“构成我的九部”生成与分享功能。你提交的分享数据（昵称、9部内容、评论）会被长期保存，用于分享展示与趋势统计。",
    "我们不会主动收集与展示你的真实身份信息，不会在页面公开你的 IP、邮箱或手机号。",
    "第三方服务（如 Vercel 平台统计、分享平台跳转）可能记录其必要日志，详情请参考对应平台隐私政策。",
    "继续使用本服务即表示你理解并同意上述处理方式。",
  ];

  return (
    <LegalDocumentPage title="隐私政策" paragraphs={paragraphs} />
  );
}
