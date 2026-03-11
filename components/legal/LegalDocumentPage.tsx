import Link from "next/link";

interface LegalDocumentPageProps {
  title: string;
  paragraphs: string[];
}

export function LegalDocumentPage({ title, paragraphs }: LegalDocumentPageProps) {
  return (
    <main className="min-h-screen bg-background px-4 py-10 text-foreground">
      <article className="mx-auto w-full max-w-3xl rounded-xl border border-border bg-card p-6 shadow-sm">
        <h1 className="text-2xl font-black">{title}</h1>
        {paragraphs.map((paragraph, index) => (
          <p
            key={`${title}-${index}`}
            className={`${index === 0 ? "mt-4" : "mt-3"} text-sm leading-7 text-muted-foreground`}
          >
            {paragraph}
          </p>
        ))}
        <Link href="/" className="mt-6 inline-block text-sm text-sky-700 hover:underline">
          返回首页
        </Link>
      </article>
    </main>
  );
}
