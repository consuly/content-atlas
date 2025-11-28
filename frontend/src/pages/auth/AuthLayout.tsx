import type { ReactNode } from "react";
import { Database, ShieldCheck, Sparkles } from "lucide-react";

type AuthLayoutProps = {
  children: ReactNode;
  formTitle: string;
  formSubtitle: string;
  footer?: ReactNode;
};

const highlightCards = [
  {
    title: "Mapping clarity",
    description: "Catch unmapped columns early with schema-aware imports.",
    icon: ShieldCheck,
  },
  {
    title: "High-fidelity data",
    description: "Postgres-first pipelines with zero silent drops.",
    icon: Database,
  },
];

export const AuthLayout = ({
  children,
  formTitle,
  formSubtitle,
  footer,
}: AuthLayoutProps) => {
  return (
    <div className="relative min-h-screen bg-brand-darker text-slate-200 overflow-hidden">
      <div className="absolute inset-0 pointer-events-none">
        <div className="absolute -left-20 -top-24 h-[520px] w-[520px] bg-brand-600/25 blur-[120px]" />
        <div className="absolute right-[-180px] top-24 h-[520px] w-[520px] bg-purple-600/20 blur-[140px]" />
        <div
          className="absolute inset-0 opacity-40"
          style={{
            backgroundImage:
              "linear-gradient(to right, rgba(148,163,184,0.08) 1px, transparent 1px), linear-gradient(to bottom, rgba(148,163,184,0.08) 1px, transparent 1px)",
            backgroundSize: "64px 64px",
          }}
        />
      </div>

      <div className="relative mx-auto max-w-6xl px-6 py-14 lg:py-20">
        <div className="mb-12 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-xl bg-gradient-to-br from-brand-500 to-purple-500 text-xl font-black text-white shadow-lg shadow-brand-500/30">
              A
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-slate-500">
                Content Atlas
              </p>
              <p className="text-sm font-semibold text-white/90">
                by Consuly.ai
              </p>
            </div>
          </div>
          <div className="hidden items-center gap-3 rounded-full border border-white/10 bg-white/5 px-4 py-2 text-xs font-medium text-slate-400 backdrop-blur md:flex">
            <ShieldCheck size={16} className="text-brand-500" />
            Security-first access
          </div>
        </div>

        <div className="grid items-center gap-10 lg:grid-cols-2">
          <div className="space-y-6">
            <div className="inline-flex items-center gap-2 rounded-full border border-brand-500/30 bg-brand-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.28em] text-brand-100">
              Early access
              <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            </div>
            <div className="space-y-3">
              <h1 className="text-3xl font-bold leading-tight text-white lg:text-4xl">
                Data ingestion without the engineering overhead
              </h1>
              <p className="text-base text-slate-400 lg:text-lg">
                Transform scattered files into reliable datasets with duplicate
                defense, schema-aware mapping, and AI guidance baked into every
                import.
              </p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              {highlightCards.map((card) => {
                const Icon = card.icon;
                return (
                  <div
                    key={card.title}
                    className="rounded-xl border border-white/10 bg-white/5 p-4 shadow-[0_10px_35px_rgba(8,19,44,0.45)] backdrop-blur"
                  >
                    <div className="mb-3 flex h-10 w-10 items-center justify-center rounded-lg bg-brand-500/15 text-brand-500">
                      <Icon size={18} />
                    </div>
                    <p className="text-sm font-semibold text-white">
                      {card.title}
                    </p>
                    <p className="text-sm text-slate-400">{card.description}</p>
                  </div>
                );
              })}
            </div>
            <div className="flex flex-wrap items-center gap-3 text-sm text-slate-400">
              <span className="flex items-center gap-2 text-brand-500">
                <Sparkles size={16} />
                AI operator ready
              </span>
              <span className="h-1 w-1 rounded-full bg-slate-700" />
              <span>Actionable errors, no silent drops</span>
              <span className="h-1 w-1 rounded-full bg-slate-700" />
              <span>Built for mapping completeness</span>
            </div>
          </div>

          <div className="relative">
            <div className="absolute inset-0 rounded-2xl bg-gradient-to-r from-brand-500/25 to-purple-500/20 blur-3xl" />
            <div className="relative rounded-2xl border border-white/10 bg-[var(--surface)] p-8 shadow-[0_25px_70px_rgba(8,19,44,0.55)] backdrop-blur-xl">
              <div className="mb-6 flex items-start justify-between gap-3">
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.28em] text-brand-500">
                    Content Atlas
                  </p>
                  <h2 className="text-2xl font-bold text-black/90">
                    {formTitle}
                  </h2>
                  <p className="text-sm text-slate-400">{formSubtitle}</p>
                </div>
                <span className="rounded-full border border-brand-500 bg-brand-500 px-3 py-1 text-xs font-semibold text-brand-100">
                  Secure
                </span>
              </div>

              {children}

              {footer && <div className="mt-6">{footer}</div>}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
