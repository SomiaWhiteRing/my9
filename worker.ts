import { runShareArchive } from "./lib/share/archive";

// @ts-ignore The OpenNext build generates this worker entry before Wrangler runs.
import openNextWorker from "./.open-next/worker.js";

type WorkerExecutionContext = {
  waitUntil(promise: Promise<unknown>): void;
};

type WorkerScheduledController = {
  cron: string;
  scheduledTime: number;
};

type WorkerHandler = {
  fetch(request: Request, env: CloudflareEnv, ctx: WorkerExecutionContext): Promise<Response> | Response;
  scheduled(
    controller: WorkerScheduledController,
    env: CloudflareEnv,
    ctx: WorkerExecutionContext
  ): Promise<void> | void;
};

const worker: WorkerHandler = {
  fetch(request, env, ctx) {
    return openNextWorker.fetch(request, env, ctx);
  },
  scheduled(controller, env, ctx) {
    ctx.waitUntil(
      runShareArchive({
        coldStorageBucket: env.MY9_COLD_STORAGE ?? null,
        logLabel: `[archive-cron:${controller.cron}]`,
      }).catch((error) => {
        console.error("[archive-cron] failed", error);
        throw error;
      })
    );
  },
};

export default worker;
