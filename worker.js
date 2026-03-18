import { runDailyShareMaintenance } from "./lib/share/daily-maintenance";
import { trackShareViewRequest } from "./lib/share/view-stats";
import openNextWorker from "./.cf-build/.open-next/worker.js";

function bindRuntimeEnv(env) {
  globalThis.__MY9_CF_ENV = env;
}

const worker = {
  fetch(request, env, ctx) {
    bindRuntimeEnv(env);
    trackShareViewRequest(request, env.MY9_SHARE_VIEW_ANALYTICS ?? null);
    return openNextWorker.fetch(request, env, ctx);
  },
  scheduled(controller, env, ctx) {
    bindRuntimeEnv(env);
    ctx.waitUntil(
      runDailyShareMaintenance({
        env,
        logLabel: `[daily-cron:${controller.cron}]`,
      }).catch((error) => {
        console.error("[daily-cron] failed", error);
        throw error;
      })
    );
  },
};

export default worker;
