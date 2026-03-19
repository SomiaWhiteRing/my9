import { runDailyShareMaintenance } from "./lib/share/daily-maintenance";
import { runHourlyTrendMaintenance } from "./lib/share/hourly-trend-maintenance";
import { trackShareViewRequest } from "./lib/share/view-stats";
import openNextWorker from "./.cf-build/.open-next/worker.js";

const TREND_ROLLUP_CRON = "30 * * * *";
const DAILY_MAINTENANCE_CRON = "5 16 * * *";

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

    if (controller.cron === TREND_ROLLUP_CRON) {
      ctx.waitUntil(
        runHourlyTrendMaintenance({
          logLabel: `[trend-cron:${controller.cron}]`,
        }).catch((error) => {
          console.error("[trend-cron] failed", error);
          throw error;
        })
      );
      return;
    }

    if (controller.cron !== DAILY_MAINTENANCE_CRON) {
      console.warn(`[scheduled] unsupported cron ${controller.cron}`);
      return;
    }

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
