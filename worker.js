import { runDailyShareMaintenance } from "./lib/share/daily-maintenance";
import { trackShareViewRequest } from "./lib/share/view-stats";
import openNextWorker from "./.open-next/worker.js";

const worker = {
  fetch(request, env, ctx) {
    trackShareViewRequest(request, env.MY9_SHARE_VIEW_ANALYTICS ?? null);
    return openNextWorker.fetch(request, env, ctx);
  },
  scheduled(controller, env, ctx) {
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
