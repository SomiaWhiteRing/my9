const { rmSync } = require("fs");
const { spawn, spawnSync } = require("child_process");

const npmCmd = "npm";
const env = {
  ...process.env,
  NEXT_DIST_DIR: ".next-e2e",
};

rmSync(".next-e2e", { recursive: true, force: true });

const buildResult = spawnSync(`${npmCmd} run build`, {
  stdio: "inherit",
  env,
  shell: true,
});

if (buildResult.error) {
  console.error(buildResult.error);
  process.exit(1);
}

if ((buildResult.status ?? 1) !== 0) {
  process.exit(buildResult.status ?? 1);
}

const server = spawn(`${npmCmd} start -- -p 3001`, {
  stdio: "inherit",
  env,
  shell: true,
});

function forward(signal) {
  if (server.killed) return;
  server.kill(signal);
}

process.on("SIGINT", () => forward("SIGINT"));
process.on("SIGTERM", () => forward("SIGTERM"));
server.on("exit", (code) => process.exit(code ?? 0));
