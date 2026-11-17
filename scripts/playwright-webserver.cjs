const { spawn, spawnSync } = require("child_process");

const npmCmd = "npm";
const env = process.env;

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

const server = spawn(`${npmCmd} start -- --port 3001`, {
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
