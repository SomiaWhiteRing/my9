import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { createConnection, createServer } from "node:net";
import { parseArgs } from "node:util";

const require = createRequire(import.meta.url);
const args = process.argv.slice(2);

function startNext(nextArgs, env = process.env) {
  const child = spawn(process.execPath, [require.resolve("next/dist/bin/next"), "dev", ...nextArgs], {
    stdio: "inherit",
    env,
  });

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => child.kill(signal));
  }
  child.on("error", (error) => {
    console.error(`[dev] ${error.message}`);
    process.exitCode = 1;
  });
  child.on("exit", (code, signal) => {
    process.exitCode = code ?? (signal === "SIGINT" ? 130 : signal === "SIGTERM" ? 143 : 1);
  });
}

function hasListener(port, host) {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ port, host });
    const finish = (occupied) => {
      socket.destroy();
      resolve(occupied);
    };
    socket.once("connect", () => finish(true));
    // A timeout is inconclusive; conservatively skip this port.
    socket.setTimeout(500, () => finish(true));
    socket.once("error", (error) => {
      socket.destroy();
      if (["ECONNREFUSED", "EADDRNOTAVAIL", "EAFNOSUPPORT", "ENETUNREACH", "EHOSTUNREACH"].includes(error.code)) {
        resolve(false);
      } else {
        reject(error);
      }
    });
  });
}

function canBind(port, host) {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once("error", (error) => {
      if (error.code === "EADDRINUSE" || error.code === "EACCES") {
        resolve(false);
      } else {
        reject(error);
      }
    });
    server.listen({ port, host, exclusive: true }, () => {
      server.close((error) => error ? reject(error) : resolve(true));
    });
  });
}

async function main() {
  if (args.includes("--help") || args.includes("-h")) {
    startNext(args);
    return;
  }

  const { values, tokens } = parseArgs({
    args,
    options: {
      port: { type: "string", short: "p" },
      hostname: { type: "string", short: "H" },
    },
    strict: false,
    allowPositionals: true,
    tokens: true,
  });
  const portInput = values.port ?? process.env.PORT ?? "3000";
  const startingPort = Number(portInput);
  if (typeof portInput !== "string" || !/^\d+$/.test(portInput) || startingPort < 1 || startingPort > 65535) {
    throw new Error("端口必须是 1–65535 之间的整数。");
  }
  const host = values.hostname ?? "0.0.0.0";
  if (typeof host !== "string" || !host) {
    throw new Error("--hostname 必须指定有效的主机名。");
  }

  // Remove port/hostname options while preserving all other Next CLI arguments.
  const consumed = new Set();
  for (const token of tokens) {
    if (token.kind === "option" && ["port", "hostname"].includes(token.name)) {
      consumed.add(token.index);
      if (!token.inlineValue && token.value !== undefined) consumed.add(token.index + 1);
    }
  }
  const nextArgs = args.filter((_, index) => !consumed.has(index));
  // On Windows, a wildcard bind can succeed alongside an existing loopback listener.
  const probeHosts = host === "0.0.0.0" || host === "::" ? ["127.0.0.1", "::1"] : [host];

  for (let port = startingPort; port <= 65535; port += 1) {
    if (port === 3001) {
      console.log("[dev] 跳过测试专用端口 3001。");
      continue;
    }
    const listeners = await Promise.all(probeHosts.map((probeHost) => hasListener(port, probeHost)));
    if (listeners.some(Boolean) || !(await canBind(port, host))) {
      console.log(`[dev] 端口 ${port} 已占用或不可用，尝试下一个端口。`);
      continue;
    }

    const distDir = process.env.NEXT_DIST_DIR || (port === 3000 ? ".next" : `.next-dev/${port}`);
    console.log(`[dev] 使用端口 ${port}，开发缓存目录：${distDir}`);
    startNext(["--hostname", host, "--port", String(port), ...nextArgs], {
      ...process.env,
      PORT: String(port),
      NEXT_DIST_DIR: distDir,
    });
    return;
  }
  throw new Error(`从 ${startingPort} 到 65535 没有可用端口。`);
}

main().catch((error) => {
  console.error(`[dev] ${error.message}`);
  process.exitCode = 1;
});
