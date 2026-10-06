#!/usr/bin/env node
"use strict";

const { spawn } = require("node:child_process");

function packageName(platform = process.platform, arch = process.arch) {
  if (!["linux", "darwin", "win32"].includes(platform) || !["x64", "arm64"].includes(arch)) {
    throw new Error(`micro-agent does not support ${platform}/${arch}.`);
  }
  return `@brokkai/micro-agent-${platform}-${arch}`;
}

function binaryName(platform = process.platform) {
  return platform === "win32" ? "micro-agent.exe" : "micro-agent";
}

function main() {
  const name = packageName();
  let binary;
  try {
    binary = require.resolve(`${name}/bin/${binaryName()}`);
  } catch {
    throw new Error(`Missing ${name}. Reinstall @brokkai/micro-agent with optional dependencies enabled.`);
  }
  const child = spawn(binary, process.argv.slice(2), { stdio: "inherit" });
  const handlers = new Map();
  for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
    const handler = () => child.kill(signal);
    handlers.set(signal, handler);
    process.on(signal, handler);
  }
  const cleanup = () => {
    for (const [signal, handler] of handlers) process.off(signal, handler);
  };
  child.on("error", (error) => {
    cleanup();
    console.error(`micro-agent: ${error.message}`);
    process.exitCode = 1;
  });
  child.on("exit", (code, signal) => {
    cleanup();
    if (signal) process.kill(process.pid, signal);
    else process.exitCode = code ?? 1;
  });
}

module.exports = { packageName, binaryName };
if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(`micro-agent: ${error.message}`);
    process.exitCode = 1;
  }
}
