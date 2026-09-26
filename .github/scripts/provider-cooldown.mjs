import { appendFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

export function needsCooldown(runs, currentId, now = Date.now()) {
  return runs.some(run => String(run.id) !== String(currentId) && run.conclusion === 'failure' &&
    Number.isFinite(Date.parse(run.updated_at)) && now - Date.parse(run.updated_at) < 600_000);
}

async function main() {
  const env = process.env;
  const response = await fetch(`https://api.github.com/repos/${env.GITHUB_REPOSITORY}/actions/workflows/production-refresh.yml/runs?per_page=30`, {
    headers: { Authorization: `Bearer ${env.GH_TOKEN}`, Accept:'application/vnd.github+json' }, signal: AbortSignal.timeout(20_000), redirect:'error',
  });
  if (!response.ok) throw new Error('cooldown_check_failed');
  const body = await response.json();
  if (!Array.isArray(body.workflow_runs)) throw new Error('invalid_runs');
  const allowed = !needsCooldown(body.workflow_runs, env.GITHUB_RUN_ID);
  await appendFile(env.GITHUB_OUTPUT, `allowed=${allowed}\n`);
  console.log(allowed ? 'Provider cooldown clear.' : 'Provider cooldown active; skipping provider calls without claiming data freshness.');
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch(() => { console.error('::error::Cannot verify provider cooldown; no provider requests allowed.'); process.exitCode = 1; });
