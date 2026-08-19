import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFile, readdir, stat } from 'node:fs/promises';
import { dirname, join, normalize } from 'node:path';

const root = new URL('..', import.meta.url);
const expected = [
  '.github/dependabot.yml',
  '.github/workflows/ci.yml',
  '.gitignore',
  '.node-version',
  'AGENTS.md',
  'LICENSE',
  'README.md',
  'SECURITY.md',
  'compose.yaml',
  'docs/adr/0001-postgres-fenced-window-publications.md',
  'docs/api.md',
  'docs/architecture.md',
  'docs/closed-book-contract.md',
  'docs/operations.md',
  'docs/research-log.md',
  'docs/requirements.md',
  'docs/threat-model.md',
  'docs/verification.md',
  'package-lock.json',
  'package.json',
  'scripts/infra-benchmark.mjs',
  'scripts/infra-smoke.mjs',
  'scripts/repo-check.mjs',
  'sql/schema.sql',
  'src/contracts.js',
  'src/crypto.js',
  'src/errors.js',
  'src/http.js',
  'src/index.js',
  'src/main.js',
  'src/model.js',
  'src/repository.js',
  'src/service.js',
  'test/integration/postgresql.test.js',
  'test/unit/contracts.test.js',
  'test/unit/http.test.js',
  'test/unit/model.test.js',
];

async function walk(directory, prefix = '') {
  const paths = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.name === '.git' || entry.name === 'node_modules' || entry.name === '.tmp') continue;
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) paths.push(...await walk(new URL(`${entry.name}/`, directory), relative));
    else paths.push(relative);
  }
  return paths;
}

for (const path of expected) {
  assert.equal((await stat(new URL(path, root))).isFile(), true, `missing required file: ${path}`);
}

const packageJson = JSON.parse(await readFile(new URL('package.json', root), 'utf8'));
assert.equal(packageJson.private, true);
assert.equal(packageJson.engines.node, '>=22');
assert.deepEqual(packageJson.dependencies, { pg: '8.23.0' });
assert.equal(
  packageJson.repository.url,
  'git+https://github.com/estelledc/system-design-21-ad-click-aggregation.git',
);
for (const script of ['lint', 'test', 'test:infra', 'smoke:infra', 'benchmark:infra', 'audit', 'check', 'check:ci']) {
  assert.equal(typeof packageJson.scripts[script], 'string', `missing package script: ${script}`);
}
const lock = JSON.parse(await readFile(new URL('package-lock.json', root), 'utf8'));
assert.equal(lock.lockfileVersion, 3);
assert.deepEqual(lock.packages[''].dependencies, packageJson.dependencies);

const workflow = await readFile(new URL('.github/workflows/ci.yml', root), 'utf8');
assert.match(workflow, /node: \[22, 24, 26\]/);
assert.match(workflow, /postgres:17\.11-alpine@sha256:[0-9a-f]{64}/);
assert.match(workflow, /permissions:\n  contents: read/);
const actionUses = [...workflow.matchAll(/uses: [^@\n]+@([^\s#]+)/g)].map((match) => match[1]);
assert.ok(actionUses.length >= 2);
assert.ok(actionUses.every((reference) => /^[0-9a-f]{40}$/.test(reference)), 'actions must use full commit pins');

const compose = await readFile(new URL('compose.yaml', root), 'utf8');
assert.match(compose, /postgres:17\.11-alpine@sha256:[0-9a-f]{64}/);
assert.match(compose, /pg_isready -U postgres -d ad_click_aggregation/);

const schema = await readFile(new URL('sql/schema.sql', root), 'utf8');
for (const contract of [
  'UNIQUE (source_id, source_epoch, batch_sequence)',
  'UNIQUE (source_id, batch_revision)',
  'UNIQUE (partition_id, input_offset)',
  'PRIMARY KEY (definition_id, event_id)',
  "disposition IN ('counted', 'late_unapplied')",
  'PRIMARY KEY (publication_revision, window_start, ad_id, region, device)',
  'delta integer NOT NULL CHECK (delta = 1)',
  'event_id text NOT NULL UNIQUE',
  'UNIQUE (principal, request_key)',
]) assert.ok(schema.includes(contract), `missing schema contract: ${contract}`);

const repository = await readFile(new URL('src/repository.js', root), 'utf8');
for (const contract of [
  'pg_advisory_xact_lock',
  'FOR UPDATE',
  'event ID binds changed intent',
  'classifyDisposition',
  'aca_window_partials.count + 1',
  'minimumWatermark',
  '#materializePublication',
  'correction.applied_publication_revision <= $1',
  "throw beyondDedupHorizon()",
  "throw retentionBlocked('unprocessed_batches')",
]) assert.ok(repository.includes(contract), `missing repository contract: ${contract}`);

const service = await readFile(new URL('src/service.js', root), 'utf8');
for (const contract of [
  'AD_CLICK_CRASH_AFTER_INGEST_COMMIT',
  'AD_CLICK_CRASH_AFTER_PROCESS_COMMIT',
  'AD_CLICK_CRASH_AFTER_PUBLICATION_COMMIT',
  'AD_CLICK_CRASH_AFTER_CORRECTION_COMMIT',
]) assert.ok(service.includes(contract), `missing service crash boundary: ${contract}`);

const logSources = [service, await readFile(new URL('src/http.js', root), 'utf8')];
const logBodies = logSources.flatMap((source) => (
  [...source.matchAll(/(?:this\.)?logger\(\{([\s\S]*?)\}\)/g)].map((match) => match[1])
));
assert.ok(logBodies.length >= 10, 'expected explicit low-cardinality log sites');
for (const body of logBodies) {
  for (const forbiddenField of [
    'sourceId', 'workerId', 'eventId', 'adId', 'region', 'device', 'requestKey',
    'correctionId', 'queryId', 'intentDigest', 'apiToken', 'body', 'authorization',
  ]) {
    assert.equal(new RegExp(`\\b${forbiddenField}\\s*:`).test(body), false, `ordinary log includes ${forbiddenField}`);
  }
}

const research = await readFile(new URL('docs/research-log.md', root), 'utf8');
for (const pinned of [
  '9d8388721e7231442763ad37398b8d82224aa68f',
  '5b6cde66a014d10dedcd6ec311b5de740edbf96a',
  '903b3424ae313e91d8e3df29942191fdd7026c28',
  'e668a046fccb5b93603bf312d4c9769f2e745f19a06674d9c358233daa599449',
  '06ed190cee60bd2c3f5926e48ca3eb2f4f172c27badefb54d7e45f5eee7174ef',
  '06615990acad4a80862e3b728ef98d07b7904fcfbed7e3e60d55325b887a3a6a',
  'c2bf5d0aca4ea5fcaac59ddf3389060662adbef43d911dfc48112140660637db',
]) assert.ok(research.includes(pinned), `research log is missing fixed evidence identity: ${pinned}`);

const integration = await readFile(new URL('test/integration/postgresql.test.js', root), 'utf8');
assert.match(integration, /Infrastructure tests never skip/);
assert.doesNotMatch(integration, /\.skip\s*\(/);
const modelTest = await readFile(new URL('test/unit/model.test.js', root), 'utf8');
assert.match(modelTest, /index < 2_000/);
const smoke = await readFile(new URL('scripts/infra-smoke.mjs', root), 'utf8');
assert.match(smoke, /killedProcesses/);
for (const boundary of ['INGEST', 'PROCESS', 'PUBLICATION', 'CORRECTION']) {
  assert.ok(smoke.includes(`AD_CLICK_CRASH_AFTER_${boundary}_COMMIT`));
}
assert.match(smoke, /externalAcceptanceProved: false/);
const benchmark = await readFile(new URL('scripts/infra-benchmark.mjs', root), 'utf8');
for (const fixture of ['submittedEvents = 2 * 16 * 64', 'queries: 64', 'corrections: 1']) {
  assert.ok(benchmark.includes(fixture), `benchmark fixture is not fixed: ${fixture}`);
}

const files = await walk(root);
const portable = files.filter((path) => /\.(?:md|js|mjs|json|sql|ya?ml)$/.test(path));
const forbidden = [
  /\/Users\//,
  /\/private\/tmp\//,
  /file:\/\//,
  /ghp_[A-Za-z0-9]{20,}/,
  /github_pat_[A-Za-z0-9_]{20,}/,
  /AKIA[0-9A-Z]{16}/,
  new RegExp(['Co', 'Authored-By:'].join('-')),
];
for (const path of portable) {
  const contents = await readFile(new URL(path, root), 'utf8');
  for (const pattern of forbidden) assert.equal(pattern.test(contents), false, `${path} contains forbidden portable data`);
}

for (const path of files.filter((value) => value.endsWith('.md'))) {
  const contents = await readFile(new URL(path, root), 'utf8');
  for (const match of contents.matchAll(/\[[^\]]+\]\(([^)]+)\)/g)) {
    const target = match[1].split('#', 1)[0];
    if (!target || /^(?:https?:|mailto:)/.test(target)) continue;
    const resolved = normalize(join(dirname(path), decodeURIComponent(target)));
    assert.equal((await stat(new URL(resolved, root))).isFile(), true, `${path} has broken link: ${target}`);
  }
}

for (const path of files.filter((value) => /\.(?:js|mjs)$/.test(value))) {
  execFileSync(process.execPath, ['--check', path], { cwd: root, stdio: 'inherit' });
}
execFileSync('git', ['diff', '--check'], { cwd: root, stdio: 'inherit' });
process.stdout.write(`${JSON.stringify({
  evidence: 'repository_policy_check',
  files: files.length,
  serviceLogSitesChecked: logBodies.length,
  markdownLinksChecked: true,
  syntaxChecked: true,
})}\n`);
