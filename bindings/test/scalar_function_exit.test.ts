import { execFile } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { expect, suite, test } from 'vitest';

// A registered scalar function must not keep its process alive. Its thread-safe
// functions are created referenced, and they are only released when the scalar
// function's extra info is destroyed -- which requires the database to be closed
// or garbage collected. Without unreferencing them, a program that registers a
// scalar function and keeps its database open never exits.
//
// These spawn a real process because that is the only way to observe it: within
// a single process the condition is "the event loop never drains", which has no
// in-process assertion.

const fixturePath = fileURLToPath(
  new URL('./fixtures/scalarFunctionExit.cjs', import.meta.url),
);
const bindingsRoot = fileURLToPath(new URL('..', import.meta.url));

// The fixture exits in well under a second when the bug is absent, and hangs
// forever when it is present. This only needs to outlast a slow process start.
//
// It must also stay below the per-test timeout below, so that execFile is what
// kills a hung child: if vitest times the test out first, the child is left
// running.
const exitTimeoutMs = 8000;
const testTimeoutMs = 30000;

function runFixture(
  scenario: string,
): Promise<{ stdout: string; stderr: string; killed: boolean; code: number | null }> {
  return new Promise((resolve, reject) => {
    const child = execFile(
      process.execPath,
      [fixturePath, scenario],
      { cwd: bindingsRoot, timeout: exitTimeoutMs },
      (error, stdout, stderr) => {
        if (error && !(error as { killed?: boolean }).killed) {
          reject(
            new Error(`fixture failed: ${error.message}\nstderr: ${stderr}`),
          );
          return;
        }
        resolve({
          stdout,
          stderr,
          killed: Boolean((error as { killed?: boolean } | null)?.killed),
          code: child.exitCode,
        });
      },
    );
  });
}

async function expectExits(scenario: string) {
  const result = await runFixture(scenario);
  expect(result.stderr).toBe('');
  expect(result.stdout.trim()).toBe('done');
  // killed means execFile's timeout fired, i.e. the process never exited.
  expect(
    result.killed,
    `process did not exit within ${exitTimeoutMs}ms for scenario "${scenario}"`,
  ).toBe(false);
}

suite('scalar function process exit', () => {
  test('exits after closing the database', async () => {
    // Control: this passed even before the thread-safe functions were
    // unreferenced, because closing destroys the extra info and releases them.
    await expectExits('closed');
  }, testTimeoutMs);
  test('exits with the database left open', async () => {
    await expectExits('database_left_open');
  }, testTimeoutMs);
  test('exits when the function was never called', async () => {
    // Registering alone creates the thread-safe functions, so this hangs too.
    await expectExits('registered_never_called');
  }, testTimeoutMs);
  test('exits when the function handle was never destroyed', async () => {
    await expectExits('registered_handle_kept');
  }, testTimeoutMs);
});
