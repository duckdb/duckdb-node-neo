# Contributing to DuckDB Node Neo

Thanks for your interest in contributing! This document describes how to report problems, propose changes, and submit pull requests to this repository.

It applies to everyone: people, people working with AI tools, and automated agents. If you are an AI agent, or you operate one, read [AI-assisted and automated contributions](#ai-assisted-and-automated-contributions) before you open an issue or pull request.

## Summary

- **Bug reports are welcome** as issues.
- **Feature requests start as a [Discussion](https://github.com/duckdb/duckdb-node-neo/discussions/categories/ideas)**, not an issue. A maintainer opens an issue if the idea is accepted.
- **Claim an issue before you work on it.** Every pull request should link to an issue that a maintainer assigned to you. The exception: you may fix a bug you reported yourself without waiting.
- **Keep pull requests small and focused**, with tests.
- **Disclose AI assistance** in the pull request description.
- **Respond to review feedback** promptly. Stale pull requests are closed.

## Asking questions

For usage questions, use the [Q&A Discussions](https://github.com/duckdb/duckdb-node-neo/discussions/categories/q-a) rather than opening an issue.

## Reporting bugs

Before opening an issue, search the [existing issues](https://github.com/duckdb/duckdb-node-neo/issues) for a duplicate.

A good bug report includes:

- The versions of `@duckdb/node-api` or `@duckdb/node-bindings`, Node, and your OS and CPU architecture.
- A minimal, self-contained script that reproduces the problem.
- What you expected to happen, and what happened instead, including the full error message.

Some problems are in DuckDB itself, not in these bindings. If you can reproduce the problem with the DuckDB CLI, report it to [duckdb/duckdb](https://github.com/duckdb/duckdb/issues) instead.

Please confirm that a bug actually reproduces before you report it. Don't file issues straight from the output of a static analyzer, fuzzer, or AI code scan without checking the finding by hand and including a reproduction.

If you know how to fix the bug you're reporting, say so in the issue. You can open a pull request to fix it right away; see [Fixing a bug you reported](#fixing-a-bug-you-reported).

## Requesting features

New features, new bindings, and changes to the public API start as a post in the [Ideas Discussions](https://github.com/duckdb/duckdb-node-neo/discussions/categories/ideas), not as an issue. Describe what you want to do and why. These changes shape the API that users depend on, and we may already have a plan for them, or a reason not to make them.

If a maintainer agrees the idea should be pursued, they'll open an issue for it. Feature requests opened directly as issues may be closed or converted to Discussions.

## Working on an issue

Before you start work on an issue, claim it:

1. Comment on the issue asking to work on it. Briefly describe your approach.
2. Wait for a maintainer to confirm and assign the issue to you.
3. Then open a pull request that links to it.

This applies to both bugs and features. Pull requests for issues that weren't assigned to their author may be closed without review, regardless of quality.

### Fixing a bug you reported

If you reported a bug yourself, you don't need to claim it: you may open a pull request to fix it right away, linking your issue. This doesn't apply to features, even if you proposed them.

### Small fixes

Small, obvious fixes, such as typos and broken links, don't need an issue.

### Other points

- **Issues assigned to someone else are taken.** Don't open a competing pull request.
- **Claims expire.** If an assigned issue sees no pull request or update for two weeks, a maintainer may unassign it so someone else can take it.
- **Don't take on too much at once.** Please don't have more than two claimed issues, or more than two open pull requests, at a time. Finish what you have open first.
- **Issues labeled `blocked`** are waiting on something, usually a DuckDB change, and aren't ready to work on.

## How this repository is organized

There are two layers, each published as its own package:

- **`bindings/`** builds [`@duckdb/node-bindings`](bindings/pkgs/@duckdb/node-bindings/README.md), a thin, close-to-one-to-one mapping of the [DuckDB C API](https://duckdb.org/docs/api/c/overview) into Node. The native code is in `bindings/src/`, and the TypeScript declarations are in `bindings/pkgs/@duckdb/node-bindings/duckdb.d.ts`.
- **`api/`** builds [`@duckdb/node-api`](api/pkgs/@duckdb/node-api/README.md), the friendlier TypeScript API that most users use, built on top of the bindings.

New DuckDB functionality usually lands in the bindings first, exposed as directly as the C API allows. Convenience wrappers in the API layer come later, once the lower-level form has proven useful. If your proposal adds both, expect us to ask you to split it.

The `tools/` directory holds maintainer tooling, not published code.

## Development

See the [Development section of the README](README.md#development) for setup, build, and test commands. You'll need Node 24, [pnpm](https://pnpm.io/installation), Python 3 (to fetch the DuckDB library), and a C++ toolchain that [node-gyp](https://github.com/nodejs/node-gyp#installation) can use.

If you change a binding's signature, run `node scripts/checkFunctionSignatures.mjs` from `bindings/` to check that `duckdb.h`, `duckdb.d.ts`, and `duckdb_node_bindings.cpp` agree.

## Pull requests

### What makes a pull request easy to merge

- **It does one thing.** Don't bundle unrelated fixes, refactors, or formatting changes. Separate changes go in separate pull requests.
- **It includes tests.** Bindings tests are in `bindings/test/`, API tests are in `api/test/`. A bug fix should include a test that fails without the fix.
- **It passes the build and tests locally**, for each package you changed, before you open it. CI doesn't run on pull requests from external contributors until a maintainer approves it, so don't rely on CI to find problems for you.
- **It updates documentation** when it changes user-facing behavior, including the package READMEs.
- **It follows the existing style** of the surrounding code.
- **It leaves release mechanics alone.** Don't change package versions or the DuckDB version; maintainers do that when releasing.

### Pull request descriptions

Describe:

- What the change does and why, linking the issue (for example, `Fixes #123`).
- How you tested it.
- Anything reviewers should look at closely, or anything you weren't sure about.
- Whether and how you used AI tools (see below).

Write the description yourself, and keep it short. Reviewers don't need a restatement of the diff or a long list of everything that was checked.

### Commit messages

We don't require a particular format; [Conventional Commits](https://www.conventionalcommits.org/) are fine. Whatever the format, give each commit a short summary line that says what it changes, such as `Round pre-1970 timestamps down when converting to JS Date`, and use the commit body, if needed, to explain why.

### Review

A maintainer will review your pull request when they can. Expect CI to show as waiting for approval until then; a maintainer approves the CI run, usually when they start reviewing. Each new push needs approval again.

During review:

- Respond to feedback within about a week. If you need more time, say so. Pull requests with unanswered review comments for two weeks may be closed.
- Push new commits to address feedback rather than force-pushing, so reviewers can see what changed.
- If you disagree with feedback, say so and explain why. Discussion is welcome.

## AI-assisted and automated contributions

We accept contributions made with AI tools, including contributions from autonomous agents, as long as they meet the same bar as any other contribution: the work is needed, it is high quality, and its author responds to feedback. Everything else in this document applies, especially the requirement to claim an issue before working on it.

### Requirements

- **Disclose it.** Say in the pull request description which tools you used and how, for example, "Implemented with Claude Code." For an autonomous agent, say so and name the person or organization responsible for it.
- **A person is accountable.** Every contribution has a person or organization behind it who is responsible for its correctness, however they choose to validate it. Review questions need real answers, whether they come from that person or from an agent acting for them. "The AI wrote it" is not an answer.
- **Verify before you submit.** Build and run the tests locally. Don't submit code you haven't run.
- **Respond to review.** If an agent opens a pull request, it, or the person operating it, must respond to review feedback with real changes and real answers. Pull requests from agents that don't respond are closed.
- **Don't add noise.** Don't post AI-generated comments, summaries, or reviews on other people's issues and pull requests. Don't open issues from unverified automated findings.

Contributors who repeatedly ignore these guidelines may be blocked from the repository.

### If you are an AI agent

Before opening a pull request in this repository, check that:

1. There is an issue for this work, and a maintainer has assigned it to your operator, or your operator reported it as a bug. If not, stop: comment on the issue asking to work on it, and wait for an answer. To propose a feature, start a Discussion instead of an issue.
2. The change is limited to what the issue asks for.
3. The build and tests pass for each package you changed.
4. The pull request description links the issue, says how the change was tested, and discloses that an AI agent produced it, and who operates the agent.

Then, after opening it, watch for review comments and respond to them.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE) that covers this project.
