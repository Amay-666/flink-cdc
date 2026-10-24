---
name: conventional-commit
description: >-
  Generate, review, and validate Git commit messages using the Conventional
  Commits specification. Use when creating commits, writing commit messages,
  reviewing commit history, or checking whether a commit follows project
  conventions.
---

# Conventional Commit

## Purpose

Produce Git commit messages that follow the Conventional Commits specification and the project's commit policy.

## When to Use

Use this skill when:

- Creating a new Git commit.
- Writing or rewriting a commit message.
- Reviewing commit history or a pull request's commits.
- Validating whether a commit message follows project conventions.
- Generating changelogs from commits.

## Required Format

```text
<type>(<scope>): <subject>

<body>

<footer>
```

- `<type>` and `<subject>` are required.
- `<scope>`, `<body>`, and `<footer>` are optional.
- Header, body, and footer must be separated by exactly one blank line.
- Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.
- Use lowercase `type` and `scope`.
- `subject` must use imperative mood, must not end with a period, and should be <= 50 characters.
- Body lines should be <= 72 characters.
- Breaking changes must be marked with `!` after the type/scope or with a `BREAKING CHANGE:` footer.
- Issue references should use `Fixes #123`, `Closes #123`, or `Refs #123`.
- One commit should represent one logical change.

## Workflow

1. Inspect the staged diff and identify the single logical change.
2. Choose exactly one valid `type`.
3. Add a lowercase `scope` only if it improves clarity.
4. Write an imperative, concise `subject` without a trailing period.
5. Add a `body` only when the reason, context, or trade-offs are not obvious from the subject.
6. Add footers for issue references or breaking-change details when applicable.
7. Validate the message against the checklist below and the full specification in `references/spec.md`.

## Validation Checklist

- [ ] Header matches `<type>(<scope>): <subject>` or `<type>: <subject>`.
- [ ] `type` is in the allowed enum and lowercase.
- [ ] `scope`, if present, is lowercase and wrapped in parentheses.
- [ ] `subject` is non-empty, imperative, and does not end with a period.
- [ ] Header, body, and footer are separated by blank lines.
- [ ] Breaking changes are marked with `!` or `BREAKING CHANGE:`.
- [ ] Body explains why, not just what.
- [ ] Issue references use `Fixes`, `Closes`, or `Refs`.
- [ ] The commit contains one logical change.

## Reference

Read `references/spec.md` when you need:

- The full grammar and regular expression.
- The complete type table and scope rules.
- Breaking-change rules and examples.
- The machine-readable summary.
- The full AI validation checklist.
- Canonical examples.

Do not repeat the full specification in the commit message. Use the reference only to validate or generate compliant messages.

## Output Rules

- Output only the commit message unless the user asks for explanation.
- Do not invent issue numbers.
- Do not claim a change is breaking unless the diff or user instruction indicates it.
- If the diff contains multiple unrelated changes, recommend splitting the commit.