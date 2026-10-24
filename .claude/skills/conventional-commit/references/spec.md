# AI-Readable Commit Message Specification

This document defines a normative, English-language commit message convention for open-source projects. It is structured for both humans and AI systems to parse, validate, and generate compliant commit messages.

## 1. Normative Language

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are to be interpreted as normative requirements.

## 2. Commit Message Structure

A commit message **MUST** follow this structure:

```text
<type>(<scope>): <subject>

<body>

<footer>
```

- `<type>` and `<subject>` are **REQUIRED**.
- `<scope>`, `<body>`, and `<footer>` are **OPTIONAL**.
- Header, body, and footer **MUST** be separated by exactly one blank line when present.

## 3. Header Grammar

```ebnf
header        = type [ "(" scope ")" ] [ "!" ] ": " subject ;
type          = "feat" | "fix" | "docs" | "style" | "refactor"
              | "perf" | "test" | "build" | "ci" | "chore" | "revert" ;
scope         = lower-case-token ;
subject       = non-empty-text ;
```

Equivalent regular expression:

```regex
^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([a-z0-9][a-z0-9._-]*\))?!?: .+$
```

### 3.1 Type Rules

`<type>` **MUST** be one of the following lowercase values:

| Type | Meaning |
|---|---|
| `feat` | A new feature |
| `fix` | A bug fix |
| `docs` | Documentation-only change |
| `style` | Formatting or style change with no logic change |
| `refactor` | Code change that neither adds a feature nor fixes a bug |
| `perf` | Performance improvement |
| `test` | Adding or modifying tests |
| `build` | Build system or external dependency change |
| `ci` | CI configuration or script change |
| `chore` | Maintenance task such as dependency updates or tooling |
| `revert` | Revert a previous commit |

### 3.2 Scope Rules

- `<scope>` **SHOULD** identify the affected module, package, component, or area.
- `<scope>` **MUST** be lowercase if present.
- `<scope>` **MUST** match `[a-z0-9][a-z0-9._-]*`.
- `<scope>` **MUST** be enclosed in parentheses, e.g. `fix(parser): ...`.

### 3.3 Subject Rules

- `<subject>` **MUST** be non-empty.
- `<subject>` **MUST** use imperative mood, e.g. `add`, `fix`, `remove`, not `added`, `fixes`, `removed`.
- `<subject>` **MUST NOT** start with an uppercase letter unless it is a proper noun or code identifier.
- `<subject>` **MUST NOT** end with a period.
- `<subject>` **SHOULD** be no more than 50 characters.
- `<subject>` **MUST** describe the change concisely.

## 4. Breaking Change Rules

A commit introduces a breaking change if it is marked in either of these ways:

1. Add `!` immediately after `<type>` or after `(<scope>)`, before the colon.
2. Include a footer line beginning with `BREAKING CHANGE:` or `BREAKING-CHANGE:`.

Examples:

```text
feat!: remove legacy authentication endpoint
```

```text
feat(api)!: change response schema for user endpoint

BREAKING CHANGE: The `name` field is replaced by `full_name`.
```

If `!` is present, the commit **MUST** also explain the breaking change in the body or footer.

## 5. Body Rules

- `<body>` **SHOULD** explain **why** the change was made and any important implementation context.
- `<body>` **MUST NOT** merely repeat the subject.
- `<body>` lines **SHOULD** be wrapped at 72 characters or fewer.
- `<body>` **MAY** contain multiple paragraphs separated by blank lines.

## 6. Footer Rules

- `<footer>` **MAY** reference issues, pull requests, or co-authors.
- Issue references **SHOULD** use `Fixes #<number>`, `Closes #<number>`, or `Refs #<number>`.
- Breaking changes **MUST** use `BREAKING CHANGE:` or `BREAKING-CHANGE:` when not marked with `!`.
- Footer tokens **SHOULD** use uppercase words followed by `:` or `#`.

Example:

```text
fix(auth): prevent token refresh loop

The refresh handler retried indefinitely when the token endpoint returned 401.
Stop retrying after the first failed refresh and surface the error to the caller.

Fixes #482
```

## 7. Atomicity Rule

Each commit **SHOULD** represent exactly one logical change.

A commit **MUST NOT** mix unrelated changes that cannot be reviewed or reverted independently.

## 8. Machine-Readable Summary

```yaml
commit_message:
  required:
    header: true
  header:
    format: "<type>(<scope>): <subject>"
    type:
      required: true
      enum: [feat, fix, docs, style, refactor, perf, test, build, ci, chore, revert]
      case: lower
    scope:
      required: false
      pattern: "^[a-z0-9][a-z0-9._-]*$"
      wrapper: "()"
    breaking_marker:
      required: false
      allowed: ["!", "BREAKING CHANGE:", "BREAKING-CHANGE:"]
    subject:
      required: true
      mood: imperative
      starts_with_uppercase: false
      ends_with_period: false
      recommended_max_length: 50
  body:
    required: false
    recommended_max_line_length: 72
    purpose: "explain why and context"
  footer:
    required: false
    issue_refs:
      allowed_prefixes: ["Fixes", "Closes", "Refs"]
  rules:
    - "One logical change per commit."
    - "Use exactly one blank line between header, body, and footer."
    - "Breaking changes must be explicit."
```

## 9. Validation Checklist for AI Systems

An AI system validating a commit message **SHOULD** check:

1. The first line matches the header regex.
2. `<type>` is in the allowed enum.
3. `<scope>`, if present, is lowercase and matches the scope pattern.
4. `<subject>` is non-empty, imperative, does not end with a period, and is preferably ≤50 characters.
5. Header, body, and footer are separated by blank lines.
6. Breaking changes are marked with `!` or a `BREAKING CHANGE:` / `BREAKING-CHANGE:` footer.
7. Body lines are preferably ≤72 characters.
8. The commit represents one logical change.

## 10. Canonical Examples

```text
feat(parser): add support for nested expressions
```

```text
fix(auth): prevent token refresh loop

The refresh handler retried indefinitely when the token endpoint returned 401.
Stop retrying after the first failed refresh and surface the error to the caller.

Fixes #482
```

```text
docs(readme): update installation instructions
```

```text
refactor(core)!: replace legacy config loader

BREAKING CHANGE: `loadConfig()` now returns a Promise.
```

```text
chore(deps): bump eslint from 8.57.0 to 9.0.0
```

```text
revert: feat(parser): add experimental cache

This reverts commit 1a2b3c4d.
```

## 11. AI Generation Instruction

When generating a commit message under this specification, an AI system **MUST**:

1. Select exactly one valid `<type>`.
2. Use a lowercase `<scope>` only if it adds clarity.
3. Write an imperative, concise `<subject>` without a trailing period.
4. Mark breaking changes explicitly.
5. Add a body only when the reason or context is not obvious from the subject.
6. Add footers for issue references or breaking-change details when applicable.
7. Output only the commit message unless additional explanation is explicitly requested.