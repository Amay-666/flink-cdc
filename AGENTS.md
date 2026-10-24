## Commit Messages

- Use Conventional Commits format: `<type>(<scope>): <subject>`.
- Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`.
- Use lowercase `type` and `scope`. `scope` is optional and must be wrapped in parentheses.
- `subject` must use imperative mood, must not end with a period, and should be <= 50 characters.
- Separate header, body, and footer with exactly one blank line.
- Body should explain why the change was made, not just what changed. Wrap body lines at 72 characters.
- Mark breaking changes with `!` after the type/scope, or with a `BREAKING CHANGE:` footer. Explain the breaking change.
- Reference issues in the footer using `Fixes #123`, `Closes #123`, or `Refs #123`.
- One commit should contain one logical change.
- Full specification, grammar, examples, and validation checklist:
  `.claude/skills/conventional-commit/references/spec.md`
- If an AI generates a commit message, validate it against the full specification before committing.
- 