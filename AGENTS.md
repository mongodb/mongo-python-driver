# PyMongo Development Guide for AI Agents

This file provides instructions for AI coding assistants (Claude Code, GitHub Copilot, Codex, etc.) working in this repository.

## PyMongo-specific Rules

- **Do not edit files in `pymongo/synchronous/` or `test/` files that have a counterpart in `test/asynchronous/`** unless the change includes a `_IS_SYNC` statement. These files are auto-generated from `pymongo/asynchronous/` and `test/asynchronous/` by `tools/synchro.py`. Edit the async source and run `python tools/synchro.py` to regenerate.
- All asynchronous functions must not call any blocking I/O.
- All public classes, modules, and methods must have documentation in Sphinx format.

## Security

- Never introduce hardcoded secrets, API keys, or credentials.
- Avoid method call injection, dynamic code execution, symbol injection, or other code injection vulnerabilities.

## Performance

- Avoid inefficient loops and algorithmic issues.
- Ensure resources are properly cleaned up (connections, cursors, sessions).

## Code Quality

- Methods should be focused and appropriately sized; split methods that do too much.
- Use clear, descriptive naming conventions.
- Avoid encapsulation violations and ensure proper separation of concerns.

## Testing

- Unit tests live in `test/`; async equivalents live in `test/asynchronous/`.
- Run `python tools/synchro.py` after editing async test files to keep sync counterparts up to date.
- Do not add tests to `test/synchronous/` directly.
