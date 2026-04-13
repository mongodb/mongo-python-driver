Follow these rules in this repo (summary). Full details: see [AGENTS.md](../AGENTS.md) in the repository root.

- Security: never introduce hardcoded secrets; avoid injection/dynamic execution hazards.
- Performance: watch for inefficient loops and resource leaks; clean up resources.
- Docs/API: public classes/modules/methods must have Sphinx docs.
- PyMongo synchro rule: do not review changes in `pymongo/synchronous` or in `test/` files that have same-named files in `test/asynchronous` unless the change includes `_IS_SYNC`; those files are generated via `tools/synchro.py`.
- Async: async functions must not call blocking I/O.
- Review style: be specific/actionable; explain “why”; ask clarifying questions when unclear.
