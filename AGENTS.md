# Agent Instructions

This file provides guidance for AI coding agents (Claude Code, GitHub Copilot, Cursor, Gemini CLI) working in this repository.

## Development Guidelines
- Read the README and CONTRIBUTING docs before making changes
- Follow the coding style and test conventions already in use
- Check open issues and PRs to avoid duplicate work
- Run existing tests before submitting

## AI Agent Usage (MAI-1)

Machine-readable contract for this library. AI agents can use this block to discover how to install and invoke the library without parsing the full README. This block is self-contained — no external services are queried.

```json
{
  "aid": "mongo-python-driver-v1",
  "logic": {
    "input_schema": {
      "type": "json"
    },
    "output_schema": {
      "type": "json"
    }
  },
  "trust": {
    "reliability_score": 0.85,
    "latency_ms": 500
  },
  "action": {
    "install_cmd": "pip install pymongo",
    "execute_cmd": "python -c \"import pymongo; client = pymongo.MongoClient('{host}', {port}); db = client.{database}\""
  }
}
```
