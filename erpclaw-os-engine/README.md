# erpclaw-os-engine

ERPClaw OS Engine — the optional self-improvement layer. Module generation, deploy pipeline, DGM evolution, semantic checks. 28 actions, all `os-` prefixed.

## Install

```bash
python3 ~/.openclaw/workspace/skills/erpclaw/scripts/module_manager.py \
  --action install-module --module-name erpclaw-os-engine
```

Requires foundation skill `erpclaw >= 4.0.0`.

## Why this is a separate addon

This addon contains the dev-time module-generation tooling that triggers static-analysis flags on ClawHub (`suspicious.dynamic_code_execution` rule). The patterns are intentional — it's the module-generation engine — but isolating them here keeps the foundation skill scan-clean. End users running ERPClaw for normal accounting/inventory/reports don't need this addon.

## License

MIT — see `LICENSE.txt`.

See `SKILL.md` for full action catalog and usage examples.
