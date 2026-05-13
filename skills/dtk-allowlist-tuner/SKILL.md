---
name: dtk-allowlist-tuner
description: Refine an existing DTK config after creation by expanding or shrinking the allowlist, validating the filtered output, and updating the matching hook rule workflow when needed.
---

# DTK Allowlist Tuner

Use this skill when a DTK config already exists and the user wants to adjust what stays visible.
This is the follow-up agent for post-creation tuning, not the first-pass config discovery flow.

Treat the config file path, installed config filename, or hook rule name as the current config identifier.
If the user says "configuration id", resolve that to one of those existing identifiers before editing.

## When To Use

- User wants to increase the allowlist and expose more fields.
- User wants to decrease the allowlist and hide fields that are no longer needed.
- User has already created a DTK config and wants a safer or smaller model-facing surface.
- User wants to tune a config from a live command without recreating it from scratch.

## Workflow

1. Locate the existing config from the provided identifier:
   - explicit config path
   - config filename under `~/.config/dtk/configs/`
   - hook rule name in `~/.config/dtk/hooks.json`
2. Read the config and identify:
   - `content_path`
   - current `allow`
   - the source command or request
3. Run the command through `dtk exec --config ... -- <command>` when the live command is available.
4. Inspect the filtered payload and `_dtk.available_fields`.
5. Ask whether the user wants to:
   - expand visibility with new fields
   - reduce visibility by removing fields
   - keep the current shape but rename or document the config
6. Use DTK-native config commands to make the change:
   - `dtk config allow add <config> <field>`
   - `dtk config allow remove <config> <field>`
   - `dtk config delete <config>` when the user wants to wipe the config out entirely
7. Edit only the minimal set of allowlist entries needed to achieve the requested shape.
8. Re-run `dtk exec --config ... -- <command>` and verify:
   - required fields are present
   - removed fields are absent
   - `_dtk.ref_id` is still available for recovery
9. If command matching changed, update the hook rule with `dtk hook add`.

## Tuning Rules

- Prefer explicit field paths over broad object-level exposure.
- Expand gradually. Add only the fields needed for the next agent decision.
- Shrink aggressively when fields are not needed for reasoning.
- Keep `content_path` aligned with the real content subtree.
- If the root is an array, prefer stable `[]` paths over numeric indexes in `allow`.
- Use `dtk retrieve` from `_dtk.ref_id` instead of widening the allowlist when only occasional recovery is needed.
- Prefer DTK config commands over ad-hoc shell or JSON editing when mutating installed configs.

## Validation Checklist

- The config still parses as valid JSON.
- The filtered output still parses as valid JSON.
- Added fields appear in the filtered output.
- Removed fields no longer appear in the filtered output.
- `_dtk.available_fields` reflects the new visible surface.
- `_dtk.ref_id` is still present so the original payload can be recovered later.
