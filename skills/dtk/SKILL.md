---
name: dtk-config-assistant
description: Help configure DTK from a live curl URL, command, or API response by running it, inspecting the payload, asking which fields matter, and drafting source-specific filtering rules.
---

# DTK Config Assistant

Use this skill when a user wants DTK configured for a new curl URL, endpoint, or command line.
The workflow is to run the live source, inspect the output, ask what fields are actually required, and draft DTK config from that conversation.
Generated configs should be saved in the user config area, not in the repo.
Default storage target: `~/.config/dtk/configs/` on Unix-like systems, or the platform equivalent.
After generating the config, update the global hook rules file at `~/.config/dtk/hooks.json` with a matching rule that points to it.
Prefer `dtk hook add` to register the rule instead of editing the file by hand.

## Suggested Prompt

```text
Use the DTK Config Assistant for this command:

curl -sS https://dummyjson.com/users

Please inspect the response, identify the fields that are likely needed, ask me any clarifying questions, and draft the DTK configuration.
```

## When To Use

- User gives a curl URL, endpoint, or command to filter.
- User wants help discovering which fields matter before writing config.
- User wants DTK rules derived from live output, not just pasted JSON.
- User wants to reduce payload size while preserving recoverability.

## Workflow

1. Run the provided command or curl request.
2. Inspect the live output and identify the root shape.
3. Read `_dtk.available_fields` and `_dtk.content_path` first when they exist.
4. Ask the user which fields are required, optional, or sensitive.
5. Propose an allowlist with minimal safe defaults.
6. Generate or update config JSON with:
   - `name`
   - `source`
   - `request`
   - `notes`
   - `content_path`
   - `allow`
   - store it under the global DTK config directory
   - append or update a matching rule in `~/.config/dtk/hooks.json` via `dtk hook add`
7. Run `dtk exec --config ... -- <command>` and compare:
   - original payload shape
   - filtered payload shape
   - `_dtk.available_fields`
   - `_dtk.content_path`
   - `_dtk.ref_id`
8. Adjust rules based on user intent and rerun.

When updating an existing installed config after creation, prefer DTK-native config commands over manual JSON editing:

- `dtk config allow add <config> <field>`
- `dtk config allow remove <config> <field>`
- `dtk config delete <config>`

## Rule Design Notes

- Start with the smallest useful surface fields.
- Keep only fields the model needs for decisions.
- Prefer stable paths that survive array growth (`data[]` patterns).
- Use `_dtk.content_path` to identify the main payload branch before writing allow rules.
- Mirror `_dtk.content_path` into the config as `content_path` when the payload has an envelope around the real data.
- If `available_fields` shows nested content, prefer explicit nested allow paths such as `users[].hair.color`.
- Use `dtk retrieve` when you need to project specific fields back out of a stored payload.
- Use nested indexes like `users[0].firstName` when you need a single array element from a nested array.
- Use `_dtk.ref_id` from filtered output as the lookup key for `dtk retrieve`.
- If the root is an array, ask whether the user wants one item or all items.

## Validation Checklist

- Filtered output remains valid JSON.
- Required decision fields are present.
- The user confirmed which fields are actually needed.
- `_dtk.available_fields` reflects the important visible branches.
- `_dtk.content_path` points at the content subtree the agent should inspect.
- The config `content_path` matches the real payload branch when one exists.
- `dtk retrieve` can recover selected fields from stored original payloads.
- Nested array indexes are supported in retrieval paths.
- The retrieve flow starts from `_dtk.ref_id`.
- `_dtk.ref_id` exists for recovery.
- Recovery command returns original payload.
