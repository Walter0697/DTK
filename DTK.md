# DTK

DTK is a structured JSON preprocessing layer.

It reduces model-facing payloads while preserving recoverability of the original data.

## What DTK Does

- accepts JSON objects and arrays
- filters fields with allowlist rules
- stores the original payload locally for recovery
- adds `_dtk` metadata with a `ref_id`, field inventory, and content path hints
- supports cleanup of expired raw payloads

## Current Commands

- `cargo run --quiet --bin dtk_detect_json`
- `cargo run --quiet --bin dtk_inspect_json`
- `cargo run --quiet --bin dtk_filter_json -- <config.json>`
- `cargo run --quiet --bin dtk_recover_json -- <ref_id>`
- `cargo run --quiet --bin dtk_retrieve_json -- <ref_id> [fields]`
- `cargo run --quiet --bin dtk_cleanup_store`
- `cargo run --quiet --bin dtk_cleanup_store -- --dry-run`
- `dtk exec --config <config.json> -- <command> [args...]`
- `dtk retrieve <ref_id> [fields] [--index N | --all]`
- `dtk version`
- `./install.sh`
- `dtk install`
- `dtk uninstall`

## Config Files

DTK configs are JSON files that describe the source and request shape for a payload.
By default, user configs should live under the global DTK config directory, not in a repo.
On Unix-like systems that is `~/.config/dtk/`; use `DTK_CONFIG_DIR` to override it.
Place source configs under `~/.config/dtk/configs/`.
`dtk install` seeds a default config at `~/.config/dtk/configs/dummyjson_users.json` so the agent can reuse it later.

Recommended fields:

- `name`
- `source`
- `request`
- `notes`
- `content_path`
- `allow`

Example:

```json
{
  "name": "n8n_workflows_list",
  "source": "n8n",
  "request": "curl -sS -H \"X-N8N-API-KEY: $N8N_API_TOKEN\" \"$N8N_BASE_URL/api/v1/workflows?limit=3\"",
  "notes": "Keep the workflow list surface fields and drop the full workflow graph.",
  "content_path": "data",
  "allow": [
    "[].id",
    "[].name",
    "[].active",
    "[].isArchived",
    "[].createdAt",
    "[].updatedAt"
  ]
}
```

## Agent Guidance

- treat `_dtk` as metadata, not user data
- use `ref_id` to recover the original payload when needed
- read `root_kind` and `item_kind` before assuming array access
- use `available_fields` to see the full flattened field inventory
- use `content_path` to identify the main content subtree
- use `dtk retrieve` to project selected fields from the stored original payload
- use `_dtk.ref_id` from filtered output as the lookup key for `dtk retrieve`

## Storage

- raw payloads are stored in the user data directory by default
- Unix-like systems use XDG data locations
- Windows uses `%LOCALAPPDATA%` or `%APPDATA%`
- `DTK_STORE_DIR` overrides the default

## Prompt Flow

DTK is designed to work like an agent-facing prompt workflow, not as an install-time hook.
RTK and DTK are complementary:

- Always use `dtk` directly for DTK-native commands such as `dtk exec`, `dtk retrieve`, `dtk doctor`, `dtk install`, and `dtk uninstall`.
- Use `rtk` only for ordinary non-DTK shell commands when token savings matters.
- `dtk` should inspect the command and look up a matching config or hook rule before choosing a raw curl command.
- Prefer an installed config from `~/.config/dtk/configs/` when one already matches the request.
- If DTK has no matching rule or schema, it should return the original command or payload unchanged so RTK can still apply its normal filtering behavior.
- RTK should still be used whenever it would normally be used for token savings.
- Use DTK whenever the command can be filtered, wrapped, or handled by a DTK config or hook rule.
- Do not default to `rtk proxy` for curl/API/JSON flows when `dtk` can handle them.

Examples:

```bash
dtk exec --config n8n_workflows_list.json -- \
  curl -sS -H "X-N8N-API-KEY: $N8N_API_TOKEN" "$N8N_BASE_URL/api/v1/workflows?limit=3"

dtk exec -- \
  curl -sS https://dummyjson.com/users

dtk retrieve dtk_1234567890abcdef users[].address,users[].age

dtk retrieve dtk_1234567890abcdef name --index 0

dtk retrieve dtk_1234567890abcdef name --all

dtk retrieve dtk_1234567890abcdef users[0].firstName,users[0].lastName
```

Use `dtk doctor` on its own when you want to inspect the local DTK setup.

Recommended flow:

1. The user provides a command, curl URL, or API response.
2. The DTK Config Assistant inspects the payload and asks what matters, using `_dtk.available_fields` and `_dtk.content_path` when available.
3. DTK writes a reusable source config under `~/.config/dtk/configs/` when needed.
4. `dtk exec` is used when you want to run the command through DTK and store the original response.
5. Use `dtk retrieve` when you need to pull a few fields back out of a stored payload.
   Example:

```bash
dtk retrieve <ref_id> 'users[0].firstName,users[0].lastName'
```
6. When a DTK rule matches or can be created, prefer DTK routing first; keep RTK in the stack whenever it would normally be used for token savings.
