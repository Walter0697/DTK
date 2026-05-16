# DTK

DTK is a structured payload preprocessing layer.

It reduces model-facing payloads while preserving recoverability of the original data.

## What DTK Does

- accepts JSON objects and arrays
- accepts YAML mappings and sequences
- accepts TOML tables and arrays of tables
- accepts Terraform-style HCL blocks
- accepts XAML / XML documents
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
- `dtk config list`
- `dtk config allow add <config> <field>`
- `dtk config allow remove <config> <field>`
- `dtk config delete <config>`
- `dtk version`
- `./install.sh`
- `dtk install`
- `dtk install-dummy`
- `dtk uninstall`

## Config Files

DTK configs are JSON files that describe the source and request shape for a payload.
By default, user configs should live under the global DTK config directory, not in a repo.
On Unix-like systems that is `~/.config/dtk/`; use `DTK_CONFIG_DIR` to override it.
Place source configs under `~/.config/dtk/configs/`.
`dtk install` seeds a default config at `~/.config/dtk/configs/dummyjson_users.json` so the agent can reuse it later.
`dtk install-dummy` installs the full bundled sample set, which currently includes a Cargo.lock-style TOML example config at `~/.config/dtk/configs/cargo_lock_packages.toml.json`, a TOML Python manifest example config at `~/.config/dtk/configs/pyproject_manifest.toml.json`, a Terraform-style HCL variables example config at `~/.config/dtk/configs/terraform_module_variables.tf.json`, a CSV inventory export example config at `~/.config/dtk/configs/csv_inventory_export.csv.json`, an INI plugin registry example config at `~/.config/dtk/configs/ini_plugin_registry.ini.json`, an XML RSS feed example config at `~/.config/dtk/configs/xml_rss_feed.xml.json`, a XAML ResourceDictionary example config at `~/.config/dtk/configs/xaml_resource_dictionary.xaml.json`, their sample payloads at `~/.config/dtk/samples/cargo_lock_packages.toml`, `~/.config/dtk/samples/pyproject_manifest.toml`, `~/.config/dtk/samples/terraform_module_variables.tf`, `~/.config/dtk/samples/csv_inventory_export.csv`, `~/.config/dtk/samples/ini_plugin_registry.ini`, `~/.config/dtk/samples/xml_rss_feed.xml`, and `~/.config/dtk/samples/xaml_resource_dictionary.xaml`, plus the Kubernetes YAML example config at `~/.config/dtk/configs/kubernetes_deployment.yaml.json` with a sample payload at `~/.config/dtk/samples/kubernetes_deployment.yaml`.

Recommended fields:

- `name`
- `source`
- `request`
- `notes`
- `format` (optional parser override such as `json`, `yaml`, `toml`, `hcl`, `csv`, `ini`, `xml`, or `xaml`)
- `content_path`
- `allow`

Example:

```json
{
  "name": "n8n_workflows_list",
  "source": "n8n",
  "request": "curl -sS -H \"X-N8N-API-KEY: $N8N_API_TOKEN\" \"$N8N_BASE_URL/api/v1/workflows?limit=3\"",
  "notes": "Keep the workflow list surface fields and drop the full workflow graph.",
  "format": "json",
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
- if `dtk retrieve` prints a `DTK recommendation:` notice about repeated field requests, ask the user whether they want that field added to the config allowlist and mention `dtk config list` followed by `dtk config allow add <config> <field>`
- if `dtk exec` prints a `DTK recommendation:` notice about repeated fallbacks, ask the user whether to tighten the config or remove DTK for that endpoint, and mention `dtk config list` followed by `dtk config allow add/remove <config> <field>` or `dtk config delete <config>`
- do not mutate a DTK config silently; ask first, then update the config after the user confirms
- think in terms of reusable list-view and decision fields, not one-off indexes
- if the user repeatedly asks for the same structural field across multiple items in a list or array, treat that as demand for the collection-level version of that field
- normalize indexed retrievals into wildcard-style reasoning before deciding whether the config should change
- when repeated retrievals indicate that exposing the field would improve the next filtered list view, comparison step, ranking step, or scanning step, proactively ask whether to add that field to the allowlist
- favor promoting stable repeated fields into the config when doing so will reduce repeated `dtk retrieve` calls and make the next filtered payload more useful
- for collections and arrays, prefer reusable wildcard paths over one-off index-specific paths when suggesting config updates

## Prompt Flow

DTK is designed to work like an agent-facing prompt workflow, not as an install-time hook.
RTK and DTK are complementary:

- Always use `dtk` directly for DTK-native commands such as `dtk exec`, `dtk retrieve`, `dtk config ...`, `dtk doctor`, `dtk install`, `dtk install-dummy`, and `dtk uninstall`.
- For ordinary shell commands, prefer the combined wrapper `rtk dtk exec -- <command> [args...]` when you want token savings and DTK routing together.
- `dtk` should inspect the command and use a matching config or hook rule when one exists, but if nothing matches it should return the original command or payload unchanged.
- Use DTK whenever the command can be filtered, wrapped, or handled by a DTK config or hook rule.
- Do not default to `rtk proxy` for curl/API/JSON flows when `dtk` can handle them.

Examples:

```bash
dtk exec --config n8n_workflows_list.json -- \
  curl -sS -H "X-N8N-API-KEY: $N8N_API_TOKEN" "$N8N_BASE_URL/api/v1/workflows?limit=3"

dtk exec -- \
  curl -sS https://dummyjson.com/users

dtk exec --config kubectl_pods.yaml.json -- \
  kubectl get pods -o yaml

dtk exec --config cargo_lock_packages.toml.json -- \
  cat Cargo.lock

dtk exec --config pyproject_manifest.toml.json -- \
  cat pyproject.toml

dtk exec --config xaml_resource_dictionary.xaml.json -- \
  cat App.xaml

CSV is useful for inventory exports and other repeated tabular payloads:

dtk exec --config csv_inventory_export.csv.json -- \
  cat inventory.csv

INI is useful for repeated section-based configs:

dtk exec --config ini_plugin_registry.ini.json -- \
  cat plugins.ini

XML is useful for repeated nested feeds and manifests:

dtk exec --config xml_rss_feed.xml.json -- \
  cat feed.xml

HCL is useful for repeated Terraform module variables:

dtk exec --config terraform_module_variables.tf.json -- \
  cat variables.tf

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
5. Before changing a config, run `dtk config list` to confirm the installed identifier you should target.
6. After a config exists, use DTK-native config commands when you want to increase or decrease the config `allow` surface without recreating the config from scratch.
7. When `dtk retrieve` or `dtk exec` emits a `DTK recommendation:` notice, treat that as a prompt to ask the user whether to add or remove specific fields from the config, or remove the config entirely if it is not reducing token usage enough for the endpoint. Include the concrete follow-up command in the message: `dtk config list` first, then `dtk config allow add <config> <field>`, `dtk config allow remove <config> <field>`, or `dtk config delete <config>` as appropriate.
8. Use `dtk retrieve` when you need to pull a few fields back out of a stored payload.
   Example:

```bash
dtk retrieve <ref_id> 'users[0].firstName,users[0].lastName'
```
9. If the user keeps retrieving the same structural field across different items in a collection, ask inline whether they want the reusable collection-level version of that field added to the config for future filtered views.
10. If repeated retrievals or repeated fallbacks trigger a `DTK recommendation:` notice, ask the user inline whether they want the config changed for that endpoint, and include the exact DTK config command they should run next.
11. When a DTK rule matches or can be created, prefer DTK routing first; keep RTK in the stack whenever it would normally be used for token savings.
