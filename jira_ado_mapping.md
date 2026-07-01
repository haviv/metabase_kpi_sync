# ADO to Jira Mapping for NEXUS UPE

Date: 2026-06-24

This document maps the `bugs` and `work_items` columns populated by `export_ado.py` to Jira fields for the NEXUS UPE board.

Live Jira context checked:

- Board: `1766`, name `UPE`, type `scrum`
- Project: `NEXUS`, name `Pathlock Nexus`
- Issue types on project: `Task`, `Sub-task`, `Story`, `Bug`, `Epic`
- Board sample size: 139 issues: 30 `Story`, 103 `Sub-task`, 6 `Bug`
- Components in use: `Universal Policy Editor`, `Data Policies (DAC)`
- Project versions: 0 Jira project versions found
- Statuses: `OPEN`, `IN PROGRESS`, `BLOCKED`, `CODE REVIEW`, `READY FOR QA`, `IN QA`, `DONE`, `CLOSED`

Important notes:

- `JIRA_PROJECT_KEY` currently points to `PN`; use `JIRA_NEXUS_PROJECT_KEY=NEXUS` for this UPE migration.
- ADO IDs are integers. Jira has both a string key, for example `NEXUS-137`, and an internal numeric issue id. For a Jira-backed table, prefer storing the Jira key in `id` and adding/using `numeric_id` for the internal Jira id.
- Some Jira fields exist on the instance but are empty on the UPE board right now. These are marked as "field exists, empty on UPE".
- `export_jira.py` already maps several standard Jira fields, but it does not yet cover all of the NEXUS migrated custom fields listed here.

## Table: `bugs`

| column_psql | ado | jira | transformation needed | desc/notes |
|---|---|---|---|---|
| `id` | `System.Id` | issue `key`; optional internal `issue.id` as `numeric_id` | Yes | Current `bugs.id` is integer. Jira issue key is string, for example `NEXUS-137`. Schema/code must support string ids or keep Jira internal id separately. |
| `parent_issue` | `System.Parent`; also ADO hierarchy relations | `parent.key`; possibly `customfield_10014` Epic Link or issue links | Yes | Jira `parent` is populated for sub-tasks, not for current UPE bugs. For bugs, parent/epic relationship needs live semantic validation. |
| `title` | `System.Title` | `summary` | No | Direct text mapping. |
| `description` | `System.Description` | `description` | Yes | Jira Cloud description is Atlassian Document Format. Convert ADF to plain text or rendered HTML before storing. |
| `assigned_to` | `System.AssignedTo.displayName` | `assignee.displayName` | No | Direct user display-name extraction. UPE board has this populated on 138/139 issues. |
| `severity` | For ADO bugs, code stores `Microsoft.VSTS.Common.Priority` | `priority.name` | Yes | Jira priority values observed: `Lowest (migrated)`, `1 - Critical`. Existing KPIs may expect `1`, `2`, etc.; normalize priority names if dashboards depend on numeric severity. |
| `state` | `System.State` | `status.name` | Yes | Jira statuses differ from ADO states. Map to KPI buckets: open/dev/QA/closed. |
| `customer_name` | `Custom.CustomernameGRC` | Candidate `customfield_12470` Customer name GRC; current env uses `customfield_10360` Customer Name | Yes | Both fields exist, both are empty on UPE. `customfield_12470` is the closest name match to ADO; `customfield_10360` is already configured but appears generic/multiselect. Needs business confirmation. |
| `area_path` | `System.AreaPath` | `components[].name` | Yes | Best Jira equivalent for UPE team/product area. Current values: `Universal Policy Editor`, `Data Policies (DAC)`. Not a one-to-one path hierarchy. |
| `created_date` | `System.CreatedDate` | `created` | Yes | Parse Jira timezone timestamp, for example `2026-06-23T08:28:20.191-0400`. |
| `changed_date` | `System.ChangedDate` | `updated` | Yes | Parse Jira timezone timestamp. |
| `iteration_path` | `System.IterationPath` | `customfield_10020` Sprint, fallback `sprint`/`closedSprints` | Yes | Jira sprint is array/json. Store current or all sprint names joined. UPE has `customfield_10020` populated on 130/139 issues. |
| `hotfix_delivered_version` | `Custom.HotfixDeliveredVersions` | `customfield_12480` Hotfix Delivered Versions | Maybe | Field exists, empty on UPE. String field; can be collected if populated. |
| `target_date` | `Microsoft.VSTS.Scheduling.TargetDate` | `customfield_12496` Target Date; possible fallback `duedate` | Maybe | `customfield_12496` exists and is the closest name match, empty on UPE. `duedate` also exists but empty. |
| `hf_status` | `Custom.HFstatus` | `customfield_12479` HF status | Maybe | Field exists, empty on UPE. Option field; store selected option value. |
| `hf_requested_versions` | `Custom.HFrequestedversions` | `customfield_12478` HF requested versions | Maybe | Field exists, empty on UPE. Text field; can be collected if populated. |

## Table: `work_items`

| column_psql | ado | jira | transformation needed | desc/notes |
|---|---|---|---|---|
| `id` | `System.Id` | issue `key`; optional internal `issue.id` as `numeric_id` | Yes | Current `work_items.id` is integer. Jira key is string. |
| `title` | `System.Title` | `summary` | No | Direct text mapping. |
| `description` | `System.Description` | `description` | Yes | Convert Jira ADF to text/HTML. |
| `assigned_to` | `System.AssignedTo.displayName` | `assignee.displayName` | No | Direct display-name extraction. |
| `severity` | Code stores `Microsoft.VSTS.Common.Priority`, except ADO `Issue Report` uses first character of `Severity` | `priority.name` | Yes | Normalize if KPI logic expects numeric priority. Jira priority is populated on all UPE issues. |
| `state` | `System.State` | `status.name` | Yes | Need status normalization for KPI compatibility. |
| `customer_name` | `Custom.CustomernameGRC` | Candidate `customfield_12470` Customer name GRC; current env uses `customfield_10360` Customer Name | Yes | Field exists but empty on UPE. Use `customfield_12470` if the intent is the migrated ADO customer field; confirm before coding. |
| `area_path` | `System.AreaPath` | `components[].name` | Yes | Store comma-separated component names. |
| `created_date` | `System.CreatedDate` | `created` | Yes | Parse timestamp. |
| `changed_date` | `System.ChangedDate` | `updated` | Yes | Parse timestamp. |
| `iteration_path` | `System.IterationPath` | `customfield_10020` Sprint | Yes | Extract sprint names from Jira sprint JSON/list. |
| `hotfix_delivered_version` | `Custom.HotfixDeliveredVersions` | `customfield_12480` Hotfix Delivered Versions | Maybe | Field exists, empty on UPE. |
| `work_item_type` | `System.WorkItemType` | `issuetype.name` | Yes | UPE supports `Task`, `Sub-task`, `Story`, `Bug`, `Epic`. Need map old ADO types like `Product Backlog Item`, `Issue Report`, `Feature` if dashboards depend on old names. |
| `target_date` | `Microsoft.VSTS.Scheduling.TargetDate` | `customfield_12496` Target Date; possible fallback `duedate` | Maybe | Field exists, empty on UPE. |
| `hf_status` | `Custom.HFstatus` | `customfield_12479` HF status | Maybe | Field exists, empty on UPE. |
| `hf_requested_versions` | `Custom.HFrequestedversions` | `customfield_12478` HF requested versions | Maybe | Field exists, empty on UPE. |
| `effort` | `Microsoft.VSTS.Scheduling.Effort` | Candidate `customfield_10619` Effort or Jira `timeoriginalestimate` | Yes | `customfield_10619` exists but is empty. Jira original estimate is populated on 9 UPE stories in seconds. Decide whether old ADO effort means points/days or time estimate. |
| `effort_dev_estimate` | `Custom.EffortDevestimate` | `customfield_10623` Effort Dev estimate | Yes | Field exists and is populated on 6 UPE stories. It is a text field, so parse to float. |
| `effort_dev_actual` | `Custom.EffortDevactual` | `customfield_10624` Effort Dev actual | Yes | Field exists, empty on UPE. Text field, so parse to float when populated. |
| `qa_effort_estimation` | `Custom.QAeffortestimation` | `customfield_12484` QA effort estimation | No/Maybe | Field exists and is populated on 8 UPE stories. Numeric float. |
| `qa_effort_actual` | `Custom.QAeffortactual` | `customfield_12483` QA effort actual | No/Maybe | Field exists, empty on UPE. Numeric float. |
| `tshirt_estimation` | `Custom.TShirtestimation` | `customfield_12489` TShirt estimation | Yes | Field exists and is populated on 6 UPE stories. Option values observed: `XS - 1-2 days`, `S - 3-5 days`, `M - 6-10 days`. Store option value string. |
| `parent_work_item` | `System.Parent` | `parent.key`; possibly `customfield_10014` Epic Link | Yes | Current table expects integer. Jira parent is key string. Parent is populated for all current UPE sub-tasks and one story. |
| `ticket_type` | `Custom.Tickettype` | Candidate `customfield_12467` Bug Type or `issuetype.name` | Yes/Unknown | `Bug Type` exists and is populated on 1 UPE bug with `Regression`. If ADO ticket type was broader than bug classification, this needs confirmation. |
| `freshdesk_ticket` | `Custom.FreshdeskTicket` | `customfield_12476` Freshdesk Ticket | Maybe | Field exists, empty on UPE. Text field. |
| `target_version` | `Custom.Targetversion` | `customfield_12490` Target version; possible fallback `customfield_12493` Version or `fixVersions` | Yes/Unknown | `Target version` exists but empty. `Version` has 1 bug value (`2025.2.1`). Jira project has no project versions, so `fixVersions` is currently empty/unusable unless versions are created. |
| `tags` | `System.Tags` | `labels[]` | Yes | Jira labels exist but are empty on UPE. Store comma-separated labels to preserve current schema. |
| `connector` | `Custom.Connector` | `customfield_12509` Nexus Connector; fallback `customfield_11384` Connector | Yes | `Nexus Connector` is populated on 28 UPE stories with `SAP`. Generic `Connector` exists but is empty. Use `customfield_12509` for NEXUS/UPE. |
| `created_by` | `System.CreatedBy.displayName` | `creator.displayName` | No | Jira also has `reporter`. ADO `CreatedBy` is closest to Jira `creator`; `reporter` is the issue reporter and may differ. |
| `blocker` | `Custom.Blocker` | `customfield_12466` Blocker | Yes | Field exists and is populated on 34 UPE issues. Option values observed: `False`, `True`. |
| `business_value` | `Microsoft.VSTS.Common.BusinessValue` | Candidate `customfield_10620` Business Value | Yes/Unknown | Field exists but empty on UPE and is text, while current DB expects integer. Needs confirmation or parse-to-int rule. |
| `business_outcome` | `Custom.BusinessOutcome` | `customfield_12468` Business Outcome | Yes | Field exists and is populated on 28 UPE stories. Option value observed: `Mid-Market UI & Core Experience Refresh`. |

## Additional Jira fields already handled by `export_jira.py`

These are Jira-native or Jira-specific fields that are not in the original ADO `bugs` / `work_items` schemas, but the current Jira exporter already knows about some of them:

| jira field | current `export_jira.py` target | notes |
|---|---|---|
| `customfield_10856` Test Iterations | `test_iterations` | Field exists but empty on UPE. |
| `customfield_10955` Bugs Found | `bugs_found` | Field exists but empty on UPE. Current code treats it as numeric, but Jira schema says option. |
| `customfield_10814` Regression | `regression` | Field exists but empty on UPE. Jira schema is multi-checkbox array. |
| `customfield_10454` Tester | `tester` | Field exists but empty on UPE. |
| `customfield_10037` Story Points | `story_points` | Populated on 1 UPE story. |
| `worklog` / `timespent` / `aggregatetimespent` | `time_spent`, `worklog_total_time_spent`, `worklog_entries_count` | UPE worklogs currently have zero entries across fetched board issues. |

## Recommended extraction rules

1. Use `JIRA_NEXUS_PROJECT_KEY=NEXUS` for the UPE board and keep `JIRA_PROJECT_KEY=PN` for the existing PN flow.
2. Use Jira issue key as the business id for new Jira tables, and keep Jira internal numeric id in a separate `numeric_id` column.
3. Normalize Jira statuses into the existing KPI buckets before reusing old dashboard SQL:
   - Open: `OPEN`, `BLOCKED`
   - Development/code: `IN PROGRESS`, `CODE REVIEW`
   - QA: `READY FOR QA`, `IN QA`
   - Closed: `DONE`, `CLOSED`
4. Normalize Jira priority names if existing KPI SQL expects numeric severity:
   - `1 - Critical` -> `1`
   - `2 - High` -> `2`
   - Keep migrated names like `Lowest (migrated)` either as-is or map them to a low numeric priority only if the business agrees.
5. Treat these fields as confirmed Jira equivalents for NEXUS/UPE:
   - `customfield_12478` HF requested versions
   - `customfield_12479` HF status
   - `customfield_12480` Hotfix Delivered Versions
   - `customfield_10623` Effort Dev estimate
   - `customfield_10624` Effort Dev actual
   - `customfield_12483` QA effort actual
   - `customfield_12484` QA effort estimation
   - `customfield_12489` TShirt estimation
   - `customfield_12476` Freshdesk Ticket
   - `customfield_12466` Blocker
   - `customfield_12468` Business Outcome
   - `customfield_12509` Nexus Connector
6. Treat these mappings as needing business confirmation:
   - `customer_name`: `customfield_12470` vs `customfield_10360`
   - `effort`: `customfield_10619` vs Jira original estimate/time tracking
   - `target_version`: `customfield_12490` vs `customfield_12493` vs `fixVersions`
   - `ticket_type`: `customfield_12467` Bug Type vs Jira `issuetype.name`
   - `business_value`: `customfield_10620`, text field, currently empty

