# GitHub Copilot Metrics Question - Lines Generated in Agent Mode Not Tracked?

## Summary
I used GitHub Copilot in VS Code **agent mode** to generate over 4,000 lines of code, committed and pushed them, but the Copilot usage metrics API shows **0 lines** for code completions - only chat activity is recorded.

## Details

**What I did:**
- Used GitHub Copilot in VS Code with agent mode (Copilot Chat with extensive code generation)
- Generated and accepted over 4,000 lines of code
- Committed and pushed the code to the repository
- Team: `test-team` in organization `Pathlock`
- Date: January 30, 2026

**Expected Result:**
Metrics should show:
- `code_lines_accepted`: ~4,000
- `code_suggestions`: Multiple suggestions
- `total_engaged_users`: >0 for code completions

**Actual Result:**
When fetching metrics via the GitHub Copilot Metrics API:

```
GET /orgs/Pathlock/team/test-team/copilot/metrics?since=2026-01-24T00:00:00Z&until=2026-01-30T23:59:59Z
```

Response shows:
```json
{
  "date": "2026-01-30",
  "total_active_users": 5,
  "total_engaged_users": 5,
  "copilot_ide_chat": {
    "editors": [
      {
        "name": "vscode",
        "models": [
          {
            "name": "default",
            "total_chats": 26,
            "total_engaged_users": 1,
            "total_chat_copy_events": 0,
            "total_chat_insertion_events": 0
          }
        ],
        "total_engaged_users": 1
      },
      {
        "name": "VisualStudio",
        "models": [
          {
            "name": "default",
            "total_chats": 24,
            "total_engaged_users": 1,
            "total_chat_copy_events": 6,
            "total_chat_insertion_events": 0
          }
        ],
        "total_engaged_users": 1
      },
      {
        "name": "JetBrains",
        "models": [
          {
            "name": "default",
            "total_chats": 1,
            "total_engaged_users": 1,
            "total_chat_copy_events": 0,
            "total_chat_insertion_events": 0
          }
        ],
        "total_engaged_users": 1
      }
    ],
    "total_engaged_users": 3
  },
  "copilot_dotcom_chat": {
    "models": [
      {
        "name": "default",
        "total_chats": 22,
        "total_engaged_users": 3
      }
    ],
    "total_engaged_users": 3
  },
  "copilot_dotcom_pull_requests": {
    "total_engaged_users": 0
  },
  "copilot_ide_code_completions": {
    "total_engaged_users": 0
  }
}
```

**Notice:** The `copilot_ide_code_completions` section is **completely empty** - no `editors` array, no language breakdowns, no line metrics.

Only chat activity is tracked:
- IDE chats: 51 total (VS Code: 26, Visual Studio: 24, JetBrains: 1)
- Dotcom chats: 22
- But `code_lines_accepted`: 0, `code_lines_suggested`: 0

## Question

**Are lines of code generated through GitHub Copilot's agent/agentic mode tracked in the usage metrics API?**

Specifically:
1. When Copilot generates code through the chat interface (agent mode) rather than inline completions/suggestions, are those lines counted in `copilot_ide_code_completions`?
2. Should code generated via agent mode appear in the `total_code_lines_accepted` and `total_code_lines_suggested` metrics?
3. Or are these metrics **only** for traditional tab-completion style suggestions, not chat-generated code?

## Context

I'm building internal metrics dashboards and need to understand:
- Whether agentic mode code generation is currently tracked separately or not at all
- If there's a different API endpoint or metric for chat-generated code
- Whether this is expected behavior or a potential gap in metrics collection

Any clarification would be greatly appreciated!

---

**Environment:**
- VS Code with GitHub Copilot extension
- Using agent/chat mode for code generation
- Copilot Metrics API v2022-11-28
- Organization-level metrics tracking
