"""LLM generates the fix as a unified diff.

Supports two providers, selected by LLM_PROVIDER in .env:
  gemini     — Google Gemini via google-generativeai (default; free tier)
  anthropic  — Anthropic Claude via anthropic SDK
"""

from __future__ import annotations

_SYSTEM = """\
You are a senior data engineer who fixes broken Apache Airflow DAGs.
You receive:
  - The failing DAG's Python source code
  - The classified error type
  - The actual traceback from the Airflow failure email

Your job:
1. Identify exactly which line(s) caused the failure.
2. Write the minimal, defensive fix — do not refactor or rename anything unrelated.
3. Return ONLY a unified diff (--- original / +++ fixed), nothing else.
   No markdown fences, no explanation, no preamble.
"""

_FIX_PROMPTS: dict[str, str] = {
    "format_mismatch": (
        "The task crashed because it called pd.read_csv() on a non-CSV file "
        "(e.g. a PDF, Excel or JSON file landed in the data directory). "
        "Add a file-extension check: skip any file whose suffix is not .csv, "
        "log a warning with the filename, and continue processing the rest. "
        "Do not change any other logic."
    ),
    "missing_file": (
        "The task crashed with FileNotFoundError because the expected file or directory "
        "was absent. Add a guard that raises AirflowSkipException with a descriptive "
        "message so the DAG run is marked Skipped rather than Failed. "
        "Do not change any other logic."
    ),
    "permission_error": (
        "The task crashed with PermissionError. Add a try/except block that catches "
        "PermissionError, logs a clear message including the file path, and either "
        "skips or re-raises as AirflowException so the failure is obvious in the UI. "
        "Do not change any other logic."
    ),
    "schema_change": (
        "The task crashed with KeyError or a column-not-found error, which means the "
        "upstream data schema changed. Add a schema validation step before accessing "
        "columns: check that the expected column names are present, log which columns "
        "are missing, and raise AirflowException with a clear diff of expected vs actual. "
        "Do not change any other logic."
    ),
    "connection_error": (
        "The task crashed because a database or API connection timed out or was refused. "
        "Wrap the connection call with a retry loop using exponential back-off "
        "(3 retries, 10 s / 30 s / 60 s). Raise AirflowException after all retries "
        "are exhausted so the task is marked Failed rather than hanging. "
        "Do not change any other logic."
    ),
    "import_error": (
        "The task crashed with ModuleNotFoundError or ImportError. "
        "Add a try/except ImportError block that catches the missing import, logs which "
        "package is missing and suggests `pip install <package>`, then raises "
        "AirflowException so the run fails cleanly. "
        "Do not change any other logic."
    ),
    "memory_error": (
        "The task crashed with MemoryError or was OOM-killed. "
        "Refactor the data processing to use chunked reading (e.g. pd.read_csv chunksize) "
        "so the full file is never loaded into RAM at once. Keep the same output shape. "
        "Do not change any other logic."
    ),
    "type_error": (
        "The task crashed with TypeError. "
        "Add input validation before the failing call: check that the argument types match "
        "what the function expects, convert them if a safe conversion exists (e.g. str→int), "
        "or raise AirflowException with a clear message if conversion is impossible. "
        "Do not change any other logic."
    ),
    "value_error": (
        "The task crashed with ValueError. "
        "Add a guard that validates the value before it is used, logs the bad value, "
        "and raises AirflowException with a clear message describing what value was expected. "
        "Do not change any other logic."
    ),
    "dependency_failed": (
        "The task was blocked because an upstream task failed or depends_on_past is "
        "preventing execution. This is not a code bug — do not change the DAG logic. "
        "Instead, add a comment above the relevant task explaining the dependency chain, "
        "and if trigger_rule is not set, set it to TriggerRule.ALL_SUCCESS explicitly "
        "so the intent is clear. Return the minimal diff."
    ),
    "unknown": (
        "The error type could not be automatically classified. "
        "Analyse the traceback carefully, identify the root cause, "
        "and write the minimal defensive fix. "
        "If the fix requires a library that is not already imported, add the import."
    ),
}


def generate_fix(fix_context: dict, api_key: str, provider: str = "gemini", model: str = "") -> str:
    """Ask the LLM to fix the DAG. Returns a unified diff string."""
    error_type = fix_context["error_type"]
    dag_code   = fix_context["dag_code"]
    error_snip = fix_context["error_snippet"]
    hint       = _FIX_PROMPTS.get(error_type, _FIX_PROMPTS["unknown"])

    user_msg = f"""\
## DAG source
```python
{dag_code}
```

## Error type
{error_type}

## Traceback from Airflow email
```
{error_snip}
```

## Fix instruction
{hint}

Return ONLY the unified diff. No explanation, no markdown fences.
"""
    if provider == "anthropic":
        return _call_anthropic(api_key, user_msg, model or "claude-sonnet-4-6")
    else:
        return _call_gemini(api_key, user_msg, model or "models/gemini-3.5-flash")


_GEMINI_FALLBACKS = [
    "models/gemini-3.5-flash",
    "models/gemini-3.6-flash",
    "models/gemini-flash-lite-latest",
]


def _call_gemini(api_key: str, user_msg: str, model: str) -> str:
    import time
    import logging
    from google import genai
    from google.genai import types

    log = logging.getLogger("triage.fixer")
    client = genai.Client(api_key=api_key)

    # Build the list: requested model first, then fallbacks (deduped)
    models_to_try = [model] + [m for m in _GEMINI_FALLBACKS if m != model]

    for m in models_to_try:
        last_exc = None
        for wait in [0, 20]:   # two attempts per model before moving on
            if wait:
                time.sleep(wait)
            try:
                response = client.models.generate_content(
                    model=m,
                    contents=user_msg,
                    config=types.GenerateContentConfig(system_instruction=_SYSTEM),
                )
                if m != model:
                    log.info("Used fallback model %s (primary %s unavailable)", m, model)
                return response.text.strip()
            except Exception as exc:
                last_exc = exc
                err = str(exc)
                if "503" in err or "UNAVAILABLE" in err or "overload" in err.lower():
                    continue   # retry / try next model
                if "404" in err or "NOT_FOUND" in err:
                    break      # this model doesn't exist — skip to next
                raise          # any other error is real — fail fast
        log.warning("Model %s unavailable, trying next...", m)

    raise last_exc


def _call_anthropic(api_key: str, user_msg: str, model: str) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model=model,
        max_tokens=1024,
        system=_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    return message.content[0].text.strip()
