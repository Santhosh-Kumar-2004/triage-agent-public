"""Apply a unified diff to the repo and open a GitHub pull request."""

from __future__ import annotations

import subprocess
import tempfile
import textwrap
from pathlib import Path


def create_pr(
    dag_id: str,
    patch: str,
    error_type: str,
    repo_local_path: str,
    github_repo: str,       # "owner/repo"
    base_branch: str = "main",
) -> str:
    """Apply `patch`, push a branch, open a PR. Returns the PR URL."""
    repo = Path(repo_local_path)
    branch = f"triage/{dag_id}-{error_type.replace('_', '-')}"

    _git(repo, ["checkout", base_branch])
    _git(repo, ["pull", "origin", base_branch])
    _git(repo, ["checkout", "-b", branch])

    _apply_patch(repo, patch)

    _git(repo, ["add", "-A"])
    _git(repo, ["commit", "-m", f"triage: fix {error_type} in DAG {dag_id}"])
    _git(repo, ["push", "-u", "origin", branch])

    body = textwrap.dedent(f"""\
        ## Triage agent auto-fix

        **DAG:** `{dag_id}`
        **Error type:** `{error_type}`

        ### What broke
        The pipeline received a file in an unexpected format.
        `pd.read_csv()` raised a `ParserError` and the task failed.

        ### What the agent changed
        Added a file-extension guard so non-CSV files are skipped with a
        warning instead of crashing the whole task.

        ### Confidence
        High — this is a deterministic pattern (format mismatch).
        Review the diff, run the DAG on a test file, then merge.

        ---
        *Opened automatically by the triage agent.*
    """)

    result = subprocess.run(
        ["gh", "pr", "create",
         "--repo", github_repo,
         "--base", base_branch,
         "--head", branch,
         "--title", f"[triage] fix {error_type} in {dag_id}",
         "--body", body],
        capture_output=True, text=True, cwd=repo,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh pr create failed: {result.stderr}")

    # gh prints the PR URL as the last line
    return result.stdout.strip().splitlines()[-1]


# ── helpers ───────────────────────────────────────────────────────────────────

def _git(repo: Path, args: list[str]) -> None:
    subprocess.run(["git"] + args, cwd=repo, check=True)


def _apply_patch(repo: Path, patch: str) -> None:
    with tempfile.NamedTemporaryFile(suffix=".patch", mode="w", delete=False) as f:
        f.write(patch)
        patch_file = f.name
    result = subprocess.run(
        ["git", "apply", "--whitespace=fix", patch_file],
        cwd=repo, capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Fallback: write the full fixed file directly (Claude sometimes
        # returns the full file instead of a diff)
        raise RuntimeError(
            f"git apply failed:\n{result.stderr}\n\n"
            "Tip: check that PATCH in the Burr state is a valid unified diff."
        )
