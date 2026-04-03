from __future__ import annotations

import argparse
from pathlib import Path
import sys

from dotenv import load_dotenv
from huggingface_hub import snapshot_download

from .config import apply_overrides, load_config
from .dataset import filter_tasks, load_tasks
from .evaluation import run_sync
from .neon_publish import publish_tracker_to_postgres
from .python_exec_smoke import run_python_exec_smoke
from .reporting import rebuild_outputs
from .task_map import build_task_map_rows, generate_task_map, write_task_map
from .tracker import promote_run, rebuild_tracker
from .utils import shorten
from .value_model import load_value_overrides, resolve_value_for_task, seed_value_file


def _print_task_table(rows: list[dict]) -> None:
    headers = ["task_id", "attachments", "input_mb", "value_base", "task_description"]
    widths = {
        "task_id": 8,
        "attachments": 11,
        "input_mb": 10,
        "value_base": 12,
        "task_description": 80,
    }
    header_line = (
        f"{'task_id':<{widths['task_id']}} "
        f"{'attachments':<{widths['attachments']}} "
        f"{'input_mb':<{widths['input_mb']}} "
        f"{'value_base':<{widths['value_base']}} "
        f"{'task_description':<{widths['task_description']}}"
    )
    print(header_line)
    print("-" * len(header_line))
    for row in rows:
        print(
            f"{str(row['task_id']):<{widths['task_id']}} "
            f"{str(row['attachment_count']):<{widths['attachments']}} "
            f"{format(float(row['attachment_total_mb']), '.3f'):<{widths['input_mb']}} "
            f"{('$' + format(row['value_base_usd'], '.2f')):<{widths['value_base']}} "
            f"{shorten(row['task_description'], widths['task_description'])}"
        )


def cmd_download_dataset(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_download(
        repo_id=args.repo_id,
        repo_type="dataset",
        local_dir=str(output_dir),
        allow_patterns=["data/*", "documents/*", "README.md"],
    )
    print(f"Downloaded dataset to {output_dir}")
    return 0


def cmd_seed_values(args: argparse.Namespace) -> int:
    path = seed_value_file(
        dataset_dir=args.dataset_dir,
        output_csv=args.output,
        domain=args.domain,
        task_metadata_path=args.task_metadata_csv,
        default_hours=args.default_hours,
        low_rate=args.low_rate,
        base_rate=args.base_rate,
        high_rate=args.high_rate,
        force=args.force,
    )
    print(f"Wrote {path}")
    return 0


def cmd_list_tasks(args: argparse.Namespace) -> int:
    tasks = filter_tasks(load_tasks(args.dataset_dir), domain=args.domain, start_index=args.start_index, limit=args.limit)
    task_map_rows = {
        int(row["task_id"]): row
        for row in build_task_map_rows(args.dataset_dir, tasks, task_metadata_path=args.task_metadata_csv)
    }
    overrides = load_value_overrides(args.values_csv) if args.values_csv else {}
    rows = []
    for task in tasks:
        value = resolve_value_for_task(
            task,
            overrides,
            default_hours=args.default_hours,
            low_rate=args.low_rate,
            base_rate=args.base_rate,
            high_rate=args.high_rate,
        )
        task_map_row = task_map_rows.get(task.task_id, {})
        rows.append(
            {
                "task_id": task.task_id,
                "attachment_count": int(task_map_row.get("attachment_count", task.attachment_count) or 0),
                "attachment_total_mb": float(task_map_row.get("attachment_total_mb", 0.0) or 0.0),
                "value_base_usd": value.value_base_usd,
                "task_description": str(task_map_row.get("task_description", task.task_description) or task.task_description),
            }
        )
    _print_task_table(rows)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    load_dotenv()
    load_dotenv(".env.local", override=False)
    config = load_config(args.config)
    task_ids = [int(value) for value in args.task_ids] if args.task_ids else None
    config = apply_overrides(config, output_dir=args.output_dir, limit=args.limit, task_ids=task_ids)
    run_sync(config)
    if config.tracking.auto_refresh_on_run:
        rebuild_tracker(
            config.tracking.outputs_root,
            config.tracking.tracker_dir,
            price_book_path=config.pricing.openai_price_book,
            task_metadata_path=config.tracking.task_metadata_csv,
        )
    print(f"Run complete. Outputs written to {config.output_dir}")
    return 0


def cmd_summarize(args: argparse.Namespace) -> int:
    task_rows, overall = rebuild_outputs(args.run_jsonl, args.output_dir)
    print(f"Rebuilt summaries for {len(task_rows)} tasks.")
    print(f"Overall pass rate: {overall['overall_pass_rate']:.6f}")
    print(f"Mean cost per attempt: ${overall['mean_total_cost_per_attempt_usd']:.6f}")
    return 0


def cmd_map_tasks(args: argparse.Namespace) -> int:
    task_ids = [int(value) for value in args.task_ids] if args.task_ids else None
    rows = generate_task_map(
        args.dataset_dir,
        task_metadata_path=args.task_metadata_csv,
        domain=args.domain,
        task_ids=task_ids,
        start_index=args.start_index,
        limit=args.limit,
        sort_by=args.sort_by,
        descending=args.descending,
    )
    path = write_task_map(args.output, rows, fmt=args.format)

    domain_counts: dict[str, int] = {}
    for row in rows:
        domain = str(row["domain"])
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
    summary = ", ".join(f"{domain}={count}" for domain, count in sorted(domain_counts.items()))

    print(f"Wrote {len(rows)} task rows to {path}")
    if summary:
        print(f"Domains: {summary}")
    return 0


def cmd_rebuild_tracker(args: argparse.Namespace) -> int:
    summary = rebuild_tracker(
        args.outputs_root,
        args.tracker_dir,
        price_book_path=args.openai_price_book,
        task_metadata_path=args.task_metadata_csv,
    )
    print(f"Discovered attempts: {summary['discovered_attempts']}")
    print(f"Promoted attempts: {summary['promoted_attempts']}")
    print(f"Tracked task setups: {summary['tracked_task_setups']}")
    print(f"Tracker written to {summary['tracker_dir']}")
    return 0


def cmd_promote_run(args: argparse.Namespace) -> int:
    selected_count, summary = promote_run(
        tracker_dir=args.tracker_dir,
        output_dir=args.output_dir,
        run_jsonl_path=args.run_jsonl,
        task_id=args.task_id,
        run_index=args.run_index,
        promote_all=args.all,
        label=args.label or "",
        notes=args.notes or "",
        headline=bool(args.headline),
        outputs_root=args.outputs_root,
        price_book_path=args.openai_price_book,
        task_metadata_path=args.task_metadata_csv,
    )
    print(f"Promoted attempts: {selected_count}")
    print(f"Discovered attempts: {summary['discovered_attempts']}")
    print(f"Promoted attempts in tracker: {summary['promoted_attempts']}")
    print(f"Tracked task setups: {summary['tracked_task_setups']}")
    return 0


def cmd_publish_neon(args: argparse.Namespace) -> int:
    load_dotenv()
    load_dotenv(".env.local", override=False)
    summary = publish_tracker_to_postgres(
        tracker_dir=args.tracker_dir,
        database_url=args.database_url,
        schema=args.schema,
    )
    print(f"Published schema: {summary['schema']}")
    print(f"Task provenances: {summary['task_provenances']}")
    print(f"Task setups: {summary['task_setups']}")
    print(f"Promoted attempts: {summary['promoted_attempts']}")
    print(f"Tracker source: {summary['tracker_dir']}")
    print(f"Database URL source: {summary['database_url_env_used']}")
    return 0


def cmd_smoke_python_exec(args: argparse.Namespace) -> int:
    load_dotenv()
    load_dotenv(".env.local", override=False)
    config = load_config(args.config)
    if args.output_dir:
        config.output_dir = Path(args.output_dir).resolve()
    result = run_python_exec_smoke(
        config,
        output_dir=config.output_dir,
        row_count=args.row_count,
    )
    summary = result["smoke_summary"]
    print(f"Smoke output: {config.output_dir}")
    print(f"Generation success: {summary['generation_success']}")
    print(f"Python exec called: {summary['python_exec_called']}")
    print(f"Python exec success: {summary['python_exec_success']}")
    print(f"Exact match: {summary['exact_match']}")
    print(f"Smoke passed: {summary['smoke_passed']}")
    return 0 if summary["smoke_passed"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run public APEX evals with Reducto parsing, Daytona assistance, and cost accounting.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download-dataset", help="Download the public APEX dataset from Hugging Face.")
    download.add_argument("--repo-id", default="mercor/APEX-v1-extended")
    download.add_argument("--output-dir", required=True)
    download.set_defaults(func=cmd_download_dataset)

    seed = subparsers.add_parser("seed-values", help="Seed a finance value CSV.")
    seed.add_argument("--dataset-dir", required=True)
    seed.add_argument("--domain", default="Finance")
    seed.add_argument("--output", required=True)
    seed.add_argument("--default-hours", type=float, default=3.5)
    seed.add_argument("--low-rate", type=float, default=100.0)
    seed.add_argument("--base-rate", type=float, default=150.0)
    seed.add_argument("--high-rate", type=float, default=250.0)
    seed.add_argument("--task-metadata-csv", default="configs/task_metadata.csv")
    seed.add_argument("--force", action="store_true")
    seed.set_defaults(func=cmd_seed_values)

    list_tasks = subparsers.add_parser("list-tasks", help="List tasks in the selected domain.")
    list_tasks.add_argument("--dataset-dir", required=True)
    list_tasks.add_argument("--domain", default="Finance")
    list_tasks.add_argument("--values-csv", default=None)
    list_tasks.add_argument("--start-index", type=int, default=0)
    list_tasks.add_argument("--limit", type=int, default=None)
    list_tasks.add_argument("--default-hours", type=float, default=3.5)
    list_tasks.add_argument("--low-rate", type=float, default=100.0)
    list_tasks.add_argument("--base-rate", type=float, default=150.0)
    list_tasks.add_argument("--high-rate", type=float, default=250.0)
    list_tasks.add_argument("--task-metadata-csv", default="configs/task_metadata.csv")
    list_tasks.set_defaults(func=cmd_list_tasks)

    run = subparsers.add_parser("run", help="Run the evaluation.")
    run.add_argument("--config", required=True)
    run.add_argument("--output-dir", default=None)
    run.add_argument("--limit", type=int, default=None)
    run.add_argument("--task-ids", nargs="*", default=None)
    run.set_defaults(func=cmd_run)

    summarize = subparsers.add_parser("summarize", help="Rebuild summaries from raw JSONL.")
    summarize.add_argument("--run-jsonl", required=True)
    summarize.add_argument("--output-dir", required=True)
    summarize.set_defaults(func=cmd_summarize)

    map_tasks = subparsers.add_parser("map-tasks", help="Export a structural map of tasks for analysis.")
    map_tasks.add_argument("--dataset-dir", required=True)
    map_tasks.add_argument("--output", required=True)
    map_tasks.add_argument("--domain", default=None)
    map_tasks.add_argument("--start-index", type=int, default=0)
    map_tasks.add_argument("--limit", type=int, default=None)
    map_tasks.add_argument("--task-ids", nargs="*", default=None)
    map_tasks.add_argument("--sort-by", default="task_id")
    map_tasks.add_argument("--descending", action="store_true")
    map_tasks.add_argument("--format", choices=["csv", "json", "jsonl"], default=None)
    map_tasks.add_argument("--task-metadata-csv", default="configs/task_metadata.csv")
    map_tasks.set_defaults(func=cmd_map_tasks)

    rebuild_tracker_parser = subparsers.add_parser(
        "rebuild-tracker",
        help="Rebuild the discovered-attempts catalog and promoted master tracker.",
    )
    rebuild_tracker_parser.add_argument("--outputs-root", default="outputs")
    rebuild_tracker_parser.add_argument("--tracker-dir", default="tracker")
    rebuild_tracker_parser.add_argument("--openai-price-book", default="configs/openai_pricing.json")
    rebuild_tracker_parser.add_argument("--task-metadata-csv", default="configs/task_metadata.csv")
    rebuild_tracker_parser.set_defaults(func=cmd_rebuild_tracker)

    promote = subparsers.add_parser(
        "promote-run",
        help="Promote one or more completed attempts into the curated master tracker.",
    )
    promote.add_argument("--output-dir", default=None)
    promote.add_argument("--run-jsonl", default=None)
    promote.add_argument("--task-id", type=int, default=None)
    promote.add_argument("--run-index", type=int, default=None)
    promote.add_argument("--all", action="store_true")
    promote.add_argument("--label", default="")
    promote.add_argument("--notes", default="")
    promote.add_argument("--headline", action="store_true")
    promote.add_argument("--outputs-root", default="outputs")
    promote.add_argument("--tracker-dir", default="tracker")
    promote.add_argument("--openai-price-book", default="configs/openai_pricing.json")
    promote.add_argument("--task-metadata-csv", default="configs/task_metadata.csv")
    promote.set_defaults(func=cmd_promote_run)

    publish_neon = subparsers.add_parser(
        "publish-neon",
        help="Publish promoted tracker data into a Postgres/Neon schema.",
    )
    publish_neon.add_argument("--tracker-dir", default="tracker")
    publish_neon.add_argument("--database-url", default=None)
    publish_neon.add_argument("--schema", default="evals")
    publish_neon.set_defaults(func=cmd_publish_neon)

    smoke_python = subparsers.add_parser(
        "smoke-python-exec",
        help="Run a synthetic smoke test that requires python_exec.",
    )
    smoke_python.add_argument("--config", required=True)
    smoke_python.add_argument("--output-dir", default=None)
    smoke_python.add_argument("--row-count", type=int, default=6000)
    smoke_python.set_defaults(func=cmd_smoke_python_exec)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    code = args.func(args)
    raise SystemExit(code)


if __name__ == "__main__":
    main(sys.argv[1:])
