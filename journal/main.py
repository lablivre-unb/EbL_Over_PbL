"""
Entry point for the journal extraction pipeline.

Usage
-----
# Extract all configured targets (PR metrics)
python -m journal.main extract

# Extract and then filter to silver layer
python -m journal.main extract --filter

# Run only the silver-layer filter on an existing bronze CSV
python -m journal.main filter

# Override output paths
python -m journal.main extract --output journal/data/bronze/custom.csv

# Extract only specific targets (match by org or repo name, repeatable)
python -m journal.main extract --target lappis-unb/decidimbr

# Re-extract a single target from scratch, overwriting its existing rows
python -m journal.main extract --target decidim-govbr --overwrite

# Dry-run: print targets and exit
python -m journal.main extract --list-targets

# Extract test coverage for all COVERAGE_REPOS
python -m journal.main coverage

# Extract coverage for specific repos
python -m journal.main coverage --repo apache/storm --repo facebook/react

# List coverage target repos without running
python -m journal.main coverage --list-repos
"""
import argparse
import logging
import os
import sys

from journal.config import settings
from journal.config.targets import COVERAGE_REPOS, SEMESTERS, TARGETS
from journal.filters.pipeline import filter_bronze_to_silver
from journal.orchestrator import ExtractionOrchestrator


def _configure_logging(log_file: str) -> None:
    import os
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, mode="a"),
        ],
    )


def _select_targets(patterns):
    """Return TARGETS filtered by --target patterns.

    A pattern matches a target when it is found (case-insensitively) in the
    target's org or in any of its repo names, so both
    ``--target lappis-unb/decidimbr`` and ``--target decidim-govbr`` select the
    Brasil Participativo target. Returns all TARGETS when no pattern is given.
    """
    if not patterns:
        return list(TARGETS)

    selected = []
    for t in TARGETS:
        for p in patterns:
            pl = p.lower()
            if (
                pl in t.org.lower()
                or any(pl in r.lower() for r in t.repos)
            ):
                selected.append(t)
                break
    return selected


def cmd_extract(args: argparse.Namespace) -> None:
    output = args.output or settings.OUTPUT_FILE
    _configure_logging(settings.ERROR_LOG_FILE)
    logger = logging.getLogger("journal.main")

    targets = _select_targets(args.target)

    if args.list_targets:
        for t in targets:
            repos = ", ".join(t.repos) if t.repos else "<all repos>"
            since = f"  since={t.since}" if t.since else ""
            print(f"[{t.platform}] {t.org}  repos=[{repos}]{since}")
        return

    if args.target and not targets:
        logger.error("No targets matched --target %s", args.target)
        sys.exit(1)

    if args.overwrite:
        pairs = [
            (t.org, r) for t in targets for r in (t.repos or ["*"])
        ]
        removed = ExtractionOrchestrator(output_file=output).persistence.remove_records(pairs)
        logger.info("Overwrite: removed %d existing rows before re-extraction", removed)

    logger.info("Starting extraction — output: %s (%d target(s))", output, len(targets))
    orchestrator = ExtractionOrchestrator(output_file=output)
    orchestrator.run(targets=targets)
    logger.info("Extraction complete")

    if args.filter:
        silver = output.replace("/bronze/", "/silver/")
        logger.info("Running filter: %s → %s", output, silver)
        filter_bronze_to_silver(output, silver)


def cmd_filter(args: argparse.Namespace) -> None:
    _configure_logging(settings.ERROR_LOG_FILE)
    logger = logging.getLogger("journal.main")
    bronze = args.input or settings.OUTPUT_FILE
    silver = args.output or bronze.replace("/bronze/", "/silver/")
    logger.info("Filtering: %s → %s", bronze, silver)
    filter_bronze_to_silver(bronze, silver)


def cmd_coverage(args: argparse.Namespace) -> None:
    from journal.extractors.coverage.extractor import CoverageExtractor
    from journal.persistence.coverage_store import CoverageStore

    _configure_logging(settings.ERROR_LOG_FILE)
    logger = logging.getLogger("journal.main")

    repos = COVERAGE_REPOS
    if args.repo:
        repos = []
        for slug in args.repo:
            if "/" not in slug:
                logger.error("--repo must be ORG/REPO, got: %s", slug)
                sys.exit(1)
            platform = "github"
            org, repo = slug.split("/", 1)
            repos.append({"platform": platform, "org": org, "repo": repo})

    if args.list_repos:
        for r in repos:
            print(f"[{r['platform']}] {r['org']}/{r['repo']}")
        return

    output = args.output or "journal/data/bronze/coverage.csv"
    store = CoverageStore(output)
    already_done = store.get_extracted_repos()

    repos_to_run = [
        r for r in repos
        if f"{r['org']}/{r['repo']}" not in already_done
    ]

    if not repos_to_run:
        logger.info("All repos already extracted. Use --force to re-run.")
        return

    extractor = CoverageExtractor(
        github_token=settings.GITHUB_TOKEN,
        gitlab_token=settings.GITLAB_TOKEN,
        codecov_token=os.getenv("CODECOV_TOKEN", ""),
        exec_timeout=args.exec_timeout,
        install_deps=not args.no_install,
    )

    logger.info("Extracting coverage for %d repositories", len(repos_to_run))
    results = extractor.extract_batch(repos_to_run)
    store.save(results)
    logger.info("Coverage extraction complete — results: %s", output)

    for r in results:
        status = f"{r.coverage_percent:.1f}%" if r.coverage_percent is not None else "N/A"
        print(f"  {r.repository:<40} {status:<8} [{r.coverage_source}]")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m journal.main",
        description="Journal PR extraction and filtering pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- extract ---
    p_extract = sub.add_parser("extract", help="Extract PRs from configured targets")
    p_extract.add_argument(
        "--output", metavar="PATH", help="Override bronze CSV output path"
    )
    p_extract.add_argument(
        "--target", metavar="NAME", action="append",
        help="Only extract targets matching NAME (org or repo, repeatable). "
             "Defaults to all TARGETS.",
    )
    p_extract.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete existing rows for the selected targets and re-extract them "
             "from scratch (instead of resuming and skipping processed PRs).",
    )
    p_extract.add_argument(
        "--filter",
        action="store_true",
        help="Run the silver-layer filter after extraction",
    )
    p_extract.add_argument(
        "--list-targets",
        action="store_true",
        help="Print configured targets and exit without extracting",
    )
    p_extract.set_defaults(func=cmd_extract)

    # --- filter ---
    p_filter = sub.add_parser("filter", help="Filter an existing bronze CSV to silver")
    p_filter.add_argument("--input", metavar="PATH", help="Bronze CSV path")
    p_filter.add_argument("--output", metavar="PATH", help="Silver CSV output path")
    p_filter.set_defaults(func=cmd_filter)

    # --- coverage ---
    p_cov = sub.add_parser("coverage", help="Extract test coverage for target repositories")
    p_cov.add_argument(
        "--repo", metavar="ORG/REPO", action="append",
        help="Override repo list (repeatable). Defaults to COVERAGE_REPOS.",
    )
    p_cov.add_argument(
        "--output", metavar="PATH",
        help="Coverage CSV output path (default: journal/data/bronze/coverage.csv)",
    )
    p_cov.add_argument(
        "--list-repos", action="store_true",
        help="Print configured coverage repos and exit",
    )
    p_cov.add_argument(
        "--exec-timeout", type=int, default=600, metavar="SEC",
        help="Timeout in seconds for each test-suite execution (default: 600)",
    )
    p_cov.add_argument(
        "--no-install", action="store_true",
        help="Skip dependency installation before running tests",
    )
    p_cov.set_defaults(func=cmd_coverage)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
