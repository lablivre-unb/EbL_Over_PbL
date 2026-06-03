"""Coverage report parsers.

Each parser accepts string content (not a file path) so it can be called with
content fetched remotely via the GitHub API or read from a local clone.

Supported formats
-----------------
  parse_pytest_json    — pytest-cov --cov-report=json  (coverage.json)
  parse_lcov           — LCOV geninfo format            (lcov.info)
  parse_jacoco_xml     — JaCoCo XML report              (jacoco.xml)
  parse_cobertura_xml  — Cobertura / coverage.py XML    (coverage.xml, cobertura.xml)
  parse_go_cover_func  — go tool cover -func output     (stdout text)
  parse_go_coverprofile — go test -coverprofile output  (coverage.out)
"""
import json
import xml.etree.ElementTree as ET
from typing import Optional

from journal.extractors.coverage.models import CoverageMetrics


# ---------------------------------------------------------------------------
# pytest-cov JSON
# ---------------------------------------------------------------------------

def parse_pytest_json(content: str) -> CoverageMetrics:
    """Parse coverage.json produced by: pytest --cov=. --cov-report=json"""
    data = json.loads(content)
    totals = data.get("totals") or {}

    pct = totals.get("percent_covered")
    covered = totals.get("covered_lines")
    num_stmts = totals.get("num_statements")
    missing = totals.get("missing_lines")
    branches_covered = totals.get("covered_branches")
    num_branches = totals.get("num_branches")

    lines_missed = (
        (num_stmts - covered)
        if (num_stmts is not None and covered is not None)
        else missing
    )
    branches_missed = (
        (num_branches - branches_covered)
        if (num_branches is not None and branches_covered is not None)
        else None
    )

    return CoverageMetrics(
        coverage_percent=round(float(pct), 2) if pct is not None else None,
        lines_covered=int(covered) if covered is not None else None,
        lines_missed=int(lines_missed) if lines_missed is not None else None,
        branches_covered=int(branches_covered) if branches_covered is not None else None,
        branches_missed=int(branches_missed) if branches_missed is not None else None,
    )


# ---------------------------------------------------------------------------
# LCOV
# ---------------------------------------------------------------------------

def parse_lcov(content: str) -> CoverageMetrics:
    """Parse an LCOV .info file by summing LH/LF/BRH/BRF across all records."""
    total_lh = 0   # lines hit (covered)
    total_lf = 0   # lines found (total)
    total_brh = 0  # branches hit
    total_brf = 0  # branches found

    for raw in content.splitlines():
        line = raw.strip()
        if line.startswith("LH:"):
            total_lh += int(line[3:])
        elif line.startswith("LF:"):
            total_lf += int(line[3:])
        elif line.startswith("BRH:"):
            total_brh += int(line[4:])
        elif line.startswith("BRF:"):
            total_brf += int(line[4:])

    pct = (total_lh / total_lf * 100.0) if total_lf > 0 else None

    return CoverageMetrics(
        coverage_percent=round(pct, 2) if pct is not None else None,
        lines_covered=total_lh if total_lf > 0 else None,
        lines_missed=(total_lf - total_lh) if total_lf > 0 else None,
        branches_covered=total_brh if total_brf > 0 else None,
        branches_missed=(total_brf - total_brh) if total_brf > 0 else None,
    )


# ---------------------------------------------------------------------------
# JaCoCo XML
# ---------------------------------------------------------------------------

def parse_jacoco_xml(content: str) -> CoverageMetrics:
    """Parse a JaCoCo XML report; reads top-level <counter> elements only."""
    root = ET.fromstring(content)

    line_ctr = branch_ctr = None
    for counter in root.findall("counter"):
        t = counter.get("type", "").upper()
        if t == "LINE":
            line_ctr = counter
        elif t == "BRANCH":
            branch_ctr = counter

    lines_covered = int(line_ctr.get("covered", 0)) if line_ctr is not None else None
    lines_missed = int(line_ctr.get("missed", 0)) if line_ctr is not None else None
    branches_covered = (
        int(branch_ctr.get("covered", 0)) if branch_ctr is not None else None
    )
    branches_missed = (
        int(branch_ctr.get("missed", 0)) if branch_ctr is not None else None
    )

    if lines_covered is not None and lines_missed is not None:
        total = lines_covered + lines_missed
        pct = (lines_covered / total * 100.0) if total > 0 else 0.0
    else:
        pct = None

    return CoverageMetrics(
        coverage_percent=round(pct, 2) if pct is not None else None,
        lines_covered=lines_covered,
        lines_missed=lines_missed,
        branches_covered=branches_covered,
        branches_missed=branches_missed,
    )


# ---------------------------------------------------------------------------
# Cobertura / coverage.py XML
# ---------------------------------------------------------------------------

def parse_cobertura_xml(content: str) -> CoverageMetrics:
    """Parse a Cobertura-format XML (also produced by coverage.py --format=xml)."""
    root = ET.fromstring(content)

    cov = root if root.tag == "coverage" else root.find(".//coverage")
    if cov is None:
        raise ValueError("No <coverage> element found in Cobertura XML")

    line_rate = cov.get("line-rate")
    lines_valid = cov.get("lines-valid")
    lines_covered_attr = cov.get("lines-covered")
    branches_valid = cov.get("branches-valid")
    branches_covered_attr = cov.get("branches-covered")

    pct = float(line_rate) * 100.0 if line_rate else None
    lv = int(lines_valid) if lines_valid else None
    lc = int(lines_covered_attr) if lines_covered_attr else None
    lm = (lv - lc) if (lv is not None and lc is not None) else None
    bv = int(branches_valid) if branches_valid else None
    bc = int(branches_covered_attr) if branches_covered_attr else None
    bm = (bv - bc) if (bv is not None and bc is not None) else None

    return CoverageMetrics(
        coverage_percent=round(pct, 2) if pct is not None else None,
        lines_covered=lc,
        lines_missed=lm,
        branches_covered=bc,
        branches_missed=bm,
    )


# ---------------------------------------------------------------------------
# Go coverage
# ---------------------------------------------------------------------------

def parse_go_cover_func(output: str) -> CoverageMetrics:
    """Parse the stdout of: go tool cover -func=coverage.out

    Looks for the summary line:  total:   (statements)   87.3%
    """
    for raw in reversed(output.splitlines()):
        line = raw.strip()
        if line.lower().startswith("total:"):
            parts = line.split()
            if parts and parts[-1].endswith("%"):
                try:
                    pct = float(parts[-1][:-1])
                    return CoverageMetrics(coverage_percent=round(pct, 2))
                except ValueError:
                    pass
    return CoverageMetrics(coverage_percent=None)


def parse_go_coverprofile(content: str) -> CoverageMetrics:
    """Parse a coverage.out file produced by: go test ./... -coverprofile=coverage.out

    Each data line format: file:start_line.col,end_line.col  num_statements  count
    count > 0 means covered.
    """
    covered = 0
    total = 0

    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("mode:"):
            continue
        parts = line.rsplit(" ", 2)
        if len(parts) != 3:
            continue
        try:
            num_stmts = int(parts[1])
            count = int(parts[2])
            total += num_stmts
            if count > 0:
                covered += num_stmts
        except ValueError:
            continue

    if total == 0:
        return CoverageMetrics(coverage_percent=None)

    return CoverageMetrics(
        coverage_percent=round(covered / total * 100.0, 2),
        lines_covered=covered,
        lines_missed=total - covered,
        branches_covered=None,
        branches_missed=None,
    )


# ---------------------------------------------------------------------------
# Registry: file-name → parser function
# ---------------------------------------------------------------------------

PARSERS: dict = {
    "coverage.json":                      parse_pytest_json,
    "coverage.xml":                       parse_cobertura_xml,
    "cobertura.xml":                      parse_cobertura_xml,
    "lcov.info":                          parse_lcov,
    "jacoco.xml":                         parse_jacoco_xml,
    "coverage/lcov.info":                 parse_lcov,
    "coverage/coverage-summary.json":     parse_lcov,   # jest summary — approximate
    "target/site/jacoco/jacoco.xml":      parse_jacoco_xml,
    "build/reports/jacoco/test/jacocoTestReport.xml": parse_jacoco_xml,
}
