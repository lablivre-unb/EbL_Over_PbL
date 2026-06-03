"""
Single source of truth for extraction targets and semester windows.

Used by both the extractor (journal/orchestrator.py) and the filter
(journal/filters/pipeline.py) so the two never diverge.

filter_by_repo:
  True  → only repos listed in `repos` pass the silver-layer filter.
  False → all repos from this org pass (used for EbL/benchmark orgs where
          we extract everything and filter only by semester).
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class Target:
    platform: str  # "github" or "gitlab"
    org: str  # GitHub org login or GitLab group full path
    repos: List[str] = field(default_factory=list)
    since: Optional[str] = None
    filter_by_repo: bool = True

    def __post_init__(self):
        # Normalise platform to lowercase for consistent comparisons
        object.__setattr__(self, "platform", self.platform.lower())


# ---------------------------------------------------------------------------
# Academic targets (EbP/PbL discipline projects)
# ---------------------------------------------------------------------------

ACADEMIC_TARGETS: List[Target] = [
    Target(
        platform="github",
        org="unb-mds",
        repos=[
            "2025-2-Mural-UnB",
            "Sonorus-2025.1",
            "2024-2-AcheiUnB",
            "2024-1-forUnB",
        ],
    ),
    Target(
        platform="github",
        org="mdsreq-fga-unb",
        repos=[
            "REQ-2025.2-T02-RxHospitalar",
            "2025.1-T01-VidracariaModelo",
            "2024.2-T03-CafeDoSitio",
            "2024.1-ObjeX",
        ],
    ),
    Target(
        platform="github",
        org="fga-eps-mds",
        repos=[
            "2025.2-Valhalla",
            "2025.2-Valhalla-Docs",
            "2025.1-VaiPelaSombra-docs",
            "2025.1-VaiPelaSombra-FrontEnd",
            "2025.1-VaiPelaSombra-BackEnd",  # fixed: trailing comma removed (was "...BackEnd,")
            "2025.1-VaiPelaSombra-API",
            "2024-2-GEROcuidado-Docs",
            "2024-2-GEROcuidado-APIForum",
            "2024-2-GEROcuidado-Front",
            "2024-2-GEROcuidado-APIUsuario",
            "2024-2-GEROcuidado-APISaude",
            "2024-1-GEROcuidado-Front",
            "2024-1-GEROcuidado-Doc",
            "2024-1-GEROcuidado-APISaude",
            "2024-1-GEROcuidado-APIUsuario",
            "2024-1-GEROcuidado-APIForum",
        ],
    ),
    Target(
        platform="gitlab",
        org="lappis-unb/decidimbr",
        repos=["decidim-govbr"],
        filter_by_repo=True,
    ),
]

# ---------------------------------------------------------------------------
# Benchmark / EbL (market) targets
# ---------------------------------------------------------------------------

BENCHMARK_TARGETS: List[Target] = [
    Target(
        platform="github",
        org="decidim",
        repos=["decidim"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="microsoft",
        repos=["vscode"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="flutter",
        repos=["flutter"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="facebook",
        repos=["react"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="kubernetes",
        repos=["kubernetes"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="tensorflow",
        repos=["tensorflow"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="apache",
        repos=["storm", "phoenix"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="pulp",
        repos=["pulpcore"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
    Target(
        platform="github",
        org="quay",
        repos=["quay"],
        since="2024-01-01T00:00:00Z",
        filter_by_repo=False,
    ),
]

TARGETS: List[Target] = ACADEMIC_TARGETS + BENCHMARK_TARGETS

# ---------------------------------------------------------------------------
# Semester windows (used by the silver-layer filter)
# ---------------------------------------------------------------------------

SEMESTERS: List[dict] = [
    {"name": "2024.1", "start": "2024-03-18 00:00:00", "end": "2024-09-21 23:59:59"},
    {"name": "2024.2", "start": "2024-10-14 00:00:00", "end": "2025-02-22 23:59:59"},
    {"name": "2025.1", "start": "2025-03-24 00:00:00", "end": "2025-07-26 23:59:59"},
    {"name": "2025.2", "start": "2025-08-01 00:00:00", "end": "2025-12-22 23:59:59"},
]

# ---------------------------------------------------------------------------
# Repositories targeted for coverage extraction (journal study)
# ---------------------------------------------------------------------------

COVERAGE_REPOS: List[dict] = [
    {"platform": "github", "org": "apache", "repo": "storm"},
    {"platform": "github", "org": "apache", "repo": "phoenix"},
    {"platform": "github", "org": "pulp", "repo": "pulpcore"},
    {"platform": "github", "org": "quay", "repo": "quay"},
    {"platform": "github", "org": "microsoft", "repo": "vscode", "skip_local": True},
    {"platform": "github", "org": "facebook", "repo": "react"},
    {"platform": "github", "org": "decidim", "repo": "decidim"},
]
