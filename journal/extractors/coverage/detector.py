"""Language, build-system, and coverage-provider detection.

Works from a flat list of filenames (as returned by the GitHub API tree endpoint
or a local directory walk), so it can operate both on remote repos and local
clones without extra I/O.
"""
import re
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Language indicators — order within each list is irrelevant; presence matters
# ---------------------------------------------------------------------------
_LANGUAGE_INDICATORS: dict = {
    "python":     ["requirements.txt", "pyproject.toml", "setup.py", "setup.cfg",
                   "Pipfile", "tox.ini"],
    "java":       ["pom.xml", "build.gradle", "build.gradle.kts", "gradlew"],
    "go":         ["go.mod", "go.sum"],
    "javascript": ["package.json"],
    # typescript checked explicitly because it shares package.json with javascript
}

# Source-file extensions used to break indicator-score ties.
# Repos like apache/storm have both pom.xml and Python binding files; counting
# actual source files picks the language that dominates the codebase.
_SOURCE_EXTENSIONS: dict = {
    "python":     frozenset({".py"}),
    "java":       frozenset({".java", ".kt", ".scala"}),
    "go":         frozenset({".go"}),
    "javascript": frozenset({".js", ".jsx", ".mjs"}),
    "typescript": frozenset({".ts", ".tsx"}),
}

# ---------------------------------------------------------------------------
# Build-system indicators per language
# First match wins.
# ---------------------------------------------------------------------------
_BUILD_SYSTEM_INDICATORS: dict = {
    "python": [
        ("pytest",   ["pytest.ini", "conftest.py"]),
        ("unittest", []),   # fallback for any Python project
    ],
    "java": [
        ("maven",    ["pom.xml"]),
        ("gradle",   ["build.gradle", "build.gradle.kts", "gradlew"]),
    ],
    "javascript": [
        ("yarn",     ["yarn.lock"]),
        ("npm",      ["package.json"]),    # also covers typescript
    ],
    "typescript": [
        ("yarn",     ["yarn.lock"]),
        ("npm",      ["package.json"]),
    ],
    "go": [
        ("go",       ["go.mod"]),
    ],
    "ruby": [
        ("bundler",  ["Gemfile"]),
    ],
}

# ---------------------------------------------------------------------------
# Coverage-service badge / reference patterns
# ---------------------------------------------------------------------------
_CODECOV_PATTERNS: List[str] = [
    r"codecov\.io",
    r"img\.shields\.io/codecov",
    r"app\.codecov\.io",
    r"codecov/c/github",
    r"codecov/codecov-action",    # GitHub Actions step: uses: codecov/codecov-action@v3
]

_COVERALLS_PATTERNS: List[str] = [
    r"coveralls\.io",
    r"img\.shields\.io/coveralls",
]


class LanguageDetector:
    """Detect primary language and build system from a list of repository file paths."""

    def detect(self, file_list: List[str]) -> Tuple[Optional[str], Optional[str]]:
        """Return (language, build_system).

        TypeScript is identified when tsconfig.json coexists with package.json.
        Otherwise the language with the most indicator files wins; source-file
        extension counts break ties so that mixed-language repos (e.g. a Java
        project with Python scripting) resolve to the dominant language.
        """
        file_names = {Path(f).name for f in file_list}

        # TypeScript must be checked before JavaScript (both use package.json)
        if "tsconfig.json" in file_names and "package.json" in file_names:
            language = "typescript"
        else:
            language = self._score_languages(file_names, file_list)

        if language is None:
            return None, None

        build_system = self._detect_build_system(language, file_names)
        return language, build_system

    # ------------------------------------------------------------------
    def _score_languages(self, file_names: set, file_list: List[str]) -> Optional[str]:
        indicator_scores: dict = {}
        for lang, indicators in _LANGUAGE_INDICATORS.items():
            score = sum(1 for ind in indicators if ind in file_names)
            if score > 0:
                indicator_scores[lang] = score

        if not indicator_scores:
            return None

        # Count source files per detected language to break indicator ties.
        source_counts: dict = {
            lang: sum(
                1 for f in file_list
                if Path(f).suffix.lower() in _SOURCE_EXTENSIONS.get(lang, frozenset())
            )
            for lang in indicator_scores
        }

        return max(indicator_scores, key=lambda l: (indicator_scores[l], source_counts[l]))

    def _detect_build_system(self, language: str, file_names: set) -> Optional[str]:
        for build_sys, indicators in _BUILD_SYSTEM_INDICATORS.get(language, []):
            if not indicators or any(ind in file_names for ind in indicators):
                return build_sys
        return None


class ProviderDetector:
    """Detect coverage-service usage from README and CI configuration text."""

    def detect(self, *texts: str) -> List[str]:
        """Return a list of provider names detected in any of the provided texts.

        Order: ["codecov", "coveralls"] — either, both, or neither.
        """
        combined = " ".join(t for t in texts if t)
        found = []
        if any(re.search(p, combined, re.IGNORECASE) for p in _CODECOV_PATTERNS):
            found.append("codecov")
        if any(re.search(p, combined, re.IGNORECASE) for p in _COVERALLS_PATTERNS):
            found.append("coveralls")
        return found
