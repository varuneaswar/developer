# Code Quality Module

A **plug-and-play** code quality module that enforces Python best practices
(formatting, linting, security, and type-checking) and is designed to be
extended for Java and Angular.

---

## Contents

```
code_quality/
├── README.md                 ← this file
├── python/
│   ├── pyproject.toml        ← black / isort / pylint / mypy settings
│   ├── setup.cfg             ← flake8 settings
│   └── bandit.yaml           ← bandit security scanner config
├── java/
│   └── README.md             ← placeholder / future plan
└── angular/
    └── README.md             ← placeholder / future plan

Root-level files added by this module
.pre-commit-config.yaml       ← pre-commit hooks (black/isort/flake8/bandit + hygiene)
.github/workflows/
└── code_quality_python.yml   ← GitHub Actions CI workflow
requirements.txt              ← pinned dev-tooling dependencies
```

---

## Quick Start (copy into any repo)

### 1. Copy the relevant paths

```bash
# From the root of *this* repo, copy into your target repo:
cp -r code_quality/          <your-repo>/code_quality/
cp .pre-commit-config.yaml   <your-repo>/.pre-commit-config.yaml
cp requirements.txt          <your-repo>/requirements.txt
cp .github/workflows/code_quality_python.yml \
   <your-repo>/.github/workflows/code_quality_python.yml
```

If `pyproject.toml` / `setup.cfg` already exist in your repo's root, merge the
relevant sections from `code_quality/python/pyproject.toml` and
`code_quality/python/setup.cfg` instead of copying them outright.

### 2. Install dev dependencies

```bash
pip install -r requirements.txt
```

### 3. Enable pre-commit hooks

```bash
pre-commit install          # installs the git hook
pre-commit run --all-files  # optional: run once across all existing files
```

From now on the hooks run automatically on every `git commit`.

---

## Running checks locally

| Tool    | Command                                      | Purpose                        |
|---------|----------------------------------------------|--------------------------------|
| black   | `black --check .`                            | Format check (no changes)      |
| isort   | `isort --check-only .`                       | Import-order check             |
| flake8  | `flake8 .`                                   | PEP 8 / style lint             |
| bandit  | `bandit -r . -c code_quality/python/bandit.yaml` | Security scan              |
| pylint  | `pylint <package>`                           | Deep lint (non-blocking in CI) |
| mypy    | `mypy <package>`                             | Type checking (non-blocking)   |

To **auto-fix** formatting locally (never done by CI):

```bash
black .
isort .
```

---

## Changing the Python version in CI

Open `.github/workflows/code_quality_python.yml` and update the single
`PYTHON_VERSION` environment variable near the top of the file:

```yaml
env:
  PYTHON_VERSION: "3.11"   # ← change this line
```

---

## Making pylint / mypy blocking in CI

In `.github/workflows/code_quality_python.yml` find the steps labelled
`Run pylint (non-blocking)` and `Run mypy (non-blocking)`.  
Remove the `|| true` suffix from the `run:` command to make them fail the
workflow on errors.

---

## Roadmap

| Language | Status     | Notes                              |
|----------|------------|------------------------------------|
| Python   | ✅ Done    | black, isort, flake8, bandit, pylint, mypy |
| Java     | 🔜 Planned | Checkstyle, SpotBugs, PMD          |
| Angular  | 🔜 Planned | ESLint, Prettier, stylelint        |

See [`java/README.md`](java/README.md) and [`angular/README.md`](angular/README.md)
for the planned approach.
