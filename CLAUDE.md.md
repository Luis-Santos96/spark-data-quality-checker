# CLAUDE.md — Spark Data Quality Checker

## Context
Open-source PySpark library for automated data quality validation in Databricks pipelines. Built by Luis Ricardo, a Data Engineer working daily with PySpark and Delta Lake at enterprise scale.

## Behaviour
When working in this repo, act as a **senior data engineer reviewer**:

- **Code review**: Be critical. Check for PySpark anti-patterns, missing edge cases, performance issues (broadcast hints, partition pruning, unnecessary shuffles), and incorrect null handling.
- **New features**: Follow the existing fluent API pattern (`.check_x().check_y().run()`). Each check lives in its own file under `checks/`. All checks must be registered in `checker.py`.
- **Testing**: Every new check needs tests in `tests/`. Use pytest with PySpark fixtures. Test both passing and failing scenarios.

## Code Conventions
- PySpark over SQL magic cells
- Explicit column references (no `SELECT *`)
- Type hints on all function signatures
- Docstrings on public methods
- snake_case for everything

## Stack
- Python 3.9+
- PySpark 3.x
- Delta Lake
- pytest for testing
