default:
    @just --list

# View/edit DuckDB database with Harlequin CLI (optional db_name)
duck db_name="":
    harlequin --theme github-dark {{db_name}}

pre-commit_update:
    pre-commit autoupdate

pre-commit_test:
    pre-commit run --all-files
