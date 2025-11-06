# Parquet Data Files

This directory contains processed data files in Parquet format. These files are **generated** by running the analysis pipeline and are **not** included in version control due to their large size (exceeding GitHub's 100MB file limit).

## Files Generated

The following files are created when you run the full pipeline:

- `processed_voterfile.parquet` - Processed voter registration data (~269MB)
- `processed_early_voting.parquet` - Processed early voting records (~9MB)
- `early_voting_merged.parquet` - Merged voterfile and early voting data (~300MB)
- `voters_with_party_modeling.parquet` - Voters with modeled party affiliation (~585MB)

## How to Generate

These files are automatically generated when you run:

```bash
uv run python main.py
```

The pipeline will process the raw voterfile and early voting data, create these Parquet files, and use them for subsequent analysis steps.

## Why Not in Git?

- GitHub has a 100MB file size limit
- Parquet files are optimized binary formats that don't compress well
- These are derived/generated files that can be recreated from source data
- Total size would be over 1GB if included

## Alternative Storage

If you need to share these files:
- Use Git LFS (Git Large File Storage)
- Store in cloud storage (S3, Google Drive, etc.)
- Use data versioning tools like DVC
- Share via file transfer services

