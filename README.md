# social-media-propaganda-dataset

This project builds a Vietnamese political comment dataset from social media platforms (Facebook, Threads, TikTok, Reddit, YouTube) for propaganda detection and classification.

## Features

- Custom DOM-based scraping (JS + Python)
- Gemini API-based post summarization  
- Comment labeling with political-context prompts
- 4-step processing pipeline: clean → summarize post → label comments → manual review
- Final 9-step preprocessing pipeline for dataset completion

## Structure

- `scraping/`: Tools to extract comments from each platform
- `preprocessing/`: Data cleaning and text normalization pipeline
- `labeling/`: Post summarization and Gemini-based comment annotation
- `dataset/sample_dataset.csv`: Sample of 100 anonymized rows from v1

## Dataset Versions

- **Version 1**: 17,000+ raw comments → 11,685 cleaned entries (completed)
- **Version 2**: 32,762 raw comments → 25,724 cleaned entries (labeling in progress)
