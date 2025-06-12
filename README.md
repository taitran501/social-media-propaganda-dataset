# social-media-propaganda-dataset

This project builds a Vietnamese political comment dataset from social media platforms (Facebook, Threads, TikTok, Reddit) for propaganda detection and classification.

##  Features

-  Custom DOM-based scraping (JS + Python)
-  Gemini API-based post summarization
- Comment labeling with political-context prompts
- 6-step preprocessing pipeline (normalize, deduplicate, teencode fix, etc.)

## Structure

- `scraping/`: Tools to extract comments from each platform
- `preprocessing/`: Data cleaning and text normalization
- `labeling/`: Summarization and Gemini-based annotation
- `dataset/sample_dataset.csv`: Sample of 100 anonymized rows, extracted from a full cleaned set of 11,685 entries (from 17,000+ raw comments)

