# social-media-propaganda-dataset

This project builds a Vietnamese political comment dataset from social media platforms (Facebook, Threads, TikTok, Reddit, YouTube) for propaganda detection and classification.

## Features

- Custom DOM-based scraping (JS + Python)
- Gemini API-based post summarization  
- Comment labeling with political-context prompts
- 4-step processing pipeline: clean → summarize post → label comments → manual review
- Final 7-step preprocessing pipeline for dataset completion

## Structure

- `scraping/`: Tools to extract comments from each platform
- `preprocessing/`: Data cleaning and text normalization pipeline
- `labeling/`: Post summarization and Gemini-based comment annotation
- `dataset/sample_dataset.csv`: Sample of 100 anonymized rows from v1

## Dataset Versions

- **Version 1**: 17,000+ raw comments → 11,685 cleaned entries (completed)
- **Version 2**: 32,762 raw comments → 25,724 cleaned entries (labeling in progress)

## Preprocessing Pipeline (7 Steps)

The preprocessing pipeline follows this specific order for optimal text cleaning:

1. **Unicode Normalization + Lowercase**: Normalize Unicode characters to NFC format and convert to lowercase
2. **Remove Emoji, Links, HTML, Mentions, Hashtags, UI Indicators**: Clean social media specific elements
3. **Reduce Elongated Characters**: Normalize repeated characters (e.g., "helloooo" → "helloo")
4. **Lexical Normalization**: Apply Vietnamese slang and abbreviation dictionary
5. **Remove Punctuation**: Strip all punctuation marks while preserving Vietnamese characters
6. **Whitespace Stripping**: Remove extra spaces and normalize spacing
7. **Deduplication**: Remove duplicate comments based on cleaned text

## Processing Results

- **Input**: 17,651 raw comments
- **Output**: 17,301 cleaned comments (98.0% retention)
- **Duplicate Removal**: 350 comments removed
- **Label Distribution**:
  - KHONG_LIEN_QUAN: 51.4%
  - KHONG_PHAN_DONG: 35.8%
  - PHAN_DONG: 12.8%
