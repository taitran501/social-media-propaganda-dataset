import itertools
import time
import re
import os
import pandas as pd
import argparse
import sys
from pathlib import Path
import google.generativeai as genai
from tqdm import tqdm
import math
from datetime import datetime
import json

# Điều chỉnh đường dẫn import
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Thêm thư mục cha vào path để import config
from utils.file_utils import save_excel_file
import config

def parse_args():
    parser = argparse.ArgumentParser(description='Label data using Gemini API')
    parser.add_argument('--version', '-v', help='Version to process (e.g., v1, v2)')
    parser.add_argument('--input', '-i', help='Input file name (default: pre_labeled.xlsx)')
    parser.add_argument('--output', '-o', help='Output file name (default: gemini_labeled.xlsx)')
    parser.add_argument('--model', '-m', default=None,
                        help='Model to use (if not specified, will prompt for selection)')
    parser.add_argument('--auto', '-a', action='store_true',
                        help='Run in full automation mode (no prompts)')
    return parser.parse_args()

# ---- Rate Limit Management ----
class RateLimitManager:
    """Manages API key rotation and rate limits for Gemini API"""
    
    def __init__(self, api_keys, model_name="gemini-1.5-flash"):
        self.api_keys = api_keys
        self.key_index = 0
        self.model_name = model_name
        
        # Current rate limits based on Google AI Studio (Free Tier)
        self.limits = {
            # Gemini 2.5 Series
            "gemini-2.5-pro": {"rpm": 5, "rpd": 100, "tpm": 250000},
            "gemini-2.5-flash": {"rpm": 10, "rpd": 250, "tpm": 250000},
            "gemini-2.5-flash-lite-preview-06-17": {"rpm": 15, "rpd": 1000, "tpm": 250000},
            "gemini-2.5-flash-preview-tts": {"rpm": 3, "rpd": 15, "tpm": 10000},
            "gemini-2.5-pro-preview-tts": {"rpm": 5, "rpd": 100, "tpm": 250000},
            
            # Gemini 2.0 Series (UPDATED with correct rates)
            "gemini-2.0-flash": {"rpm": 15, "rpd": 200, "tpm": 1000000},
            "gemini-2.0-flash-preview-image-generation": {"rpm": 10, "rpd": 100, "tpm": 200000},
            "gemini-2.0-flash-lite": {"rpm": 30, "rpd": 200, "tpm": 1000000},
        }
        
        # Default to gemini-2.5-flash limits if model not found
        self.current_limits = self.limits.get(model_name, self.limits["gemini-2.5-flash"])
        
        # Track usage per key
        self.usage = {key: {
            "rpm_count": 0,
            "rpd_count": 0,
            "last_minute": datetime.now().minute,
            "last_day": datetime.now().day
        } for key in api_keys}
        
        # Set initial key
        self.set_current_key()
        
    def set_current_key(self):
        """Configure the current API key"""
        current_key = self.api_keys[self.key_index]
        genai.configure(api_key=current_key)
        return current_key
    
    def rotate_key(self):
        """Rotate to the next available API key"""
        self.key_index = (self.key_index + 1) % len(self.api_keys)
        current_key = self.set_current_key()
        print(f"  → Rotating to API key: ...{current_key[-4:]}")
        return current_key
    
    def reset_counters_if_needed(self, key):
        """Reset counters if minute/day has changed"""
        now = datetime.now()
        key_usage = self.usage[key]
        
        # Reset minute counter if minute changed
        if now.minute != key_usage["last_minute"]:
            key_usage["rpm_count"] = 0
            key_usage["last_minute"] = now.minute
            
        # Reset day counter if day changed
        if now.day != key_usage["last_day"]:
            key_usage["rpd_count"] = 0
            key_usage["last_day"] = now.day
    
    def check_limits(self, key):
        """Check if current key exceeds any limits"""
        self.reset_counters_if_needed(key)
        key_usage = self.usage[key]
        
        if key_usage["rpm_count"] >= self.current_limits["rpm"]:
            return False, "RPM limit exceeded"
        
        if key_usage["rpd_count"] >= self.current_limits["rpd"]:
            return False, "RPD limit exceeded"
            
        return True, "OK"
    
    def record_usage(self, key):
        """Record a request against the current key"""
        key_usage = self.usage[key]
        key_usage["rpm_count"] += 1
        key_usage["rpd_count"] += 1
    
    def get_available_key(self):
        """Find an available key that hasn't exceeded limits"""
        start_index = self.key_index
        
        while True:
            current_key = self.api_keys[self.key_index]
            available, reason = self.check_limits(current_key)
            
            if available:
                return current_key
                
            # If not available, try next key
            self.key_index = (self.key_index + 1) % len(self.api_keys)
            
            # If we've checked all keys and come back to start, none are available
            if self.key_index == start_index:
                return None
                
        return None
    
    def wait_for_available_key(self):
        """Wait until an API key becomes available"""
        key = self.get_available_key()
        
        if key:
            # Key available now
            self.set_current_key()
            return key
            
        # All keys at rate limit, wait for next minute
        wait_seconds = 65 - datetime.now().second
        print(f"  ⏱️ All keys at rate limit. Waiting {wait_seconds}s for reset...")
        time.sleep(wait_seconds)
        
        # Reset minute counters after waiting
        for key in self.api_keys:
            self.usage[key]["rpm_count"] = 0
            self.usage[key]["last_minute"] = datetime.now().minute
            
        # Try again
        return self.get_available_key()

# ---- API Keys ----
# Import API keys from centralized config
from config import get_api_keys
API_KEYS = get_api_keys()

# Initialize rate limit manager
rate_manager = RateLimitManager(API_KEYS)

# ---- System Instruction & Prompts ----
SYSTEM_INSTRUCTION = """You are an expert in Vietnamese social-media content moderation and political sentiment analysis.

Your task is to classify Vietnamese comments into three categories based on their political stance:

PHAN_DONG: Comments expressing anti-government, anti-communist, or pro-reactionary views
- Criticizes current Vietnamese government/party/socialist system
- Uses derogatory terms for the government: "việt cộng", "đảng cướp", "cs thối", "độc tài"
- Supports opposing political forces: "VNCH", "dmcs", reactionary groups
- Spreads false information about Vietnamese history or achievements

KHONG_PHAN_DONG: Comments expressing pro-government or anti-reactionary views  
- Supports Vietnamese government/party/socialist system
- Defends Vietnamese revolutionary history and achievements
- Criticizes reactionary/anti-government forces
- Uses terms like "phản động", "cali", "vệ quốc", "ba que" to criticize opposition

KHONG_LIEN_QUAN: Comments unrelated to politics
- Sports, entertainment, daily life discussions
- Spam, advertisements, personal disputes
- No political content whatsoever

IMPORTANT RULES:
- Consider the article context when analyzing comments
- When a comment contains both pro and anti-government elements, prioritize the stronger sentiment
- Respond ONLY with a JSON object containing comment classifications
- Use Vietnamese political context and terminology for accurate classification"""

def compress_text(text: str, limit_chars=400) -> str:
    """Simple text compression for API efficiency"""
    if not isinstance(text, str) or len(text) <= limit_chars:
        return str(text) if text else ""
    return text[:limit_chars] + "..."

def ensure_output_dir(version):
    """Ensure output directory exists"""
    paths = config.get_version_paths(version)
    paths['output_dir'].mkdir(parents=True, exist_ok=True)
    return paths['output_dir']

def generate_model_specific_filename(base_filename, model_name, timestamp=False):
    """Generate model-specific filename to avoid overwriting previous results"""
    # Extract base name and extension
    if base_filename.endswith('.xlsx'):
        base_name = base_filename[:-5]
        extension = '.xlsx'
    else:
        base_name = base_filename
        extension = '.xlsx'
    
    # Clean model name for filename
    clean_model = model_name.replace('-', '_').replace('.', '_')
    
    # Add timestamp if requested
    if timestamp:
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{base_name}_{clean_model}_{timestamp_str}{extension}"
    else:
        return f"{base_name}_{clean_model}{extension}"

def check_file_overwrite(file_path, model_name):
    """Check if file exists and ask user for overwrite confirmation"""
    if not os.path.exists(file_path):
        return True, file_path
    
    print(f"\n⚠️  File already exists: {file_path}")
    print("Options:")
    print("  1. Overwrite existing file")
    print("  2. Create new file with model name")
    print("  3. Create new file with model name + timestamp")
    print("  4. Cancel")
    
    while True:
        choice = input("Choose option (1-4): ").strip()
        
        if choice == "1":
            return True, file_path
        elif choice == "2":
            base_filename = os.path.basename(file_path)
            new_filename = generate_model_specific_filename(base_filename, model_name, timestamp=False)
            new_path = os.path.join(os.path.dirname(file_path), new_filename)
            return True, new_path
        elif choice == "3":
            base_filename = os.path.basename(file_path)
            new_filename = generate_model_specific_filename(base_filename, model_name, timestamp=True)
            new_path = os.path.join(os.path.dirname(file_path), new_filename)
            return True, new_path
        elif choice == "4":
            return False, None
        else:
            print("Invalid choice. Please enter 1-4.")

def compare_model_results(version, file1_name, file2_name, output_comparison=True):
    """Compare labeling results between two different model outputs"""
    print(f"\n📊 COMPARING MODEL RESULTS")
    print("=" * 50)
    
    # Get file paths
    output_dir = config.get_path(version, "output")
    file1_path = output_dir / file1_name
    file2_path = output_dir / file2_name
    
    # Check if files exist
    if not os.path.exists(file1_path):
        print(f"❌ File 1 not found: {file1_path}")
        return None
    
    if not os.path.exists(file2_path):
        print(f"❌ File 2 not found: {file2_path}")
        return None
    
    # Load datasets
    print(f"Loading {file1_name}...")
    df1 = pd.read_excel(file1_path)
    print(f"Loading {file2_name}...")
    df2 = pd.read_excel(file2_path)
    
    if len(df1) != len(df2):
        print(f"⚠️  Datasets have different sizes: {len(df1)} vs {len(df2)}")
        return None
    
    # Compare labels
    print(f"\nComparing {len(df1)} records...")
    
    # Ensure both have comment_raw and label columns
    if 'comment_raw' not in df1.columns or 'label' not in df1.columns:
        print("❌ File 1 missing required columns")
        return None
    
    if 'comment_raw' not in df2.columns or 'label' not in df2.columns:
        print("❌ File 2 missing required columns")
        return None
    
    # Compare labels
    matches = 0
    differences = []
    
    for i in range(len(df1)):
        label1 = df1.iloc[i]['label']
        label2 = df2.iloc[i]['label']
        comment = str(df1.iloc[i]['comment_raw'])[:100]
        
        if label1 == label2:
            matches += 1
        else:
            differences.append({
                'index': i,
                'comment': comment,
                'model1_label': label1,
                'model2_label': label2
            })
    
    # Statistics
    accuracy = matches / len(df1) * 100
    print(f"\n📈 COMPARISON RESULTS:")
    print(f"  - Total records: {len(df1)}")
    print(f"  - Matching labels: {matches} ({accuracy:.1f}%)")
    print(f"  - Different labels: {len(differences)} ({100-accuracy:.1f}%)")
    
    # Label distribution comparison
    print(f"\n📋 LABEL DISTRIBUTION:")
    
    # File 1 distribution
    dist1 = df1['label'].value_counts()
    print(f"\n{file1_name}:")
    for label, count in dist1.items():
        print(f"  - {label}: {count} ({count/len(df1)*100:.1f}%)")
    
    # File 2 distribution
    dist2 = df2['label'].value_counts()
    print(f"\n{file2_name}:")
    for label, count in dist2.items():
        print(f"  - {label}: {count} ({count/len(df2)*100:.1f}%)")
    
    # Show sample differences
    if differences:
        print(f"\n🔍 SAMPLE DIFFERENCES (first 10):")
        for diff in differences[:10]:
            print(f"  {diff['index']+1}. {diff['comment']}...")
            print(f"     Model 1: {diff['model1_label']} | Model 2: {diff['model2_label']}")
    
    # Save comparison report if requested
    if output_comparison and differences:
        comparison_filename = f"comparison_{file1_name.replace('.xlsx', '')}_{file2_name.replace('.xlsx', '')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        comparison_path = output_dir / comparison_filename
        
        # Create comparison dataframe
        comparison_data = []
        for diff in differences:
            comparison_data.append({
                'index': diff['index'],
                'comment_text': diff['comment'],
                'model1_label': diff['model1_label'],
                'model2_label': diff['model2_label'],
                'comment_full': str(df1.iloc[diff['index']]['comment_raw'])
            })
        
        comparison_df = pd.DataFrame(comparison_data)
        comparison_df.to_excel(comparison_path, index=False)
        print(f"\n💾 Comparison report saved: {comparison_path}")
    
    return {
        'total_records': len(df1),
        'matches': matches,
        'differences': len(differences),
        'accuracy': accuracy,
        'distribution1': dist1.to_dict(),
        'distribution2': dist2.to_dict(),
        'sample_differences': differences[:20]  # Return first 20 for further analysis
    }

# ---- Main Processing Functions ----
def label_comments_batch(batch_df, summary="", max_retry=3):
    """Label a batch of comments using JSON response format"""
    
    # Prepare comments for batch processing
    comments_data = {}
    for idx, row in batch_df.iterrows():
        comment_text = str(row.get('comment_raw', '')).strip()
        if comment_text:
            comments_data[str(idx)] = compress_text(comment_text, 300)
    
    if not comments_data:
        return {}
    
    # Compress summary
    summary_short = compress_text(summary, 200) if summary else "Không có tóm tắt"
    
    # Create optimized prompt
    prompt = f"""
ARTICLE SUMMARY: {summary_short}

COMMENTS TO CLASSIFY:
{json.dumps(comments_data, ensure_ascii=False, indent=2)}

Classify each comment and respond with JSON format:
{{
  "comment_id": "LABEL",
  "comment_id": "LABEL",
  ...
}}

Valid labels: PHAN_DONG, KHONG_PHAN_DONG, KHONG_LIEN_QUAN
"""
    
    for attempt in range(max_retry):
        try:
            # Get available API key respecting rate limits
            current_key = rate_manager.wait_for_available_key()
            if not current_key:
                print("  ❌ No API keys available. All at daily limit.")
                return {}
            
            # Create model with system instruction
            model = genai.GenerativeModel(
                rate_manager.model_name,
                system_instruction=SYSTEM_INSTRUCTION
            )
            
            # Make API request
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0,
                    response_mime_type="application/json"
                )
            )
            
            # Record usage
            rate_manager.record_usage(current_key)
            
            # Parse JSON response
            try:
                labels_dict = json.loads(response.text)
                print(f"  → Labeled {len(labels_dict)} comments")
                return labels_dict
            except json.JSONDecodeError as e:
                print(f"  ⚠️ JSON parse error: {e}")
                return {}
                
        except Exception as e:
            print(f"  ❌ Error labeling comments (attempt {attempt+1}): {e}")
            
            # Check if it's a quota or rate limit error
            error_str = str(e).lower()
            if "429" in error_str or "quota" in error_str or "rate" in error_str:
                rate_manager.rotate_key()
                
            time.sleep(2)
    
    return {}

def parse_json_labels(labels_dict, batch_df):
    """Parse JSON labels and apply to dataframe indices"""
    labels = {}
    valid_labels = {'PHAN_DONG', 'KHONG_PHAN_DONG', 'KHONG_LIEN_QUAN'}
    
    # Map string IDs back to integer indices
    for str_idx, label in labels_dict.items():
        try:
            idx = int(str_idx)
            if label in valid_labels and idx in batch_df.index:
                labels[idx] = label
        except (ValueError, TypeError):
            continue
    
    # Fill missing labels with default
    for idx in batch_df.index:
        if idx not in labels:
            labels[idx] = "KHONG_LIEN_QUAN"
    
    return labels

def apply_regex_overrides(labels, batch_df):
    """Apply regex-based political keyword overrides"""
    
    # Fixed ANTI_GOVT_PATTERNS
    ANTI_GOVT_PATTERNS = [
        # Original patterns (longer first)
        r'\b(việt\s*cộng|đảng\s*cướp|độc\s*tài|csvn|xứ\s*vẹm|cộng\s*phỉ|đbrr)\b',
        # Phục quốc
        r'\b(phục\s*quốc)\b',
        # phụt quốc
        r'\b(phụt\s*quốc)\b',
        # Enhanced patterns from JSON map (longer first)
        r'\b(vịt\s*cộng|vịt\s*cọng|bò\s*dát\s*vàng|red\s*bull|cộng\s*sản\s*thổ\s*phỉ)\b',
        r'\b(cộng\s*sả|cọng\s*sả|cạn\s*sổng|cơm\s*sườn|cộng\s*nô|súc\s*nô)\b',
        r'\b(béc\s*hù|hochominh|csthophi|đacosa)\b',
        
        # Escaped special characters
        r'\b(v\+|việt\+|viet\+|vịt\s*\+)\b',
        r'\b(bò\s*đỏ|bo\s*do|redbull)\b',
        
        # Short patterns last (more specific context)
        r'\b(cs|béc|đẻng|đẽng)\b'
    ]

    # Fixed ANTI_REACTIONARY_PATTERNS  
    ANTI_REACTIONARY_PATTERNS = [
        # Original patterns (longer first)
        r'\b(ba\s*que|3\s*que|phản\s*động)\b',
        r'\b(cali)\b.*\b(phản\s*quốc|bán\s*nước)\b',
        
        # Enhanced patterns (longer first)
        r'\b(3\s*\/\/\/|phổng\s*đạn|bắc\s*kầy|băc\s*kì|bac\s*kì|bac\s*ki|parkầy)\b',
        r'\b(ba\s*kẻ|3\s*gạch|3\s*xẹt|parque|parwe|bac\s*ky)\b',
        r'\b(backy|parky|bakye|bakey|parkey|parke)\b',
        r'\b(barqe|bakue|3soc|becgie|bẹc\s*giê)\b',
        r'\b(ka\s*li|calo|calu|fandong)\b',
        
        # Short patterns last
        r'\b(3q|3que|\/\/\/|bake|parq|baq|kali|cal|ali)\b'
    ]
    
    updated = 0
    for idx, row in batch_df.iterrows():
        text = str(row.get('comment_raw', '')).lower()
        current_label = labels.get(idx, "KHONG_LIEN_QUAN")
        
        # Check anti-government patterns
        is_anti_govt = any(re.search(pattern, text) for pattern in ANTI_GOVT_PATTERNS)
        
        # Check anti-reactionary patterns  
        is_anti_reactionary = any(re.search(pattern, text) for pattern in ANTI_REACTIONARY_PATTERNS)
        
        # Apply overrides
        if is_anti_govt and current_label != "PHAN_DONG":
            labels[idx] = "PHAN_DONG"
            updated += 1
        elif is_anti_reactionary and not is_anti_govt and current_label != "KHONG_PHAN_DONG":
            labels[idx] = "KHONG_PHAN_DONG"
            updated += 1
    
    if updated > 0:
        print(f"  → Applied {updated} regex overrides")
    
    return labels

def post_process_dai_mentions(labels, batch_df):
    """Post-process comments with 'đài' to reduce false positives"""
    
    updated = 0
    for idx, row in batch_df.iterrows():
        if labels.get(idx) != "PHAN_DONG":
            continue
            
        text = str(row.get('comment_raw', '')).lower()
        
        # Only process if contains "đài" but no strong political keywords
        if 'đài' in text and not any(keyword in text for keyword in [
            'việt cộng', 'đảng cướp', 'csvn', 'cộng sản', 'độc tài', 'bò đỏ', 
            'phục quốc', 'phản động', 'ba que', '3 que', 'bán nước'
        ]):
            labels[idx] = "KHONG_LIEN_QUAN"
            updated += 1
    
    if updated > 0:
        print(f"  → Corrected {updated} 'đài' mentions from PHAN_DONG to KHONG_LIEN_QUAN")
    
    return labels

def run_optimized_labeling(df, version, input_file, output_file, model_name):
    """Optimized labeling pipeline with JSON responses"""
    # Update rate manager model
    rate_manager.model_name = model_name
    
    # Ensure output directory exists
    output_dir = ensure_output_dir(version)
    initial_output_path = config.get_path(version, "output", filename=output_file)
    
    # Check for file overwrite with user choice
    proceed, final_output_path = check_file_overwrite(initial_output_path, model_name)
    if not proceed:
        print("❌ Labeling cancelled by user.")
        return None
    
    output_path = Path(final_output_path)
    
    print(f"Version: {version}")
    print(f"Model: {model_name}")
    print(f"Output file: {output_path}")
    print(f"Processing {len(df)} rows with optimized JSON approach")
    
    # Check if label column exists, otherwise add it
    if "label" not in df.columns:
        df["label"] = ""
    
    # Check if summary column exists
    has_summary = "summary" in df.columns
    print(f"Summary column {'found' if has_summary else 'not found'} in input file")
    
    # Get unique summaries (treating them as unique articles)
    if has_summary:
        df_with_summaries = df.dropna(subset=["summary"])
        unique_summaries = df_with_summaries["summary"].unique()
        print(f"Found {len(unique_summaries)} unique summaries to process")
        
        # Process each summary group
        for summary_idx, summary_text in enumerate(tqdm(unique_summaries, desc="Processing summaries")):
            # Get all comments for this summary
            summary_comments = df[df["summary"] == summary_text].copy()
            
            # Process comments in batches
            batch_size = 50  # Increased batch size for efficiency
            for start in range(0, len(summary_comments), batch_size):
                batch_df = summary_comments.iloc[start:start+batch_size]
                
                # Label batch with summary context
                labels_dict = label_comments_batch(batch_df, summary_text)
                
                # Parse labels
                labels = parse_json_labels(labels_dict, batch_df)
                
                # Apply regex overrides
                labels = apply_regex_overrides(labels, batch_df)
                
                # Post-process 'đài' mentions
                labels = post_process_dai_mentions(labels, batch_df)

                # Update main dataframe
                for idx, label in labels.items():
                    df.loc[idx, "label"] = label
            
            # Save progress periodically
            if (summary_idx + 1) % 20 == 0 or summary_idx == len(unique_summaries) - 1:
                temp_output = output_path.parent / f"temp_{output_path.name}"
                df.to_excel(temp_output, index=False)
                print(f"  💾 Saved progress ({summary_idx+1}/{len(unique_summaries)} summaries)")
    
    else:
        # Fallback: process all comments without summary context
        print("No summary column found, processing all comments without context")
        batch_size = 50
        total_batches = math.ceil(len(df) / batch_size)
        
        for batch_idx in tqdm(range(total_batches), desc="Processing batches"):
            start = batch_idx * batch_size
            end = min(start + batch_size, len(df))
            batch_df = df.iloc[start:end]
            
            # Label batch without summary
            labels_dict = label_comments_batch(batch_df, "")
            
            # Parse and apply labels
            labels = parse_json_labels(labels_dict, batch_df)
            labels = apply_regex_overrides(labels, batch_df)

           # Post-process 'đài' mentions
            labels = post_process_dai_mentions(labels, batch_df) 

            # Update main dataframe
            for idx, label in labels.items():
                df.loc[idx, "label"] = label
            
            # Save progress periodically
            if (batch_idx + 1) % 50 == 0 or batch_idx == total_batches - 1:
                temp_output = output_path.parent / f"temp_{output_path.name}"
                df.to_excel(temp_output, index=False)
                print(f"  💾 Saved progress ({batch_idx+1}/{total_batches} batches)")
    
    # Final save
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"\n✅ Saved {len(df)} labeled rows to: {output_path}")
    
    # Statistics
    label_counts = df["label"].value_counts()
    print("\nLabel distribution:")
    for label, count in label_counts.items():
        if count > 0:
            print(f"  - {label}: {count} ({count/len(df)*100:.1f}%)")
    
    return df

def demo_optimized_labeling(df, num_items=20):
    """Demo optimized labeling with JSON response"""
    
    print(f"\n==== DEMO OPTIMIZED LABELING ====")
    print(f"Selecting {num_items} random items for demo")
    
    # Sample random items
    if len(df) > num_items:
        sample_df = df.sample(num_items)
    else:
        sample_df = df.copy()
    
    # Get summary if available
    summary_text = ""
    if "summary" in df.columns:
        summaries = sample_df["summary"].dropna().unique()
        if len(summaries) > 0:
            summary_text = summaries[0]
            print(f"Using summary: {summary_text[:100]}...")
    
    print(f"\nSelected {len(sample_df)} comments:")
    for i, (idx, row) in enumerate(sample_df.iterrows()):
        comment = str(row.get('comment_raw', ''))
        print(f"  {i+1}. {comment[:70]}...")
    
    # Label comments
    print("\nLabeling comments...")
    labels_dict = label_comments_batch(sample_df, summary_text)
    labels = parse_json_labels(labels_dict, sample_df)
    labels = apply_regex_overrides(labels, sample_df)
    
    labels = post_process_dai_mentions(labels, sample_df)


    print("\nLabeling results:")
    for idx, row in sample_df.iterrows():
        comment = str(row.get('comment_raw', ''))
        label = labels.get(idx, "UNKNOWN")
        print(f"  {comment[:50]}... → {label}")
    
    # Statistics
    label_counts = {}
    for label in labels.values():
        label_counts[label] = label_counts.get(label, 0) + 1
    
    print(f"\nDemo label distribution:")
    for label, count in label_counts.items():
        percentage = count / len(sample_df) * 100
        print(f"  - {label}: {count} ({percentage:.1f}%)")
    
    return sample_df, labels

def list_available_models():
    """List all available Gemini models"""
    try:
        genai.configure(api_key=API_KEYS[0])
        models = list(genai.list_models())
        
        print("\n📋 Available Gemini models:")
        gemini_models = []
        for model in models:
            if 'gemini' in model.name.lower() and 'generateContent' in model.supported_generation_methods:
                model_name = model.name.split('/')[-1]  # Extract model name
                gemini_models.append(model_name)
                print(f"  - {model_name}")
        
        return gemini_models
    except Exception as e:
        print(f"❌ Failed to list models: {e}")
        return []
def estimate_tokens(text):
    """Estimate tokens (approx 4 chars = 1 token for Vietnamese)"""
    if not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

def get_model_rate_limits(model_name):
    """Get rate limits for a specific model"""
    temp_manager = RateLimitManager(API_KEYS, model_name)
    return temp_manager.current_limits

def compare_models_capacity(df, models_to_compare, batch_size=50):
    """Compare capacity and feasibility of multiple models - SIMPLIFIED"""
    print(f"\n📊 SO SÁNH MODELS (Simplified)")
    print("=" * 60)
    
    total_comments = len(df)
    has_summary = "summary" in df.columns
    
    if has_summary:
        estimated_batches = sum(
            math.ceil(len(df[df["summary"] == s]) / batch_size) 
            for s in df["summary"].dropna().unique()
        )
    else:
        estimated_batches = math.ceil(total_comments / batch_size)
    
    print(f"📋 DATASET INFO:")
    print(f"  - Total comments: {total_comments:,}")
    print(f"  - Estimated batches: {estimated_batches:,}")
    print(f"  - API keys available: {len(API_KEYS)}")
    
    print(f"\n📈 MODELS COMPARISON:")
    print(f"{'Model':<35} {'RPM':<6} {'TPM':<10} {'RPD':<6} {'Time':<10} {'Status':<10}")
    print("-" * 80)
    
    for model_name in models_to_compare:
        limits = get_model_rate_limits(model_name)
        
        # Calculate total capacity with all API keys
        num_keys = len(API_KEYS)
        total_rpm = limits.get("rpm", 15) * num_keys
        total_tpm = limits.get("tpm", 1000000) * num_keys
        total_rpd = limits.get("rpd", 100) * num_keys
        
        # Simple feasibility check
        rpm_feasible = total_rpm >= 10  # Need at least 10 RPM total
        rpd_feasible = estimated_batches <= total_rpd
        tpm_feasible = total_tpm >= 100000  # Need at least 100K TPM total
        
        is_feasible = rpm_feasible and rpd_feasible and tpm_feasible
        
        # Simple time estimate (conservative)
        time_hours = max(estimated_batches / total_rpm / 60, 0.1)
        time_str = f"{time_hours:.1f}h"
        
        status = "✅ YES" if is_feasible else "❌ NO"
        
        print(f"{model_name:<35} {total_rpm:<6} {total_tpm:<10,} {total_rpd:<6} {time_str:<10} {status:<10}")
        
        # Show any issues (simplified)
        if not is_feasible:
            print(f"  Issues: ", end="")
            if not rpd_feasible:
                print(f"RPD limit ({estimated_batches} > {total_rpd})")
            elif not rpm_feasible:
                print(f"RPM too low ({total_rpm})")
            elif not tpm_feasible:
                print(f"TPM too low ({total_tpm:,})")
    
    print(f"\n💡 All models support automatic rate limiting adjustment!")
    print(f"   → Batch size will be auto-adjusted to fit within TPM limits")
    print(f"   → Processing will be throttled to respect RPM limits")
    
    return []

def enhanced_estimate_processing_time(df, model_name, batch_size=50):
    """SIMPLIFIED version with automatic adjustment"""
    rate_manager = RateLimitManager(API_KEYS, model_name)
    limits = rate_manager.current_limits
    
    total_comments = len(df)
    has_summary = "summary" in df.columns
    
    if has_summary:
        estimated_batches = sum(
            math.ceil(len(df[df["summary"] == s]) / batch_size) 
            for s in df["summary"].dropna().unique()
        )
    else:
        estimated_batches = math.ceil(total_comments / batch_size)
    
    # Calculate total capacity
    num_keys = len(API_KEYS)
    total_rpm = limits.get("rpm", 15) * num_keys
    total_tpm = limits.get("tpm", 1000000) * num_keys
    total_rpd = limits.get("rpd", 100) * num_keys
    
    # Auto-adjust batch size if needed
    avg_tokens_per_comment = 35  # Conservative estimate
    base_tokens_per_batch = (avg_tokens_per_comment * batch_size) + 500
    
    # If TPM is tight, reduce effective batch size
    max_batches_per_minute = total_tpm // base_tokens_per_batch
    if max_batches_per_minute < total_rpm:
        # Reduce batch size to fit TPM
        adjusted_batch_size = max(10, (total_tpm // total_rpm) // avg_tokens_per_comment)
        print(f"  → Auto-adjusting batch size from {batch_size} to {adjusted_batch_size} for TPM compliance")
        batch_size = adjusted_batch_size
        # Recalculate batches with new batch size
        if has_summary:
            estimated_batches = sum(
                math.ceil(len(df[df["summary"] == s]) / batch_size) 
                for s in df["summary"].dropna().unique()
            )
        else:
            estimated_batches = math.ceil(total_comments / batch_size)
    
    # More realistic time estimate
    # Use 90% of total capacity for safety margin (code handles exceptions well)
    effective_rpm = int(total_rpm * 0.9)
    estimated_minutes = estimated_batches / effective_rpm
    estimated_hours = estimated_minutes / 60
    
    # Add minimal buffer for processing time
    estimated_hours *= 1.1  # 10% buffer
    
    # Simple feasibility (almost always feasible with adjustment)
    is_feasible = estimated_batches <= total_rpd and total_rpm >= 6
    
    return {
        'total_comments': total_comments,
        'estimated_batches': estimated_batches,
        'adjusted_batch_size': batch_size,
        'total_rpm_capacity': total_rpm,
        'total_tpm_capacity': total_tpm,
        'total_rpd_capacity': total_rpd,
        'effective_rpm': effective_rpm,
        'estimated_minutes': estimated_minutes,
        'estimated_hours': estimated_hours,
        'is_feasible': is_feasible,
        'capacity_issues': [] if is_feasible else [f"RPD limit: {estimated_batches} > {total_rpd}"],
        'recommendations': [] if is_feasible else ["Split processing across multiple days"]
    }

def choose_model_with_comparison(df):
    """SIMPLIFIED model selection"""
    print("\n🤖 CHỌN MODEL CHO LABELING")
    print("-" * 50)
    
    # Get available models
    available_models = list_available_models()
    
    if not available_models:
        print("⚠️ Cannot get models list, using default")
        return "gemini-2.0-flash"
    
    # Show only the 3 main models for selection
    print(f"📋 Recommended models:")
    recommended_models = []
    for model in ["gemini-2.0-flash", "gemini-2.5-flash", "gemini-2.5-pro"]:
        if model in available_models:
            recommended_models.append(model)
    
    if len(recommended_models) >= 2:
        print(f"\n🔍 Quick comparison for your dataset:")
        compare_models_capacity(df, recommended_models)
    
    print(f"\n📝 Choose your model:")
    for i, model in enumerate(recommended_models):
        limits = get_model_rate_limits(model)
        print(f"  {i+1}. {model}")
        print(f"     → {limits['rpm']*len(API_KEYS)} RPM, {limits['tpm']*len(API_KEYS):,} TPM, {limits['rpd']*len(API_KEYS)} RPD (total)")
    
    # Option to see all models
    print(f"  {len(recommended_models)+1}. Show all available models")
    
    while True:
        try:
            choice = input(f"\nChọn model (1-{len(recommended_models)+1}): ").strip()
            model_idx = int(choice) - 1
            
            if 0 <= model_idx < len(recommended_models):
                selected_model = recommended_models[model_idx]
                print(f"✅ Selected: {selected_model}")
                return selected_model
            elif model_idx == len(recommended_models):
                # Show all models
                print(f"\n📋 All models:")
                for i, model in enumerate(available_models):
                    limits = get_model_rate_limits(model)
                    print(f"  {i+1}. {model} (RPM: {limits['rpm']}, TPM: {limits['tpm']:,}, RPD: {limits['rpd']})")
                
                while True:
                    try:
                        choice = input(f"\nChọn model (1-{len(available_models)}): ").strip()
                        model_idx = int(choice) - 1
                        if 0 <= model_idx < len(available_models):
                            selected_model = available_models[model_idx]
                            print(f"✅ Selected: {selected_model}")
                            return selected_model
                        else:
                            print("Invalid choice!")
                    except ValueError:
                        print("Please enter a number!")
            else:
                print("Invalid choice!")
        except ValueError:
            print("Please enter a number!")

def main(version, input_file="pre_labeled.xlsx", output_file="gemini_labeled.xlsx", model_name=None):
    """Main function to run the optimized labeling pipeline"""
    print("OPTIMIZED GEMINI LABELING PIPELINE")
    print("-----------------------------------")
    
    # Use interactive model selection if model_name is not provided
    if not model_name:
        model_name = choose_model_with_comparison(pd.read_excel(config.get_path(version, "output", filename=input_file)))
    
    # Set model for rate manager
    rate_manager.model_name = model_name
    print(f"Using model: {model_name}")
    
    # Mode selection
    print("Choose mode:")
    print("1. Demo mode (test with sample data)")
    print("2. Full dataset labeling")
    print("3. Compare model results")
    mode = input("Choice (1/2/3): ").strip()
    
    # Only load data for modes 1 and 2 (comparison mode loads its own data)
    if mode in ["1", "2"]:
        # Get input file path
        input_path = config.get_path(version, "output", filename=input_file)
        
        if not os.path.exists(input_path):
            print(f"⚠️ File not found: {input_path}")
            
            # List available files
            output_dir = config.get_path(version, "output")
            available_files = list(output_dir.glob("*.xlsx"))
            
            if available_files:
                print("\nAvailable files:")
                for i, file in enumerate(available_files):
                    print(f"  {i+1}. {file.name}")
                    
                try:
                    choice = int(input("Choose file number: ").strip()) - 1
                    if 0 <= choice < len(available_files):
                        input_path = available_files[choice]
                    else:
                        print("Invalid choice")
                        return
                except:
                    print("Invalid input")
                    return
            else:
                print("No Excel files found in output directory")
                return
        
        # Load data
        print(f"\nLoading data from {input_path}...")
        df = pd.read_excel(input_path)
        print(f"Loaded {len(df)} rows with columns: {', '.join(df.columns)}")
        
        # Check required columns
        if "comment_raw" not in df.columns:
            print("⚠️ Required column 'comment_raw' not found!")
            return
    
    if mode == "1":
        # Demo mode
        print(f"\n=== DEMO MODE ===")
        
        # Show dataset info
        total_comments = len(df)
        has_summary = "summary" in df.columns
        
        print(f"Dataset info:")
        print(f"  - Total comments: {total_comments}")
        print(f"  - Summary column: {'Available' if has_summary else 'Not available'}")
        
        # Get demo parameters
        try:
            num_items = input(f"Number of items to test (1-100, default 20): ").strip()
            num_items = int(num_items) if num_items else 20
            num_items = max(1, min(num_items, 100, len(df)))
        except ValueError:
            print("Invalid input, using default: 20 items")
            num_items = 20
        
        print(f"\nDemo configuration:")
        print(f"  - Items to test: {num_items}")
        print(f"  - Model: {model_name}")
        
        proceed = input("\nProceed with demo? (y/n): ").strip().lower()
        if proceed == "y":
            sample_df, labels = demo_optimized_labeling(df, num_items)
            
            # Post-process 'đài' mentions
            labels = post_process_dai_mentions(labels, sample_df)
            
            # Display results with corrected 'đài' mentions
            print("\nFinal labeling results (after post-processing):")
            for idx, row in sample_df.iterrows():
                comment = str(row.get('comment_raw', ''))
                label = labels.get(idx, "UNKNOWN")
                print(f"  {comment[:50]}... → {label}")
            
            # Ask if user wants to save demo results
            save_demo = input("\nSave demo results to file? (y/n): ").strip().lower()
            if save_demo == "y":
                demo_output_file = f"demo_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                demo_results = []
                for idx, row in sample_df.iterrows():
                    demo_results.append({
                        'comment_idx': idx,
                        'comment': str(row.get('comment_raw', '')),
                        'label': labels.get(idx, 'UNKNOWN')
                    })
                demo_df = pd.DataFrame(demo_results)
                output_dir = ensure_output_dir(version)
                demo_path = output_dir / demo_output_file
                demo_df.to_excel(demo_path, index=False)
                print(f"Demo results saved to: {demo_path}")
        else:
            print("Demo cancelled.")
    
    elif mode == "2":
        # Full pipeline - Calculate estimates
        estimates = enhanced_estimate_processing_time(df, model_name)
        
        # Calculate unique summaries if available
        unique_summaries = 'N/A'
        if 'summary' in df.columns:
            unique_summaries = df['summary'].nunique()
        
        print(f"\n=== FULL LABELING MODE ===")
        print(f"Resource estimation:")
        print(f"  - Comments to label: {estimates['total_comments']}")
        print(f"  - Unique summaries: {unique_summaries}")
        print(f"  - Estimated batches: {estimates['estimated_batches']}")
        print(f"  - Adjusted batch size: {estimates['adjusted_batch_size']}")
        print(f"  - API keys: {len(API_KEYS)}")
        print(f"  - Model: {model_name}")
        print(f"  - Total capacity: {estimates['total_rpm_capacity']} RPM, {estimates['total_tpm_capacity']:,} TPM, {estimates['total_rpd_capacity']} RPD")
        print(f"  - Estimated time: ~{estimates['estimated_minutes']:.1f} minutes (~{estimates['estimated_hours']:.2f} hours)")
        print(f"  - Effective RPM: {estimates['effective_rpm']} (90% of total capacity)")
        print(f"  - Feasible: {'✅ Yes' if estimates['is_feasible'] else '❌ No'}")
        
        if not estimates['is_feasible']:
            print(f"  - Issues: {', '.join(estimates['capacity_issues'])}")
            print(f"  - Recommendations: {', '.join(estimates['recommendations'])}")
        
        proceed = input("\nProceed with full labeling? (y/n): ").strip().lower()
        if proceed == "y":
            labeled_df = run_optimized_labeling(df, version, input_file, output_file, model_name)
            print("\n✅ Labeling completed successfully!")
        else:
            print("Full labeling cancelled.")
    
    elif mode == "3":
        # Comparison mode
        print(f"\n=== MODEL COMPARISON MODE ===")
        
        # List available Excel files in output directory
        output_dir = config.get_path(version, "output")
        excel_files = list(output_dir.glob("*.xlsx"))
        
        if len(excel_files) < 2:
            print(f"❌ Need at least 2 Excel files for comparison. Found {len(excel_files)} files.")
            return
        
        print(f"Available files for comparison:")
        for i, file in enumerate(excel_files):
            print(f"  {i+1}. {file.name}")
        
        # Select first file
        try:
            choice1 = int(input(f"\nSelect first file (1-{len(excel_files)}): ").strip()) - 1
            if not (0 <= choice1 < len(excel_files)):
                print("Invalid choice for first file")
                return
            file1 = excel_files[choice1]
        except ValueError:
            print("Invalid input for first file")
            return
        
        # Select second file
        try:
            choice2 = int(input(f"Select second file (1-{len(excel_files)}): ").strip()) - 1
            if not (0 <= choice2 < len(excel_files)):
                print("Invalid choice for second file")
                return
            file2 = excel_files[choice2]
        except ValueError:
            print("Invalid input for second file")
            return
        
        if choice1 == choice2:
            print("❌ Cannot compare the same file with itself")
            return
        
        # Run comparison
        print(f"\nComparing:")
        print(f"  File 1: {file1.name}")
        print(f"  File 2: {file2.name}")
        
        comparison_result = compare_model_results(version, file1.name, file2.name)
        
        if comparison_result:
            print(f"\n✅ Comparison completed successfully!")
            print(f"Overall accuracy: {comparison_result['accuracy']:.1f}%")
        else:
            print("❌ Comparison failed")
    
    else:
        print("❌ Invalid mode selection. Please choose 1, 2, or 3.")

if __name__ == "__main__":
    args = parse_args()
    if args.version:
        main(args.version, args.input or "pre_labeled.xlsx", 
             args.output or "gemini_labeled.xlsx", args.model)
    else:
        # Interactive mode
        version = input("Enter version (e.g., v1, v2): ").strip()
        if version:
            main(version)
        else:
            print("Version is required!")