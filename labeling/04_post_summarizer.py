import pandas as pd
import time
import itertools
import os
from tqdm import tqdm
import unicodedata
import json
import uuid
import numpy as np
from dotenv import load_dotenv

# Try to import google-generativeai with error handling
try:
    import google.generativeai as genai
    GENAI_AVAILABLE = True
    print("✅ Google GenerativeAI imported successfully")
except ImportError as e:
    GENAI_AVAILABLE = False
    print(f"❌ Failed to import google.generativeai: {e}")
    print("🔧 Please run: pip install --force-reinstall google-generativeai protobuf==4.25.3")
    exit(1)

# Tải biến môi trường từ file .env
load_dotenv()

# Lấy danh sách API keys từ biến môi trường, tách bằng dấu ";"
api_keys_str = os.getenv("GEMINI_API_KEYS", "")
API_KEYS = [key.strip() for key in api_keys_str.split(";") if key.strip()]

# Tạo iterator xoay vòng API key
key_cycle = itertools.cycle(API_KEYS)

# Kiểm tra nếu không có key nào thì cảnh báo
if not API_KEYS:
    print("❌ No API keys loaded. Please set GEMINI_API_KEYS in .env.")
    exit(1)


MODEL = "gemini-2.0-flash"
RATE_LIMIT = 4.1  # 60s/15 requests = 4s + buffer
BATCH_SIZE = 5    # Number of posts per request
MAX_TOKENS = 3000 # Maximum tokens per request 
RETRY_ATTEMPTS = 3

# ---- OPTIMIZED PROMPT ----
OPTIMIZED_PROMPT = """Tóm tắt mỗi đoạn văn bản sau thành 3 mục:
1. Nội dung sơ lược
2. Vấn đề
3. Phản động/tin giả (có/không và giải thích ngắn)

{text_entries}

Trả lời theo định dạng JSON chính xác như sau, với mỗi đoạn văn bản có ID riêng:
```json
{{
  "results": [
    {{
      "id": "id1",
      "summary": {{
        "nội_dung": "tóm tắt nội dung...",
        "vấn_đề": "vấn đề muốn trình bày...",
        "phản_động": "có/không và giải thích ngắn..."
      }}
    }},
    {{
      "id": "id2",
      "summary": {{
        "nội_dung": "tóm tắt nội dung...",
        "vấn_đề": "vấn đề muốn trình bày...",
        "phản_động": "có/không và giải thích ngắn..."
      }}
    }}
  ]
}}
```"""

def check_environment():
    """Kiểm tra môi trường trước khi chạy"""
    print("🔍 CHECKING ENVIRONMENT")
    print("-" * 40)
    
    # Check packages
    try:
        import google.generativeai as genai
        print("✅ google-generativeai: OK")
    except ImportError:
        print("❌ google-generativeai: FAILED")
        return False
    
    try:
        import google.protobuf
        print(f"✅ protobuf version: {google.protobuf.__version__}")
    except ImportError:
        print("❌ protobuf: FAILED")
        return False
    
    # Test API connection
    try:
        genai.configure(api_key=API_KEYS[0])
        models = list(genai.list_models())
        print(f"✅ API connection: OK ({len(models)} models available)")
        return True
    except Exception as e:
        print(f"❌ API connection: FAILED - {e}")
        return False

def set_next_key():
    """Chuyển sang API key tiếp theo"""
    api_key = next(key_cycle)
    genai.configure(api_key=api_key)
    return api_key

def estimate_tokens(text):
    """Ước tính tokens (4 chars = 1 token for Vietnamese)"""
    if not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

def clean_text(text):
    """Làm sạch text"""
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize('NFC', text.strip())
    # Replace problematic characters
    text = text.replace('"', "'").replace('\n', ' ').replace('\r', ' ')
    return text

def create_batch_prompt(post_batch):
    """Tạo prompt cho batch posts"""
    text_entries = []
    batch_ids = []
    
    for idx, post in enumerate(post_batch):
        post_id = f"id{idx+1}"
        batch_ids.append(post_id)
        clean_post = clean_text(post)
        
        # Truncate if too long
        if estimate_tokens(clean_post) > MAX_TOKENS // len(post_batch):
            clean_post = clean_post[:int(len(clean_post) * 0.75)] + "..."
        
        text_entries.append(f"Văn bản {post_id}:\n\"{clean_post}\"")
    
    formatted_entries = "\n\n".join(text_entries)
    prompt = OPTIMIZED_PROMPT.format(text_entries=formatted_entries)
    
    return prompt, batch_ids

def process_batch(post_batch, batch_index, total_batches):
    """Xử lý một batch posts"""
    # Convert NumPy array to list if needed
    if isinstance(post_batch, np.ndarray):
        post_batch = post_batch.tolist()
    
    # Check if batch is empty using length
    if len(post_batch) == 0:
        return {}
    
    prompt, batch_ids = create_batch_prompt(post_batch)
    estimated_tokens = estimate_tokens(prompt)
    
    print(f"\n📦 Processing batch {batch_index+1}/{total_batches}")
    print(f"   Posts in batch: {len(post_batch)}")
    print(f"   Estimated tokens: {estimated_tokens}")
    
    # Check if batch is too large
    if estimated_tokens > MAX_TOKENS:
        print(f"⚠️  Batch too large ({estimated_tokens} tokens > {MAX_TOKENS})!")
        if len(post_batch) <= 1:
            # If single post is too large, truncate it
            print("   Single post too large, truncating...")
            prompt, batch_ids = create_batch_prompt([post_batch[0][:int(len(post_batch[0])*0.5)] + "..."])
        else:
            # Split batch in half and process recursively
            print("   Splitting batch in half...")
            mid = len(post_batch) // 2
            results1 = process_batch(post_batch[:mid], batch_index, total_batches)
            results2 = process_batch(post_batch[mid:], batch_index, total_batches)
            results1.update(results2)
            return results1
    
    # Process batch with retries
    for attempt in range(RETRY_ATTEMPTS):
        try:
            print(f"  ⏳ Waiting {RATE_LIMIT}s to respect rate limit...")
            time.sleep(RATE_LIMIT)
            
            current_key = set_next_key()
            print(f"  🔑 Using API key: ...{current_key[-4:]}")
            
            response = genai.GenerativeModel(MODEL).generate_content(
                prompt,
                generation_config={
                    "temperature": 0.2,
                    "max_output_tokens": 1024
                }
            )
            
            # Log token usage
            try:
                usage = response.usage_metadata
                prompt_tokens = usage.prompt_token_count
                output_tokens = getattr(usage, 'candidates_token_count', 0)
                print(f"  ✅ Batch {batch_index+1}/{total_batches} | " 
                      f"In: {prompt_tokens} | Out: {output_tokens} tokens")
            except:
                print(f"  ✅ Batch {batch_index+1}/{total_batches} | Token usage unavailable")
            
            # Extract JSON from response
            response_text = response.text.strip()
            
            # Find JSON part
            json_start = response_text.find('```json')
            json_end = response_text.rfind('```')
            
            if json_start != -1 and json_end != -1 and json_end > json_start:
                json_text = response_text[json_start+7:json_end].strip()
            else:
                json_text = response_text
            
            # Clean up JSON text
            json_text = json_text.replace('```', '').strip()
            
            try:
                results_dict = json.loads(json_text)
                
                # Map results to post_batch
                summaries = {}
                for result in results_dict.get('results', []):
                    result_id = result.get('id')
                    if result_id in batch_ids:
                        post_idx = batch_ids.index(result_id)
                        if post_idx < len(post_batch):
                            summaries[post_batch[post_idx]] = result.get('summary', {})
                
                print(f"  ✅ Successfully processed {len(summaries)}/{len(post_batch)} posts")
                return summaries
                
            except json.JSONDecodeError as e:
                print(f"  ⚠️ JSON parse error: {e}")
                print(f"  Raw response: {response_text[:100]}...")
                
                # Fallback for each post in batch
                fallback_summaries = {}
                for idx, post in enumerate(post_batch):
                    fallback_summaries[post] = {
                        "nội_dung": f"Không thể phân tích JSON từ response. Raw: {response_text[:50]}...",
                        "vấn_đề": "Lỗi xử lý",
                        "phản_động": "Không xác định do lỗi xử lý"
                    }
                return fallback_summaries
                
        except Exception as e:
            error_str = str(e)
            print(f"  ❌ Attempt {attempt+1} failed: {error_str}")
            
            if "429" in error_str or "quota" in error_str.lower():
                wait_time = 60
                print(f"  🕐 Rate limit hit! Waiting {wait_time}s for quota reset...")
                time.sleep(wait_time)
            elif attempt < RETRY_ATTEMPTS - 1:
                time.sleep(10)
    
    # Fallback if all attempts fail
    print("  ❌ All attempts failed for this batch")
    fallback = {}
    for post in post_batch:
        fallback[post] = {
            "nội_dung": "Không thể tóm tắt sau nhiều lần thử",
            "vấn_đề": "Lỗi API",
            "phản_động": "Không xác định do lỗi xử lý"
        }
    return fallback

def format_summary(summary_dict):
    """Format summary dictionary into readable text"""
    if not summary_dict:
        return "❌ Không thể tóm tắt"
    
    noi_dung = summary_dict.get('nội_dung', 'Không có thông tin')
    van_de = summary_dict.get('vấn_đề', 'Không có thông tin')
    phan_dong = summary_dict.get('phản_động', 'Không có thông tin')
    
    return f"""1. Nội dung sơ lược: {noi_dung}

2. Vấn đề: {van_de}

3. Phản động/tin giả: {phan_dong}"""

def main_batch():
    """Main function với batch processing"""
    # Check environment first
    if not check_environment():
        print("\n🚨 Environment check failed! Please fix the issues above.")
        return
    
    input_file = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\dataset\dataset.xlsx"
    output_file = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\dataset\dataset_labeled.xlsx"
    txt_file = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\dataset\posts_comparison.txt"
    
    print("\n🚀 GEMINI 2.0 FLASH - BATCH PROCESSING")
    print("=" * 60)
    
    # Check input file
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    # Load data
    try:
        df_original = pd.read_excel(input_file)
        # Convert NumPy array to list for safety
        unique_posts = df_original['post'].unique().tolist()
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return
    
    # Estimate time
    num_batches = (len(unique_posts) + BATCH_SIZE - 1) // BATCH_SIZE
    estimated_minutes = num_batches * RATE_LIMIT / 60
    
    print(f"📊 Dataset Info:")
    print(f"   Total records: {len(df_original):,}")
    print(f"   Unique posts: {len(unique_posts):,}")
    print(f"   Batch size: {BATCH_SIZE} posts per request")
    print(f"   Number of batches: {num_batches}")
    print(f"   Model: {MODEL}")
    print(f"   Rate limit: {RATE_LIMIT}s per batch")
    print(f"   Estimated time: {estimated_minutes:.1f} minutes")
    
    # Confirm to proceed
    proceed = input(f"\n🤔 Proceed with batch processing? (y/n): ").lower().strip()
    if proceed != 'y':
        print("❌ Cancelled")
        return
    
    # Create batches - Explicitly convert to list
    batches = []
    for i in range(0, len(unique_posts), BATCH_SIZE):
        batches.append(unique_posts[i:i+BATCH_SIZE])
    
    print(f"\n🔄 Processing {len(unique_posts)} posts in {len(batches)} batches...")
    
    # Dictionary to store all summaries
    all_summaries = {}
    
    # Text comparison content
    txt_content = [
        "=" * 80,
        f"GEMINI {MODEL.upper()} - BATCH SUMMARIZATION RESULTS",
        f"Generated on: {pd.Timestamp.now()}",
        f"Total posts: {len(unique_posts)}",
        f"Batch size: {BATCH_SIZE}",
        "=" * 80,
        ""
    ]
    
    start_time = time.time()
    
    # Process all batches
    for batch_idx, batch in enumerate(tqdm(batches, desc="Processing batches")):
        batch_results = process_batch(batch, batch_idx, len(batches))
        all_summaries.update(batch_results)
        
        # Add batch results to text comparison
        for post_idx, post in enumerate(batch):
            post_num = batch_idx * BATCH_SIZE + post_idx + 1
            summary_dict = batch_results.get(post, {})
            formatted_summary = format_summary(summary_dict)
            
            txt_content.extend([
                f"POST {post_num}:",
                "-" * 50,
                "ORIGINAL:",
                str(post),
                "",
                "SUMMARY:",
                formatted_summary,
                "",
                "=" * 80,
                ""
            ])
    
    # Create DataFrame with summaries
    posts_df = pd.DataFrame({'post_original': list(all_summaries.keys())})
    posts_df['post_summary'] = posts_df['post_original'].apply(
        lambda x: format_summary(all_summaries.get(x, {}))
    )
    
    # Merge with original dataset
    # Create a mapping from post to summary
    summary_mapping = dict(zip(posts_df['post_original'], posts_df['post_summary']))
    
    # Add summary column to original DataFrame
    df_output = df_original.copy()
    df_output['summary'] = df_output['post'].map(summary_mapping)
    
    # Save results
    try:
        df_output.to_excel(output_file, index=False)
        print(f"✅ Excel saved: {output_file}")
    except Exception as e:
        print(f"❌ Failed to save Excel: {e}")
    
    try:
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(txt_content))
        print(f"✅ TXT saved: {txt_file}")
    except Exception as e:
        print(f"❌ Failed to save TXT: {e}")
    
    # Final stats
    elapsed_time = time.time() - start_time
    success_count = sum(1 for summary in all_summaries.values() if 'nội_dung' in summary)
    
    print(f"\n🎉 COMPLETED!")
    print(f"📊 Results:")
    print(f"   ✅ Successful: {success_count}/{len(unique_posts)}")
    print(f"   ❌ Failed: {len(unique_posts) - success_count}/{len(unique_posts)}")
    print(f"   ⏱️  Time taken: {elapsed_time/60:.2f} minutes")
    print(f"   💰 Requests saved: {len(unique_posts) - len(batches)} (~{100 - len(batches)*100/len(unique_posts):.1f}%)")

if __name__ == "__main__":
    main_batch()