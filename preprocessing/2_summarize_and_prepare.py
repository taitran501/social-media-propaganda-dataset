import re
import pandas as pd
import time
import itertools
import os
import argparse
import sys
from pathlib import Path
from tqdm import tqdm
import unicodedata
import json
import uuid
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# Điều chỉnh đường dẫn import
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Thêm thư mục cha vào path để import config
from utils.file_utils import save_excel_file
import config

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Summarize posts and prepare dataset')
    parser.add_argument('--version', '-v', help='Version to process (e.g., v1, v2)')
    parser.add_argument('--source', '-s', 
                        choices=['platform_split', 'output', 'merge', 'raw'], 
                        help='Source folder to process files from')
    parser.add_argument('--file', '-f', help='Specific file to process')
    parser.add_argument('--all', '-a', action='store_true', 
                        help='Process all Excel files in the selected folder')
    return parser.parse_args()

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

# ---- API CONFIGURATION WITH RATE LIMITING ----
# Import API keys from centralized config
from config import get_api_keys
API_KEYS = get_api_keys()

# Rate limits cho free tier (conservative values)
RATE_LIMITS = {
    "gemini-2.0-flash": {
        "rpm": 15,      # Requests per minute
        "tpm": 1000000, # Tokens per minute  
        "rpd": 1500     # Requests per day
    },
    "gemini-2.5-flash-preview-05-20": {
        "rpm": 10,
        "tpm": 250000,
        "rpd": 500
    }
}

BATCH_SIZE = 3    # Giảm batch size để tránh lỗi
MAX_TOKENS = 4000 # Tăng token limit cho prompt phức tạp hơn
RETRY_ATTEMPTS = 3

class APIKeyManager:
    def __init__(self, api_keys, model_name="gemini-2.0-flash"):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.model_name = model_name
        self.limits = RATE_LIMITS.get(model_name, RATE_LIMITS["gemini-2.0-flash"])
        
        # Tracking cho mỗi API key
        self.usage_tracking = {key: {
            "requests_today": 0,
            "requests_this_minute": 0,
            "last_request_time": None,
            "last_reset_time": datetime.now()
        } for key in api_keys}
        
        self.setup_genai()
    
    def setup_genai(self):
        """Setup Google Generative AI with current API key"""
        try:
            genai.configure(api_key=self.api_keys[self.current_key_index])
            # Test connection
            list(genai.list_models())
            print(f"✅ API Key {self.current_key_index + 1} configured successfully")
        except Exception as e:
            print(f"❌ Failed to configure API Key {self.current_key_index + 1}: {e}")
            self.switch_api_key()
    
    def switch_api_key(self):
        """Switch to next available API key"""
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        print(f"🔄 Switching to API Key {self.current_key_index + 1}")
        self.setup_genai()
    
    def can_make_request(self):
        """Check if we can make a request with current API key"""
        current_key = self.api_keys[self.current_key_index]
        usage = self.usage_tracking[current_key]
        now = datetime.now()
        
        # Reset daily counter if needed
        if now.date() > usage["last_reset_time"].date():
            usage["requests_today"] = 0
            usage["last_reset_time"] = now
        
        # Reset minute counter if needed
        if usage["last_request_time"] and (now - usage["last_request_time"]).seconds >= 60:
            usage["requests_this_minute"] = 0
        
        # Check limits
        if usage["requests_today"] >= self.limits["rpd"]:
            print(f"⚠️ Daily limit reached for API Key {self.current_key_index + 1}")
            return False
        
        if usage["requests_this_minute"] >= self.limits["rpm"]:
            print(f"⚠️ Minute limit reached for API Key {self.current_key_index + 1}")
            return False
        
        return True
    
    def find_available_key(self):
        """Find an API key that can make requests"""
        original_index = self.current_key_index
        
        while True:
            if self.can_make_request():
                return True
            
            self.switch_api_key()
            
            # If we've tried all keys, return False
            if self.current_key_index == original_index:
                return False
    
    def record_request(self):
        """Record that a request was made"""
        current_key = self.api_keys[self.current_key_index]
        usage = self.usage_tracking[current_key]
        now = datetime.now()
        
        usage["requests_today"] += 1
        usage["requests_this_minute"] += 1
        usage["last_request_time"] = now
    
    def wait_if_needed(self):
        """Wait if we need to respect rate limits"""
        if not self.find_available_key():
            # All keys are rate limited, wait
            print("⏳ All API keys are rate limited. Waiting 60 seconds...")
            time.sleep(60)
            return self.find_available_key()
        return True
    
    def get_usage_stats(self):
        """Get usage statistics for all API keys"""
        stats = {}
        for i, key in enumerate(self.api_keys):
            usage = self.usage_tracking[key]
            stats[f"Key_{i+1}"] = {
                "requests_today": usage["requests_today"],
                "requests_this_minute": usage["requests_this_minute"],
                "daily_limit": self.limits["rpd"],
                "minute_limit": self.limits["rpm"]
            }
        return stats

# ---- IMPROVED PROMPT WITH KEY TERMS RECOGNITION ----
IMPROVED_PROMPT = """Trước khi tóm tắt, hãy nhận diện các từ khóa/biệt ngữ chính trị sau trong văn bản:

1. Từ khóa ám chỉ phe ủng hộ Đảng Cộng sản:
   - "bò đỏ": người ủng hộ/tuyên truyền cho Đảng Cộng sản, thường mang tính chế giễu
   - "dư luận viên": người được cho là được thuê để định hướng dư luận ủng hộ chính phủ
   - "Rét bun": chơi chữ từ "Red Bull", dùng để chỉ phe "đỏ" (Cộng sản) một cách mỉa mai

2. Từ khóa miệt thị chỉ Việt Nam hiện tại/Đảng Cộng sản:
   - "vẹm" / "xứ vẹm": từ miệt thị chỉ Việt Nam Cộng sản (từ "Việt cộng")
   - "đẽng": từ lóng chỉ Đảng Cộng sản Việt Nam
   - "Việt cộng"/"vc": từ chỉ người cộng sản Việt Nam, mang tính chất miệt thị
   - "+ Sản": viết tắt, lóng của "Cộng sản"
   - "Béc" / "béc hù" / "hochominh": cách gọi Hồ Chí Minh theo lối chế nhạo
   - "Tô thị Lâm Bò": ám chỉ Tổng Bí thư Tô Lâm
   - "cộng hoà xuống hố cả nút": ám chỉ chế độ hiện tại

3. Từ khóa liên quan đến phe quốc gia/Việt Nam Cộng Hòa:
   - "Cờ vàng ba sọc đỏ": biểu tượng Việt Nam Cộng hòa, thường gắn với người Việt hải ngoại
   - "ba que" / "3 que": từ lóng để chỉ người ủng hộ Việt Nam Cộng Hòa, từ miệt thị
   - "VNCH": viết tắt của Việt Nam Cộng Hòa
   - "Đu càng Cali": ám chỉ người Việt hải ngoại ở California
   - "Hậu duệ Việt Nam Cộng Hòa": cách gọi con cháu người miền Nam di cư sau 1975

4. Các từ khóa chính trị khác cần chú ý:
   - "yêu lước": cách viết chế giễu từ "yêu nước", ám chỉ việc lợi dụng lòng yêu nước
   - "Mẽo": cách gọi Mỹ, thường mang tính châm biếm
   - "tộc cối": từ miệt thị để chỉ người miền Bắc
   - "chệt": từ miệt thị để chỉ người Trung Quốc
   - "Đấu tố": hành động tố giác, công kích công khai một người
   - "tay sai": ám chỉ người làm việc cho thế lực nước ngoài
   - "phản động": từ dùng để chỉ những người chống đối chính quyền
   - "Đảng trị": ám chỉ hệ thống chính trị một đảng cầm quyền
   - "barwhere", "cani": từ lóng miệt thị, biến tấu từ tiếng Anh
   - "ngụy": từ miệt thị chỉ chính quyền Việt Nam Cộng Hòa và người ủng hộ

Bây giờ, hãy tóm tắt nội dung trên thành 3 mục, yêu cầu ngắn gọn:

{text_entries}

Trả lời theo định dạng JSON chính xác như sau:
```json
{{
  "results": [
    {{
      "id": "id1",
      "summary": "1. Nội dung sơ lược: [tóm tắt ngắn gọn]\\n2. Vấn đề: [vấn đề chính]\\n3. Phản động/tin giả: [có/không và giải thích ngắn]"
    }},
    {{
      "id": "id2", 
      "summary": "1. Nội dung sơ lược: [tóm tắt ngắn gọn]\\n2. Vấn đề: [vấn đề chính]\\n3. Phản động/tin giả: [có/không và giải thích ngắn]"
    }}
  ]
}}
```

Lưu ý: 
1. Trả lời ngắn gọn và theo đúng format với 3 mục như yêu cầu.
2. Nếu văn bản chứa các từ khóa/biệt ngữ đã liệt kê ở trên, hãy đánh giá đúng tính chất chính trị của nó.
3. Đánh giá "Phản động/tin giả" cần dựa trên việc có sử dụng ngôn ngữ thù ghét, kích động chia rẽ, xuyên tạc hay không.
4. Ngay cả khi văn bản ngắn, hãy chú ý đến các từ khóa và biệt ngữ đã liệt kê để đánh giá đúng."""

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

def get_source_folder(version, source_type):
    """Get source folder path based on type"""
    paths = config.get_version_paths(version)
    
    if source_type == 'platform_split':
        return paths['raw_dir'].parent / "platform_split"
    elif source_type == 'output':
        return paths['output_dir']
    elif source_type == 'merge':
        return paths['merge_dir']
    elif source_type == 'raw':
        return paths['raw_dir']
    else:
        raise ValueError(f"Unknown source type: {source_type}")

def find_excel_files(folder_path):
    """Find all Excel files in a folder"""
    excel_files = list(folder_path.glob("*.xlsx"))
    excel_files.extend(list(folder_path.glob("*.xls")))
    return excel_files

def choose_source_and_files(version):
    """Interactive function to choose source folder and files"""
    print(f"\n📁 CHỌN NGUỒN DỮ LIỆU (VERSION {version})")
    print("-" * 50)
    
    # List available source folders
    source_options = ['platform_split', 'output', 'merge', 'raw']
    paths = config.get_version_paths(version)
    
    print("Các folder có sẵn:")
    available_sources = []
    for i, source in enumerate(source_options):
        folder_path = get_source_folder(version, source)
        excel_files = find_excel_files(folder_path)
        if folder_path.exists() and excel_files:
            available_sources.append(source)
            print(f"  {len(available_sources)}. {source} ({len(excel_files)} file Excel)")
        else:
            print(f"     {source} (không có file hoặc không tồn tại)")
    
    if not available_sources:
        print("❌ Không tìm thấy file Excel nào trong tất cả các folder!")
        return None, []
    
    # Choose source folder
    while True:
        try:
            choice = input(f"\nChọn source folder (1-{len(available_sources)}): ").strip()
            source_idx = int(choice) - 1
            if 0 <= source_idx < len(available_sources):
                selected_source = available_sources[source_idx]
                break
            else:
                print("Lựa chọn không hợp lệ!")
        except ValueError:
            print("Vui lòng nhập số!")
    
    # Get files from selected source
    source_folder = get_source_folder(version, selected_source)
    excel_files = find_excel_files(source_folder)
    
    print(f"\n📄 CHỌN FILE TỪ {selected_source.upper()}")
    print("-" * 50)
    print("Các file có sẵn:")
    for i, file in enumerate(excel_files):
        print(f"  {i+1}. {file.name}")
    
    print(f"  {len(excel_files)+1}. Tất cả file ({len(excel_files)} files)")
    
    # Choose files
    while True:
        try:
            choice = input(f"\nChọn file (1-{len(excel_files)+1}, hoặc 'a' cho tất cả): ").strip().lower()
            
            if choice == 'a' or choice == str(len(excel_files)+1):
                return selected_source, excel_files
            else:
                file_idx = int(choice) - 1
                if 0 <= file_idx < len(excel_files):
                    return selected_source, [excel_files[file_idx]]
                else:
                    print("Lựa chọn không hợp lệ!")
        except ValueError:
            print("Vui lòng nhập số hoặc 'a'!")

def estimate_tokens(text):
    """Ước tính tokens (4 chars = 1 token for Vietnamese)"""
    if not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

def clean_text(text):
    """Làm sạch text để tránh safety filters"""
    if not isinstance(text, str):
        return ""
    
    text = unicodedata.normalize('NFC', text.strip())
    
    # Remove URLs, emails, phone numbers
    import re
    text = re.sub(r'http[s]?://\S+', '[URL]', text)
    text = re.sub(r'\S+@\S+', '[EMAIL]', text)
    text = re.sub(r'\d{3}-?\d{3}-?\d{4}', '[PHONE]', text)
    
    # Replace problematic characters
    text = text.replace('"', "'").replace('\n', ' ').replace('\r', ' ')
    
    # Remove excessive punctuation
    text = re.sub(r'[!@#$%^&*()]+', ' ', text)
    
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
    
    # Cập nhật prompt với hướng dẫn JSON rõ ràng hơn
    prompt_template = IMPROVED_PROMPT.replace("{text_entries}", formatted_entries)
    prompt_template += """

LƯU Ý QUAN TRỌNG VỀ ĐỊNH DẠNG JSON:
1. Đảm bảo JSON trả về PHẢI hợp lệ 100%.
2. KHÔNG sử dụng dấu xuống dòng thực tế trong chuỗi JSON, thay vào đó sử dụng '\\n'.
3. Escape tất cả dấu ngoặc kép trong chuỗi JSON với '\\\"'.
4. Mỗi thẻ 'summary' phải là một chuỗi liên tục, không có ngắt dòng thật.
5. Đặt phần tóm tắt trong một chuỗi duy nhất, đảm bảo có dấu phẩy đúng cách giữa các đối tượng.
6. Sử dụng đúng định dạng 'id1', 'id2', v.v. như đã cung cấp trong văn bản.

JSON PHẢI có cấu trúc chính xác như sau:
```json
{
  "results": [
    {
      "id": "id1",
      "summary": "1. Nội dung sơ lược: [tóm tắt]\\n2. Vấn đề: [vấn đề]\\n3. Phản động/tin giả: [có/không và giải thích]"
    },
    {
      "id": "id2",
      "summary": "1. Nội dung sơ lược: [tóm tắt]\\n2. Vấn đề: [vấn đề]\\n3. Phản động/tin giả: [có/không và giải thích]"
    }
  ]
}
```"""
    
    return prompt_template, batch_ids

def process_batch(api_manager, post_batch, batch_index, total_batches):
    """Xử lý một batch posts với API manager"""
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
            results1 = process_batch(api_manager, post_batch[:mid], batch_index, total_batches)
            results2 = process_batch(api_manager, post_batch[mid:], batch_index, total_batches)
            results1.update(results2)
            return results1
    
    # Process batch with retries
    for attempt in range(RETRY_ATTEMPTS):
        try:
            # Wait for rate limit using API manager
            if not api_manager.wait_if_needed():
                print("❌ All API keys exhausted for today")
                return {}
            
            current_key = api_manager.api_keys[api_manager.current_key_index]
            print(f"  🔑 Using API key: ...{current_key[-4:]}")
            
            response = genai.GenerativeModel(api_manager.model_name).generate_content(
                prompt,
                generation_config={
                    "temperature": 0.1,
                    "max_output_tokens": 2048
                },
                safety_settings={
                    'HATE': 'BLOCK_NONE',
                    'HARASSMENT': 'BLOCK_NONE', 
                    'SEXUAL': 'BLOCK_NONE',
                    'DANGEROUS': 'BLOCK_NONE'
                }
            )
            
            # Record the request
            api_manager.record_request()
            
            # Log token usage
            try:
                usage = response.usage_metadata
                prompt_tokens = usage.prompt_token_count
                output_tokens = getattr(usage, 'candidates_token_count', 0)
                print(f"  ✅ Batch {batch_index+1}/{total_batches} | " 
                      f"In: {prompt_tokens} | Out: {output_tokens} tokens")
            except:
                print(f"  ✅ Batch {batch_index+1}/{total_batches} | Token usage unavailable")
            
            # Check if response is blocked or empty
            if not response.candidates or not response.candidates[0].content.parts:
                finish_reason = response.candidates[0].finish_reason if response.candidates else "UNKNOWN"
                print(f"  ⚠️ Response blocked or empty. Finish reason: {finish_reason}")
                
                # Create fallback summaries for this batch
                fallback_summaries = {}
                for post in post_batch:
                    fallback_summaries[post] = "Nội dung bị chặn bởi AI safety filter"
                return fallback_summaries
    
            # Extract text from response
            response_text = response.text.strip()
            
            # Super resilient JSON parsing
            try:
                results_dict = super_resilient_json_parser(response_text)
                
                # Map results to post_batch
                summaries = {}
                for result in results_dict.get('results', []):
                    result_id = result.get('id')
                    if result_id in batch_ids:
                        post_idx = batch_ids.index(result_id)
                        if post_idx < len(post_batch):
                            summaries[post_batch[post_idx]] = result.get('summary', '')
                
                print(f"  ✅ Successfully processed {len(summaries)}/{len(post_batch)} posts")
                return summaries
                
            except json.JSONDecodeError as e:
                print(f"  ❌ All JSON parsing methods failed: {e}")
                print(f"  📑 Dumping response for debugging (first 200 chars): {response_text[:200]}...")
                
                # Ultimate fallback - extract anything that looks like a summary with regex
                summaries = {}
                pattern = r'1\.\s*Nội dung sơ lược:(.*?)(?:(?:\n|\\n)2\.|$)'
                matches = re.findall(pattern, response_text, re.DOTALL)
                
                if matches:
                    print(f"  🔄 Last resort: Found {len(matches)} potential summaries")
                    for i, match in enumerate(matches):
                        if i < len(post_batch):
                            # Try to rebuild a complete summary by looking for parts 2 and 3
                            summary_text = f"1. Nội dung sơ lược:{match.strip()}"
                            
                            # Look for part 2
                            part2_match = re.search(r'2\.\s*Vấn đề:(.*?)(?:(?:\n|\\n)3\.|$)', response_text, re.DOTALL)
                            if part2_match:
                                summary_text += f"\n2. Vấn đề:{part2_match.group(1).strip()}"
                            
                            # Look for part 3
                            part3_match = re.search(r'3\.\s*Phản động/tin giả:(.*?)(?:\n|\\n|$)', response_text, re.DOTALL)
                            if part3_match:
                                summary_text += f"\n3. Phản động/tin giả:{part3_match.group(1).strip()}"
                            
                            summaries[post_batch[i]] = summary_text
                    
                    if summaries:
                        print(f"  ✅ Extracted {len(summaries)} summaries through final fallback")
                        return summaries
                
                # If absolutely nothing worked, create placeholder summaries
                print("  ⚠️ Using placeholder summaries as last resort")
                placeholders = {}
                for idx, post in enumerate(post_batch):
                    placeholders[post] = f"1. Nội dung sơ lược: [Lỗi JSON]\n2. Vấn đề: Không xác định\n3. Phản động/tin giả: Không xác định"
                return placeholders
                
        except Exception as e:
            error_str = str(e)
            print(f"  ❌ Attempt {attempt+1} failed: {error_str}")
            
            # Check for specific error types
            if "finish_reason" in error_str and "2" in error_str:
                print(f"  🚫 Content blocked by safety filter")
                # Return fallback immediately for safety blocks
                fallback_summaries = {}
                for post in post_batch:
                    fallback_summaries[post] = "Nội dung bị chặn bởi AI safety filter"
                return fallback_summaries
            elif "429" in error_str or "quota" in error_str.lower():
                print(f"  🔄 Rate limit detected, switching API key...")
                api_manager.switch_api_key()
                time.sleep(2)
            elif attempt < RETRY_ATTEMPTS - 1:
                time.sleep(10)
    
    # Fallback if all attempts fail
    print("  ❌ All attempts failed for this batch")
    fallback_summaries = {}
    for post in post_batch:
        fallback_summaries[post] = "Không thể tóm tắt sau nhiều lần thử"
    return fallback_summaries

def save_error_log(batch_index, response_text, error):
    """Save error log for debugging"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = Path("./error_logs")
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"batch_{batch_index}_{timestamp}.log"
    
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"ERROR: {error}\n\n")
        f.write("RESPONSE TEXT:\n")
        f.write(response_text)
    
    print(f"  📝 Error log saved to {log_file}")

def clean_json_text(text):
    """Clean up JSON text from response"""
    if '```json' in text:
        json_start = text.find('```json') + 7
        json_end = text.find('```', json_start)
        if json_end != -1:
            text = text[json_start:json_end].strip()
        else:
            text = text[json_start:].strip()
    elif '{' in text and '}' in text:
        start = text.find('{')
        end = text.rfind('}') + 1
        text = text[start:end]
    
    text = text.replace('```', '').strip()
    return text

def fix_json_format(json_text):
    """Fix common JSON format issues, especially with newlines"""
    # Level 1: Basic fixes
    json_text = re.sub(r'([^\\])\\n', r'\1\\n', json_text)  # Fix unescaped newlines
    json_text = re.sub(r'([^\\])\\\"', r'\1\\"', json_text)  # Fix unescaped quotes
    json_text = re.sub(r'([^\\])"([^"]*)"([^"]*)"', r'\1"\2\\"\3"', json_text)  # Fix nested quotes
    json_text = re.sub(r'"}(\s*){', r'"},\1{', json_text)  # Fix missing commas between objects
    
    # Level 2: More aggressive fixes
    json_text = re.sub(r'\n', '', json_text)  # Remove actual newlines
    json_text = re.sub(r'\\+n', '\\n', json_text)  # Fix multiple backslashes before n
    json_text = re.sub(r'\\+\"', '\\"', json_text)  # Fix multiple backslashes before quotes
    
    # Level 3: Extreme fixes
    try:
        # Try parsing as is
        json.loads(json_text)
        return json_text
    except json.JSONDecodeError as e:
        error_message = str(e)
        
        if "Expecting ',' delimiter" in error_message:
            # Fix missing commas - find position and insert comma
            position = int(re.search(r'char (\d+)', error_message).group(1))
            fixed_text = json_text[:position] + "," + json_text[position:]
            return fixed_text
            
        elif "Expecting property name enclosed in double quotes" in error_message:
            # Fix missing quotes around property names
            return re.sub(r'(\s+)(\w+)(:)', r'\1"\2"\3', json_text)
            
        elif "Unterminated string" in error_message:
            # Fix unterminated strings - add missing quote
            position = int(re.search(r'char (\d+)', error_message).group(1))
            fixed_text = json_text[:position] + "\"" + json_text[position:]
            return fixed_text
    
    # Return the original if nothing worked
    return json_text

def super_resilient_json_parser(response_text):
    """Super resilient JSON parser with multiple fallback strategies"""
    # Strategy 1: Clean and try to parse directly
    json_text = clean_json_text(response_text)
    
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        print("  ⚠️ Initial JSON parse failed, trying fixes...")
    
    # Strategy 2: Apply fixes and try again
    fixed_json = fix_json_format(json_text)
    
    try:
        return json.loads(fixed_json)
    except json.JSONDecodeError:
        print("  ⚠️ Fixed JSON still failed, trying more aggressive fixes...")
    
    # Strategy 3: Rebuild JSON entirely if there's a pattern
    summaries = {}
    results = []
    
    # Try extracting with regex
    id_pattern = r'"id"\s*:\s*"(id\d+)"'
    summary_pattern = r'"summary"\s*:\s*"([^"]*(?:\\.[^"]*)*)(?<!\\)"'
    
    id_matches = re.findall(id_pattern, response_text)
    summary_matches = re.findall(summary_pattern, response_text)
    
    if id_matches and summary_matches and len(id_matches) == len(summary_matches):
        print(f"  ⚠️ Rebuilding JSON from {len(id_matches)} matched patterns")
        
        for i in range(len(id_matches)):
            result_id = id_matches[i]
            summary = summary_matches[i].replace('\\n', '\n').replace('\\"', '"')
            results.append({"id": result_id, "summary": summary})
        
        return {"results": results}
    
    # Strategy 4: Super aggressive - just extract based on labels
    if "1. Nội dung sơ lược:" in response_text:
        print("  ⚠️ Last resort: Rebuilding from text patterns")
        
        # Extract all post sections
        post_sections = re.split(r'"id"\s*:\s*"id\d+"', response_text)
        
        if len(post_sections) > 1:
            for i, section in enumerate(post_sections[1:], 1):  # Skip first empty split
                result_id = f"id{i}"
                
                # Try to extract the three parts
                summary_match = re.search(r'1\.\s*Nội dung sơ lược:\s*(.*?)(?:2\.|$)', section, re.DOTALL)
                problem_match = re.search(r'2\.\s*Vấn đề:\s*(.*?)(?:3\.|$)', section, re.DOTALL)
                fake_match = re.search(r'3\.\s*Phản động/tin giả:\s*(.*?)(?:"|$)', section, re.DOTALL)
                
                summary_text = ""
                if summary_match:
                    summary_text += f"1. Nội dung sơ lược: {summary_match.group(1).strip()}\n"
                if problem_match:
                    summary_text += f"2. Vấn đề: {problem_match.group(1).strip()}\n"
                if fake_match:
                    summary_text += f"3. Phản động/tin giả: {fake_match.group(1).strip()}"
                
                if summary_text:
                    results.append({"id": result_id, "summary": summary_text.strip()})
        
        if results:
            return {"results": results }
    
    # If all else fails, throw an error that will trigger the regex fallback
    raise json.JSONDecodeError("Failed all parsing strategies", json_text, 0)

def check_required_columns(df):
    """Kiểm tra các cột bắt buộc trong DataFrame"""
    required_columns = ['post_raw']  # Chỉ cần post_raw là bắt buộc
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Thiếu các cột bắt buộc: {', '.join(missing_columns)}")
        return False
    
    # Kiểm tra các cột khác và thông báo
    optional_columns = ['post_id', 'comment_id', 'comment_raw', 'created_date', 'platform']
    available_optional = [col for col in optional_columns if col in df.columns]
    missing_optional = [col for col in optional_columns if col not in df.columns]
    
    if available_optional:
        print(f"✅ Các cột có sẵn: {', '.join(available_optional)}")
    if missing_optional:
        print(f"⚠️  Các cột tùy chọn thiếu: {', '.join(missing_optional)} (sẽ được tạo tự động)")
    
    return True

def process_single_file(api_manager, input_file, version, model_name):
    """Process a single Excel file"""
    print(f"\n🔄 Xử lý file: {input_file.name}")
    print("-" * 50)
    
    # Load data
    try:
        df_original = pd.read_excel(input_file)
        print(f"📊 Đã đọc file với {len(df_original)} dòng")
        print(f"📋 Các cột hiện có: {list(df_original.columns)}")
        
        # Check for required columns
        if not check_required_columns(df_original):
            return None
            
        # Get unique posts
        post_column = 'post_raw'
        unique_posts = df_original[post_column].dropna().unique().tolist()
        print(f"🔍 Tìm thấy {len(unique_posts)} bài post duy nhất để tóm tắt")
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        return None
    
    # Estimate time
    num_batches = (len(unique_posts) + BATCH_SIZE - 1) // BATCH_SIZE
    rate_limit = 60 / api_manager.limits["rpm"]  # Calculate delay based on RPM
    estimated_minutes = num_batches * rate_limit / 60
    
    print(f"\n📊 Thông tin xử lý:")
    print(f"   Tổng số bản ghi: {len(df_original):,}")
    print(f"   Số post duy nhất: {len(unique_posts):,}")
    print(f"   Kích thước batch: {BATCH_SIZE} posts/request")
    print(f"   Số lượng batch: {num_batches}")
    print(f"   Model: {model_name}")
    print(f"   Rate limit: {rate_limit:.1f}s/batch")
    print(f"   Thời gian ước tính: {estimated_minutes:.1f} phút")
    
    # Create batches - Explicitly convert to list
    batches = []
    for i in range(0, len(unique_posts), BATCH_SIZE):
        batches.append(unique_posts[i:i+BATCH_SIZE])
    
    print(f"\n🔄 Đang xử lý {len(unique_posts)} bài viết trong {len(batches)} batch...")
    
    # Dictionary to store all summaries
    all_summaries = {}
    
    # Text comparison content
    txt_content = [
        "=" * 80,
        f"GEMINI {model_name.upper()} - KẾT QUẢ PHÂN TÍCH CẢI TIẾN",
        f"File: {input_file.name}",
        f"Tạo vào: {pd.Timestamp.now()}",
        f"Tổng số post: {len(unique_posts)}",
        f"Kích thước batch: {BATCH_SIZE}",
        f"Model: {model_name}",
        "=" * 80,
        ""
    ]
    
    start_time = time.time()
    
    # Process all batches
    for batch_idx, batch in enumerate(tqdm(batches, desc="Xử lý batch")):
        batch_results = process_batch(api_manager, batch, batch_idx, len(batches))
        all_summaries.update(batch_results)
        
        # Add batch results to text comparison
        for post_idx, post in enumerate(batch):
            post_num = batch_idx * BATCH_SIZE + post_idx + 1
            summary_text = batch_results.get(post, "❌ Không thể tóm tắt")
            
            txt_content.extend([
                f"POST {post_num}:",
                "-" * 50,
                "ORIGINAL:",
                str(post),
                "",
                "SUMMARY:",
                str(summary_text),
                "",
                "=" * 80,
                ""
            ])
    
    # Add summary column to original DataFrame
    df_output = df_original.copy()
    
    # Create mapping and add summary column
    for post in unique_posts:
        summary_text = all_summaries.get(post, '')
        mask = df_output[post_column] == post
        df_output.loc[mask, 'summary'] = summary_text

    # Make sure all required columns are present
    required_cols = ['post_id', 'post_raw', 'comment_id', 'comment_raw', 'created_date', 'platform']
    for col in required_cols:
        if col not in df_output.columns:
            if col == 'post_id':
                df_output[col] = df_output.index.map(lambda x: f"post_{x+1}")
            elif col == 'comment_id':
                df_output[col] = df_output.index.map(lambda x: f"comment_{x+1}")
            elif col == 'created_date':
                df_output[col] = pd.Timestamp.now().strftime("%d-%m-%Y")
            elif col == 'platform':
                # Detect platform from filename
                filename = input_file.name.lower()
                if 'facebook' in filename:
                    df_output[col] = "Facebook"
                elif 'youtube' in filename:
                    df_output[col] = "YouTube"
                elif 'reddit' in filename:
                    df_output[col] = "Reddit"
                elif 'tiktok' in filename:
                    df_output[col] = "TikTok"
                elif 'threads' in filename:
                    df_output[col] = "Threads"
                else:
                    df_output[col] = "Unknown"
            else:
                df_output[col] = ""

    # Reorder columns according to the desired format
    desired_order = ['post_id', 'post_raw', 'summary', 'comment_id', 'comment_raw', 'created_date', 'platform']

    # Add 'label' column if it exists
    if 'label' in df_output.columns:
        desired_order.append('label')

    # Only keep columns that actually exist in the DataFrame
    available_columns = [col for col in desired_order if col in df_output.columns]
    df_output = df_output[available_columns]
    
    # Generate output filename - use 'summarized' not 'analyzed'
    file_stem = input_file.stem
    if not file_stem.endswith('_summarized'):
        file_stem += '_summarized'
    
    output_file = config.get_path(version, "summarized", filename=f"{file_stem}.xlsx")
    txt_file = config.get_path(version, "summarized", filename=f"{file_stem}_comparison.txt")
    
    # Ensure output directories exist
    output_file.parent.mkdir(parents=True, exist_ok=True)
    txt_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Save results
    try:
        df_output.to_excel(output_file, index=False)
        print(f"✅ Đã lưu Excel: {output_file}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu Excel: {e}")
        return None
    
    try:
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(txt_content))
        print(f"✅ Đã lưu TXT: {txt_file}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu TXT: {e}")
    
    # Stats for this file
    elapsed_time = time.time() - start_time
    success_count = sum(1 for summary in all_summaries.values() 
                       if isinstance(summary, str) and 
                       summary and 
                       "Không thể" not in summary and
                       "JSON lỗi" not in summary and
                       "bị chặn" not in summary)
    
    print(f"\n📊 Kết quả file {input_file.name}:")
    print(f"   ✅ Thành công: {success_count}/{len(unique_posts)}")
    print(f"   ❌ Thất bại: {len(unique_posts) - success_count}/{len(unique_posts)}")
    print(f"   ⏱️  Thời gian xử lý: {elapsed_time/60:.2f} phút")
    
    return {
        'file': input_file.name,
        'success': success_count,
        'total': len(unique_posts),
        'time': elapsed_time
    }

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

def choose_model():
    """Interactive function to choose model"""
    print("\n🤖 CHỌN MODEL")
    print("-" * 40)
    
    # First, try to list available models
    available_models = list_available_models()
    
    if available_models:
        print(f"\nCác model có sẵn:")
        for i, model in enumerate(available_models):
            print(f"  {i+1}. {model}")
        
        while True:
            try:
                choice = input(f"\nChọn model (1-{len(available_models)}): ").strip()
                model_idx = int(choice) - 1
                if 0 <= model_idx < len(available_models):
                    selected_model = available_models[model_idx]
                    print(f"✅ Đã chọn: {selected_model}")
                    return selected_model
                else:
                    print("Lựa chọn không hợp lệ!")
            except ValueError:
                print("Vui lòng nhập số!")
    else:
        # Fallback to predefined choices
        print("⚠️ Không thể lấy danh sách model, sử dụng lựa chọn mặc định:")
        print("  1. gemini-2.0-flash (faster, recommended)")
        print("  2. gemini-2.5-flash-preview-05-20 (higher quality)")
        
        model_choice = input("Select model (1 or 2): ").strip()
        if model_choice == "2":
            return "gemini-2.5-flash-preview-05-20"
        else:
            return "gemini-2.0-flash"

def main(version, source_type=None, target_files=None, process_all=False):
    """Main function - Analyze posts with improved prompt"""
    # Check environment first
    if not check_environment():
        print("\n🚨 Environment check failed! Please fix the issues above.")
        return
    
    print("🔧 INITIALIZING API MANAGEMENT")
    print("-" * 40)
    
    # Choose model interactively
    model_name = choose_model()
    
    # Initialize API manager
    api_manager = APIKeyManager(API_KEYS, model_name)
    
    # Choose source and files if not provided
    if source_type is None or target_files is None:
        source_type, target_files = choose_source_and_files(version)
        if source_type is None:
            return
    
    print(f"\n🚀 GEMINI {model_name.upper()} - IMPROVED CONTENT ANALYSIS")
    print("=" * 70)
    print(f"Version: {version}")
    print(f"Source: {source_type}")
    print(f"Files to process: {len(target_files)}")
    for file in target_files:
        print(f"  - {file.name}")
    
    print(f"\n📝 Phân tích theo 3 mục:")
    print(f"   1. Nội dung sơ lược")
    print(f"   2. Vấn đề")
    print(f"   3. Phản động/tin giả (có/không và giải thích)")
    
    # Confirm to proceed
    proceed = input(f"\n🤔 Tiếp tục xử lý {len(target_files)} file(s)? (y/n): ").lower().strip()
    if proceed != 'y':
        print("❌ Đã hủy")
        return
    
    # Process all files
    results = []
    total_start_time = time.time()
    
    for i, file in enumerate(target_files):
        print(f"\n{'='*70}")
        print(f"FILE {i+1}/{len(target_files)}: {file.name}")
        print(f"{'='*70}")
        
        result = process_single_file(api_manager, file, version, model_name)
        if result:
            results.append(result)
    
    # Final summary
    total_elapsed = time.time() - total_start_time
    final_stats = api_manager.get_usage_stats()
    
    print(f"\n🎉 HOÀN THÀNH TẤT CẢ!")
    print("=" * 70)
    print(f"📊 Tổng kết:")
    print(f"   📁 Đã xử lý: {len(results)}/{len(target_files)} file(s)")
    print(f"   ⏱️  Tổng thời gian: {total_elapsed/60:.2f} phút")
    
    if results:
        total_success = sum(r['success'] for r in results)
        total_posts = sum(r['total'] for r in results)
        success_rate = (total_success / total_posts * 100) if total_posts > 0 else 0
        
        print(f"   ✅ Tổng posts thành công: {total_success}/{total_posts}")
        print(f"   📈 Tỷ lệ thành công: {success_rate:.1f}%")
    
    print(f"\n📈 Final API Usage:")
    for key, stats in final_stats.items():
        print(f"   {key}: {stats['requests_today']}/{stats['daily_limit']} requests today")
    
    if results:
        output_dir = config.get_path(version, "summarized").parent
        print(f"\n📁 Kết quả được lưu trong: {output_dir}")
    
    print(f"\n✅ Step 5 completed successfully.")
    print(f"\n✨ Processing complete!")

if __name__ == "__main__":
    args = parse_args()
    
    if args.version:
        version = args.version
    else:
        version = input("Enter version (e.g., v1, v2): ").strip()
    
    if not version:
        print("❌ Version is required!")
        exit(1)
    
    main(version, args.source, None, args.all)