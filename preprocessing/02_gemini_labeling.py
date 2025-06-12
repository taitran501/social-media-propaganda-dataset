# Bổ sung imports cần thiết
import itertools
import time
import re
import os
import pandas as pd
import numpy as np
import google.generativeai as genai
from tqdm import tqdm
import math
import json
import unicodedata
from collections import defaultdict

from dotenv import load_dotenv
import itertools

load_dotenv()  # Đọc file .env

# Lấy API keys từ biến môi trường, tách theo dấu ;
api_keys_str = os.getenv("GEMINI_API_KEYS", "")
API_KEYS = [key.strip() for key in api_keys_str.split(";") if key.strip()]

# Tạo vòng xoay key
key_cycle = itertools.cycle(API_KEYS)


# Đảm bảo thư mục output tồn tại
os.makedirs("output", exist_ok=True)

# ---- Enhanced regex patterns with more comprehensive mapping ----
ANTI_TARGET = r"\b(ba\s*que|3\s*que|vện|vẹm|vện\s*vàng|vện\s*vong|cali|đu\s*càng|phản\s*động|phản\s*quốc|vọng\s*nô|vọng\s*ngoại|bọn\s*lật\s*đổ|vnch|cờ\s*vàng|ba\s*sọc)\b"
NEG_WORD = r"\b(chó|đồ|thằng|lũ|bọn|ngu|súc\s*vật|rác|mất\s*dạy|sủa|đần|óc\s*chó|cặn\s*bã|mất\s*nước|đụ|đéo|cayyy|khốn|điên|dở|hại|ác)\b"
GOV_NEG = r"\b(csvn|cs\s*vn|đảng\s*cs|đảng\s*chó|lũ\s*cộng|tham\s*nhũng|cướp\s*đất|băng\s*đảng|bọn\s*cầm\s*quyền|độc\s*tài|đảng\s*cướp|bò\s*đỏ|đbrr|hút\s*máu|cộng\s*sản\s*thối)\b"
GOV_POS = r"\b(đảng\s*sáng\s*suốt|bác\s*hồ|hồ\s*chí\s*minh|yêu\s*nước|độc\s*lập|tự\s*do|hạnh\s*phúc|tự\s*hào|vinh\s*dự|quân\s*đội|đoàn\s*kết)\b"
ANTI_POS = r"\b(tự\s*do|dân\s*chủ|nhân\s*quyền|đa\s*đảng|chống\s*độc\s*tài|vnch\s*muôn\s*năm|cờ\s*vàng|phục\s*quốc)\b"
IDEOLOGY_NEG = r"\b(chủ\s*nghĩa\s*cộng\s*sản|cộng\s*sản\s*chủ\s*nghĩa|mác\s*lê|chủ\s*nghĩa\s*xã\s*hội)\b.{0,30}\b(bịp\s*bợm|lừa\s*đảo|thất\s*bại|sai\s*lầm|độc\s*tài)\b"

POLITICAL_TERMS = {
    "quốc hận": {"context": "ANTI_TARGET", "meaning": "Term used by VNCH supporters to refer to April 30, 1975"},
    "giải phóng miền nam": {"context": "GOV_TARGET", "meaning": "Official term for the 1975 reunification events"},
    # Add many more terms with clear political alignments
}

def set_next_key():
    """Chuyển sang API key tiếp theo trong vòng xoay"""
    api_key = next(key_cycle)
    genai.configure(api_key=api_key)
    return api_key

# Cấu hình key đầu tiên
set_next_key()
MODEL = "gemini-2.0-flash"  # free tier, 1 M TPM · 15 RPM

# Hàm ước tính token chính xác hơn
def estimate_tokens(text: str) -> int:
    """Ước tính token (≈ 4 ký tự tiếng Việt / 1 token)"""
    if not isinstance(text, str):
        return 0
    return max(1, len(text) // 4)

# Fuzzy pattern matching cho slang/teen-code
def fuzzy_match(text, patterns, max_distance=1):
    """
    Fuzzy matching đơn giản cho biến thể slang/teen-code
    Sử dụng optional character groups trong regex để bắt các biến thể phổ biến
    """
    if not isinstance(text, str):
        return False
        
    # Normalize text to improve matching
    text = unicodedata.normalize('NFC', text.lower())
    
    # Create pattern variants with optional spaces and characters
    fuzzy_patterns = []
    for pattern in patterns:
        # Add optional spaces between characters
        spaced_pattern = ''.join([c + '\\s*' for c in pattern.strip()]).rstrip('\\s*')
        # Allow 1-2 character variations (e.g., "bac ky" → "bắc kỳ")
        fuzzy_patterns.append(spaced_pattern)
        
    # Combine patterns into single regex
    combined_pattern = '|'.join([f"\\b({p})\\b" for p in fuzzy_patterns])
    
    # Check if any pattern matches
    return bool(re.search(combined_pattern, text))

def compress_post(text: str, limit_tok=600) -> str:
    """
    Nén post để giới hạn token:
    1. Nếu ≤ limit_tok: giữ nguyên
    2. Nếu ≤ 4000: giữ phần đầu + phần cuối
    3. Nếu > 4000: gọi Gemini để tóm tắt
    """
    n = estimate_tokens(text)
    if n <= limit_tok:               # giữ nguyên
        return text
    elif n <= 4000:
        head = text[:1600]           # ~400 tok
        tail = text[-800:]           # ~200 tok
        return f"{head} … {tail}"
    else:
        # Tóm tắt cực ngắn ≤ 60 tok bằng Gemini
        try:
            current_key = set_next_key()
            print(f"Tóm tắt post dài {n} tokens với key {current_key[-5:]}")
            
            # Giới hạn input chỉ còn ~2000 token thay vì 4000 như trước
            summ_prompt = f"Tóm tắt ngắn gọn trong 60 từ: ```{text[:8000]}```"
            
            rsp = genai.GenerativeModel(MODEL).generate_content(
                summ_prompt, generation_config={"temperature": 0})
            
            summary = rsp.text.strip()
            
            # Log token usage
            try:
                meta = rsp.usage_metadata
                prompt_tokens = meta.prompt_token_count
                output_tokens = getattr(meta, "candidates_token_count", 0)
                print(f"  → Tokens: {prompt_tokens} in, {output_tokens} out")
            except AttributeError:
                pass  # Bỏ qua nếu field chưa hỗ trợ
            
            print(f"  → Đã tóm tắt thành: {summary[:50]}...")
            time.sleep(0.6)  # Tăng sleep để tránh rate limit
            return summary
        except Exception as e:
            print(f"Lỗi khi tóm tắt post: {str(e)}")
            # Fallback: cắt đầu + cuối
            head = text[:1200]
            tail = text[-600:]
            return f"{head} ... {tail}"

# Enhanced stance prompt with historical context
STANCE_PROMPT_ENHANCED = """
VIETNAMESE POLITICAL CONTEXT:
The Socialist Republic of Vietnam operates under the leadership of the Communist Party of Vietnam. The correct political line follows Marxism-Leninism and Ho Chi Minh Thought.

TASK: Analyze this social media post and determine its political stance toward the Vietnamese government.

POST:
\"\"\"
{post_text}
\"\"\"

Answer with ONLY ONE of these letters:
A if the post criticizes or attacks the Vietnamese government/Party/socialism
B if the post is neutral or unrelated to politics
C if the post supports or defends the Vietnamese government/Party/socialism

ANSWER (A, B, or C only):
"""

def classify_stance_enhanced(post_short: str, max_retry=3) -> str:
    """
    Phân loại stance của post với prompt nâng cao
    Trả về: POST_ANTI, POST_NEUTRAL, hoặc POST_PRO
    """
    last_time = time.time()
    for attempt in range(max_retry):
        try:
            # Rate limiting - đảm bảo ít nhất 0.6s giữa các request
            while time.time() - last_time < 0.6:
                time.sleep(0.01)
            
            current_key = set_next_key()
            print(f"Phân loại stance với key {current_key[-5:]}")
            
            rsp = genai.GenerativeModel(MODEL).generate_content(
                STANCE_PROMPT_ENHANCED.format(post_text=post_short),
                generation_config={"temperature": 0})
            
            # Log token usage
            try:
                meta = rsp.usage_metadata
                prompt_tokens = meta.prompt_token_count
                output_tokens = getattr(meta, "candidates_token_count", 0)
                print(f"  → Tokens: {prompt_tokens} in, {output_tokens} out")
            except AttributeError:
                pass
            
            letter = rsp.text.strip()[:1].upper()
            stance = {"A": "POST_ANTI", "B": "POST_NEUTRAL", "C": "POST_PRO"}.get(letter, "POST_NEUTRAL")
            
            print(f"  → Stance: {stance}")
            last_time = time.time()
            return stance
        except Exception as e:
            print(f"Lỗi khi phân loại stance (lần {attempt+1}): {str(e)}")
            time.sleep(1)
            last_time = time.time()
    
    # Fallback sau khi thử lại nhiều lần
    print("  → Fallback về POST_NEUTRAL do lỗi")
    return "POST_NEUTRAL"

def analyze_post_context_enhanced(post_text):
    """Phân tích ngữ cảnh của bài đăng với prompt toàn diện hơn"""
    post_short = compress_post(post_text, 1000)
    
    prompt = f"""
    VIETNAMESE POLITICAL CONTEXT:
    The Socialist Republic of Vietnam operates under the leadership of the Communist Party of Vietnam. The correct political line follows Marxism-Leninism and Ho Chi Minh Thought.
    
    HISTORICAL CONTEXT:
    - The August Revolution (1945) established Vietnamese independence
    - Vietnam's struggle against foreign imperialism was legitimate
    - The reunification in 1975 represents national liberation
    - "Three reactionary types" (ba que) refers to anti-government elements
    
    Analyze this social media post and provide a comprehensive assessment:
    
    POST:
    \"\"\"
    {post_short}
    \"\"\"
    
    Return ONLY JSON format with these fields:
    {{
        "stance": "POST_ANTI, POST_NEUTRAL, or POST_PRO",
        "topic": "Choose ONE from: TQ, 30-4, VNCH, Tham_nhung, Chinh_tri, Xa_hoi, Kinh_te, Khac",
        "tone": "Choose ONE from: Chui_dang, Chui_phan_dong, Ke_chuyen, Trung_lap, Ung_ho",
        "key_terms": "List 3-5 most important political keywords in the post",
        "sarcasm_risk": "low, medium, or high (likelihood post uses sarcasm/irony)"
    }}
    """
    
    try:
        current_key = set_next_key()
        print(f"Phân tích ngữ cảnh post với key {current_key[-5:]}")
        
        rsp = genai.GenerativeModel(MODEL).generate_content(
            prompt, generation_config={"temperature": 0})
        
        # Parse JSON response
        text = rsp.text
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            json_text = json_match.group(0)
            try:
                result = json.loads(json_text)
                print(f"  → Phân tích thành công: {result['stance']}, {result['topic']}, {result['tone']}")
                return result
            except json.JSONDecodeError:
                print(f"  → Lỗi parse JSON: {json_text[:100]}...")
        
        # Fallback khi không parse được JSON
        return {
            "stance": "POST_NEUTRAL",
            "topic": "Khac",
            "tone": "Trung_lap",
            "key_terms": "general terms",
            "sarcasm_risk": "low"
        }
        
    except Exception as e:
        print(f"Lỗi khi phân tích post: {str(e)}")
        return {
            "stance": "POST_NEUTRAL", 
            "topic": "Khac",
            "tone": "Trung_lap", 
            "key_terms": "general terms",
            "sarcasm_risk": "low"
        }

def check_sarcasm_enhanced(post_raw, analysis_result):
    """
    Kiểm tra mỉa mai dựa trên phân tích ngữ cảnh và regex
    """
    # Get sarcasm risk from analysis
    sarcasm_risk = analysis_result.get("sarcasm_risk", "low")
    
    # Extract stance from analysis
    stance = analysis_result.get("stance", "POST_NEUTRAL")
    
    if not isinstance(post_raw, str):
        return 0, 0
        
    post_text = post_raw.lower()
    
    # Compute score with enhanced regex patterns
    if stance == "POST_PRO":
        neg_count = len(re.findall(GOV_NEG, post_text))
        pos_count = len(re.findall(GOV_POS, post_text))
    else:  # POST_ANTI or POST_NEUTRAL 
        neg_count = len(re.findall(ANTI_TARGET, post_text)) * 0.5  # Weight lower
        pos_count = len(re.findall(ANTI_POS, post_text))
    
    # Calculate basic score
    score = neg_count - pos_count
    
    # Amplify score based on analysis
    if sarcasm_risk == "high":
        score *= 1.5
    elif sarcasm_risk == "medium":
        score *= 1.2
    
    # Check for ideological statements
    if re.search(IDEOLOGY_NEG, post_text) and stance != "POST_ANTI":
        score += 3  # Strong indicator of sarcasm if post seems pro-govt
    
    # Determine flag
    flag_sarcasm = 1 if (score >= 3 and stance == "POST_PRO") or (score <= -3 and stance == "POST_ANTI") else 0
    
    return flag_sarcasm, score

def build_comment_prompt_enhanced_v3(analysis_result, post_short, batch_df):
    """Create comprehensive prompt with historical context and target identification"""
    stance = analysis_result.get("stance", "POST_NEUTRAL")
    topic = analysis_result.get("topic", "Khac")
    tone = analysis_result.get("tone", "Trung_lap") 
    key_terms = analysis_result.get("key_terms", "general terms")
    
    header = f"""
VIETNAMESE POLITICAL CONTEXT:
The Socialist Republic of Vietnam operates under the leadership of the Communist Party of Vietnam. The correct political line follows Marxism-Leninism and Ho Chi Minh Thought.

HISTORICAL UNDERSTANDING:
- The August Revolution (1945) established Vietnamese independence
- Vietnam's struggle against foreign imperialism was legitimate
- The reunification in 1975 represents national liberation
- "Three reactionary types" (ba que) refers to anti-government elements

POST CONTEXT:
POST_STANCE = {stance}
POST_TOPIC = {topic}
POST_TONE = {tone}
KEY_TERMS = "{key_terms}"

POST:
\"\"\"
{post_short}
\"\"\"

==== THREE-STEP ANALYSIS FOR CLASSIFICATION ====

STEP 1: TARGET IDENTIFICATION
First determine WHO is being criticized or discussed in each comment:
- Party/Government/State/Socialism (GOV_TARGET)
- Anti-government groups/VNCH/Foreign opposition (ANTI_TARGET)
- Regular people/Non-political entities (OTHER_TARGET)

STEP 2: SENTIMENT ORIENTATION
Then determine the SENTIMENT toward the target:
- Negative sentiment (criticism, insults, attacks) → NEG
- Positive sentiment (praise, defense, support) → POS
- Neutral sentiment (factual, questioning) → NEU

STEP 3: IDEOLOGICAL CLASSIFICATION
Apply this classification rule:
- GOV_TARGET + NEG → PHAN_DONG
- ANTI_TARGET + NEG → KHONG_PHAN_DONG
- GOV_TARGET + POS → KHONG_PHAN_DONG
- ANTI_TARGET + POS → PHAN_DONG
- OTHER_TARGET + ANY_SENTIMENT → KHONG_LIEN_QUAN

For low-context comments (≤10 words):
- Comments supporting POST_ANTI → PHAN_DONG
- Comments supporting POST_PRO → KHONG_PHAN_DONG
- Comments criticizing anti-government elements → KHONG_PHAN_DONG
- If truly ambiguous → prioritize national security perspective

EXAMPLES OF LOW-CONTEXT COMMENTS:
1. "đúng rồi" (on anti-government post) → PHAN_DONG (supports anti-government sentiment)
2. "đúng rồi" (on pro-government post) → KHONG_PHAN_DONG (supports pro-government sentiment)
3. "chết hết đi" (on post criticizing government) → PHAN_DONG (reinforces anti-government stance)
4. "chết hết đi" (on post criticizing reactionaries) → KHONG_PHAN_DONG (targets anti-government groups)
5. "bọn cộng sản" (standalone) → PHAN_DONG (derogatory reference to communists)
6. "bọn 3 que" (standalone) → KHONG_PHAN_DONG (criticizes anti-government elements)
7. "chủ nghĩa cộng sản là một chủ nghĩa bịp bợm" → PHAN_DONG (directly attacks communist ideology)
8. "chủ nghĩa tư bản là một chủ nghĩa bịp bợm" → KHONG_PHAN_DONG (criticizes capitalism)
9. "tự do dân chủ vạn tuổi" → PHAN_DONG (opposing the socialist revolutionary line)
10. "hồ chí minh muôn năm" → KHONG_PHAN_DONG (supports revolutionary leadership)

Classify the comments below following this methodology. Return ONLY CSV format id,label:
"""
    
    lines = [
        f"{idx}|{txt.replace('|',' ')}"
        for idx, txt in zip(batch_df.index, batch_df["comment_raw"])
    ]
    
    return header + "\n".join(lines) + "\n\nReturn ONLY CSV format id,label (no explanation)."

def label_comments_enhanced_v3(batch_df, analysis_result, post_short, max_retry=3):
    """Gán nhãn cho một batch comments với prompt toàn diện"""
    prompt = build_comment_prompt_enhanced_v3(analysis_result, post_short, batch_df)
    batch_size = len(batch_df)
    last_time = time.time()
    
    for attempt in range(max_retry):
        try:
            # Rate limiting - đảm bảo ít nhất 0.6s giữa các request
            while time.time() - last_time < 0.6:
                time.sleep(0.01)
                
            current_key = set_next_key()
            print(f"Gán nhãn {batch_size} comments với key {current_key[-5:]}")
            
            rsp = genai.GenerativeModel(MODEL).generate_content(
                prompt,
                generation_config={"temperature": 0})
            
            # Log token usage
            try:
                meta = rsp.usage_metadata
                prompt_tokens = meta.prompt_token_count
                output_tokens = getattr(meta, "candidates_token_count", 0)
                print(f"  → Tokens: {prompt_tokens} in, {output_tokens} out")
            except AttributeError:
                pass
            
            csv_text = rsp.text
            print(f"  → Đã nhận {len(csv_text.splitlines())} dòng phản hồi")
            last_time = time.time()
            return csv_text
        except Exception as e:
            print(f"Lỗi khi gán nhãn comments (lần {attempt+1}): {str(e)}")
            time.sleep(2)
            last_time = time.time()
    
    # Fallback
    print("  → Không thể gán nhãn sau nhiều lần thử")
    return ""

def parse_csv(csv_text):
    """Parse output CSV từ Gemini về dict {id: label}"""
    out = {}
    if not csv_text:
        return out
        
    valid_labels = {'PHAN_DONG', 'KHONG_PHAN_DONG', 'KHONG_LIEN_QUAN'}
    
    for line in csv_text.strip().splitlines():
        if "," in line or "|" in line:
            parts = re.split("[,|]", line, 1)
            if len(parts) == 2:
                try:
                    # Đảm bảo ID là integer
                    idx = int(parts[0].strip())
                    label = parts[1].strip().upper()
                    # Chỉ lấy label hợp lệ
                    if label in valid_labels:
                        out[idx] = label
                except:
                    pass  # Bỏ qua dòng không hợp lệ
    return out

def apply_enhanced_override_rules(labels_dict, batch_df):
    """Apply sophisticated override rules based on target-sentiment detection"""
    
    # Target anti-government groups - expanded
    ANTI_TARGET = r"\b(ba\s*que|3\s*que|vện|vẹm|vện\s*vàng|vện\s*vong|cali|đu\s*càng|phản\s*động|phản\s*quốc|vọng\s*nô|vọng\s*ngoại|bọn\s*lật\s*đổ|vnch|cờ\s*vàng|ba\s*sọc)\b"
    
    # Negative/derogatory words - expanded
    NEG_WORD = r"\b(chó|đồ|thằng|lũ|bọn|ngu|súc\s*vật|rác|mất\s*dạy|sủa|đần|óc\s*chó|cặn\s*bã|mất\s*nước|đụ|đéo|cayyy|khốn|điên|dở|hại|ác)\b"
    
    # Government criticism words - expanded
    GOV_NEG = r"\b(csvn|cs\s*vn|đảng\s*cs|đảng\s*chó|lũ\s*cộng|tham\s*nhũng|cướp\s*đất|băng\s*đảng|bọn\s*cầm\s*quyền|độc\s*tài|đảng\s*cướp|bò\s*đỏ|đbrr|hút\s*máu|cộng\s*sản\s*thối)\b"
    
    # Ideological criticism pattern
    IDEOLOGY_NEG = r"\b(chủ\s*nghĩa\s*cộng\s*sản|cộng\s*sản\s*chủ\s*nghĩa|mác\s*lê|chủ\s*nghĩa\s*xã\s*hội)\b.{0,30}\b(bịp\s*bợm|lừa\s*đảo|thất\s*bại|sai\s*lầm|độc\s*tài)\b"
    
    updated_labels = labels_dict.copy()
    
    for idx in batch_df.index:
        if idx not in updated_labels:
            continue
            
        comment_text = batch_df.loc[idx, "comment_raw"]
        if not isinstance(comment_text, str):
            continue
            
        comment_lower = comment_text.lower()
        current_label = updated_labels[idx]
        
        # Rule 1: Comment attacks anti-government groups but labeled as irrelevant → KHONG_PHAN_DONG
        if (current_label == "KHONG_LIEN_QUAN" and 
            re.search(ANTI_TARGET, comment_lower) and 
            re.search(NEG_WORD, comment_lower)):
            updated_labels[idx] = "KHONG_PHAN_DONG"
            print(f"  → Fixed: Comment {idx} reclassified from KHONG_LIEN_QUAN to KHONG_PHAN_DONG")
        
        # Rule 2: Comment attacks anti-government groups but wrongly labeled as PHAN_DONG → KHONG_PHAN_DONG
        if (current_label == "PHAN_DONG" and 
            re.search(ANTI_TARGET, comment_lower) and 
            re.search(NEG_WORD, comment_lower) and
            not re.search(GOV_NEG, comment_lower)):
            updated_labels[idx] = "KHONG_PHAN_DONG"
            print(f"  → Fixed: Comment {idx} reclassified from PHAN_DONG to KHONG_PHAN_DONG")
            
        # Rule 3: Comment criticizes communist ideology but missed → PHAN_DONG
        if (current_label != "PHAN_DONG" and
            re.search(IDEOLOGY_NEG, comment_lower)):
            updated_labels[idx] = "PHAN_DONG"
            print(f"  → Fixed: Comment {idx} reclassified to PHAN_DONG due to ideological criticism")
        
        # Rule 4: Check for word order and proximity - anti-govt words followed by negation might be supportive
        # This is a more complex rule that requires parsing, just checking basic patterns here
        anti_govt_pattern = r"\b(phản\s*động|ba\s*que|3\s*que|vện|cali)\b.{0,5}\b(không|chẳng|đéo|đ|đếch)\b.{0,10}\b(phải|đúng|hay|tốt)\b"
        if (current_label != "KHONG_PHAN_DONG" and
            re.search(anti_govt_pattern, comment_lower)):
            updated_labels[idx] = "KHONG_PHAN_DONG"
            print(f"  → Fixed: Comment {idx} reclassified to KHONG_PHAN_DONG due to negation pattern")
    
    return updated_labels

def score_comment(comment_text, post_analysis):
    """
    Calculate sentiment-target scores for a comment
    Returns scores for different classes
    """
    if not isinstance(comment_text, str):
        return {"PHAN_DONG": 0, "KHONG_PHAN_DONG": 0, "KHONG_LIEN_QUAN": 1}
    
    comment_lower = comment_text.lower()
    scores = {"PHAN_DONG": 0, "KHONG_PHAN_DONG": 0, "KHONG_LIEN_QUAN": 0}
    
    # Get post stance for context
    post_stance = post_analysis.get("stance", "POST_NEUTRAL")
    
    # Base scores from regex patterns
    pd_score = 0
    kpd_score = 0
    
    # Check for direct criticism of government
    if re.search(GOV_NEG, comment_lower):
        pd_score += 5
    
    # Check for direct criticism of anti-government elements
    if re.search(ANTI_TARGET, comment_lower) and re.search(NEG_WORD, comment_lower):
        dist = 100  # Large default distance
        for anti_match in re.finditer(ANTI_TARGET, comment_lower):
            for neg_match in re.finditer(NEG_WORD, comment_lower):
                # Calculate word proximity
                curr_dist = abs(anti_match.start() - neg_match.start())
                dist = min(dist, curr_dist)
        
        # Stronger signal if words are close together
        if dist <= 20:  # Within ~4-5 words
            kpd_score += 5
        else:
            kpd_score += 2
    
    # Check for ideological statements
    if re.search(IDEOLOGY_NEG, comment_lower):
        pd_score += 7  # Very strong signal
    
    # Check for positive government sentiment
    if re.search(GOV_POS, comment_lower):
        kpd_score += 3
    
    # Check for positive anti-government sentiment
    if re.search(ANTI_POS, comment_lower):
        pd_score += 3
    
    # Context from post (low-context handling)
    if len(comment_lower.split()) <= 10:  # Short comment
        if post_stance == "POST_ANTI" and re.search(r"\b(đúng|phải|hay|ok|yes)\b", comment_lower):
            pd_score += 2  # Supporting anti-govt post
        elif post_stance == "POST_PRO" and re.search(r"\b(đúng|phải|hay|ok|yes)\b", comment_lower):
            kpd_score += 2  # Supporting pro-govt post
    
    # If both scores are low, it's likely irrelevant
    if pd_score <= 1 and kpd_score <= 1:
        scores["KHONG_LIEN_QUAN"] = 1
    else:
        scores["PHAN_DONG"] = pd_score
        scores["KHONG_PHAN_DONG"] = kpd_score
    
    return scores

def second_stage_classification(batch_df, raw_labels, post_analysis):
    """
    Apply scoring system to comments with close or ambiguous classifications
    Helps resolve edge cases not covered by first-stage classification
    """
    enhanced_labels = raw_labels.copy()
    
    for idx in batch_df.index:
        if idx not in enhanced_labels:
            continue
            
        comment_text = batch_df.loc[idx, "comment_raw"]
        current_label = enhanced_labels[idx]
        
        # Apply scoring only for potentially misclassified comments
        scores = score_comment(comment_text, post_analysis)
        max_score = max(scores.values())
        max_class = max(scores.items(), key=lambda x: x[1])[0]
        
        # If one class has a dominant score, use it
        if max_score >= 5 and current_label != max_class:
            enhanced_labels[idx] = max_class
            print(f"  → Score-based override: Comment {idx} reclassified from {current_label} to {max_class}")
            
        # Add confidence information
        confidence = max_score / (sum(scores.values()) + 0.001)  # Avoid div by 0
        batch_df.loc[idx, "confidence"] = min(confidence, 1.0)
        
        # Flag truly ambiguous cases
        if max_score < 2 and current_label != "KHONG_LIEN_QUAN":
            batch_df.loc[idx, "ambiguous"] = 1
    
    return enhanced_labels

def run_enhanced_labeling_pipeline(df):
    """Run the enhanced labeling pipeline with all improvements"""
    OUTPUT_FILE = "output/gemini2_flash_enhanced_labeled.xlsx"
    
    print(f"Xử lý {len(df)} dòng dữ liệu")
    
    # Thêm cột label và confidence
    df["label"] = ""
    df["confidence"] = 0.0
    df["ambiguous"] = 0
    
    # Số post duy nhất
    unique_posts = df["post_raw"].unique()
    print(f"Tổng số bài post cần phân loại: {len(unique_posts)}")
    
    # Xử lý từng post và các comment của nó
    for post_raw in tqdm(unique_posts, desc="Đang phân loại các post"):
        # Lấy tất cả comment thuộc post này
        sub_df = df[df["post_raw"] == post_raw].copy()
        
        # Nén post để tiết kiệm token
        post_short = compress_post(post_raw)
        
        # Phân tích ngữ cảnh của post
        post_analysis = analyze_post_context_enhanced(post_raw)
        
        # Kiểm tra sarcasm
        flag_sarcasm, sarcasm_score = check_sarcasm_enhanced(post_raw, post_analysis)
        
        # Xử lý sarcasm nếu phát hiện
        if flag_sarcasm:
            original_stance = post_analysis['stance']
            if original_stance == "POST_ANTI":
                post_analysis['stance'] = "POST_PRO"
                print(f"  → Phát hiện mỉa mai (score: {sarcasm_score}), đổi stance thành POST_PRO")
            elif original_stance == "POST_PRO":
                post_analysis['stance'] = "POST_ANTI"
                print(f"  → Phát hiện mỉa mai (score: {sarcasm_score}), đổi stance thành POST_ANTI")
        
        # Chia các comment thành các batch nhỏ (35 comment/batch)
        # Giảm batch size để tránh quá token limit với prompt lớn hơn
        batch_size = 35
        for start in range(0, len(sub_df), batch_size):
            batch_df = sub_df.iloc[start:start+batch_size]
            
            # Gán nhãn cho batch comments với prompt được cải thiện
            csv_text = label_comments_enhanced_v3(batch_df, post_analysis, post_short)
            
            # Parse kết quả
            raw_labels = parse_csv(csv_text)
            
            # Phân loại giai đoạn 2 - dựa trên hệ thống điểm và ngữ cảnh
            labels = second_stage_classification(batch_df, raw_labels, post_analysis)
            
            # Áp dụng quy tắc hậu kiểm cải tiến
            labels = apply_enhanced_override_rules(labels, batch_df)
            
            # Xử lý comments không nhận được label (missing)
            missing_ids = set(batch_df.index) - set(labels.keys())
            if missing_ids:
                print(f"  → {len(missing_ids)} comment không được gán nhãn, đặt mặc định KHONG_LIEN_QUAN")
                for idx in missing_ids:
                    labels[idx] = "KHONG_LIEN_QUAN"
                    batch_df.loc[idx, "confidence"] = 0.3  # Low confidence for default labels
            
            # Cập nhật DataFrame với các nhãn nhận được
            for idx, label in labels.items():
                df.loc[idx, "label"] = label
                # Confidence đã được cập nhật trong second_stage_classification
    
    # Lưu index gốc nếu cần
    df.index.name = "orig_idx"
    
    # Lưu kết quả
    df.to_excel(OUTPUT_FILE, index=True)
    print(f"\n✅ Đã lưu {len(df)} dòng đã gán nhãn vào: {OUTPUT_FILE}")
    
    # Thống kê nhãn
    label_counts = df["label"].value_counts()
    print("\nPhân bố nhãn:")
    for label, count in label_counts.items():
        print(f"  - {label}: {count} ({count/len(df)*100:.1f}%)")
    
    # Thống kê các comment đánh dấu ambiguous
    ambiguous_count = df["ambiguous"].sum()
    print(f"\nSố comment được đánh dấu ambiguous: {ambiguous_count} ({ambiguous_count/len(df)*100:.1f}%)")
    
    return df

def enrich_comment_context(comment_text, post_text):
    """Add contextual information based on recognized political terms"""
    enriched_text = comment_text
    for term, info in POLITICAL_TERMS.items():
        if term in comment_text.lower():
            # Flag this term's presence and its alignment
            if info["context"] == "ANTI_TARGET" and re.search(r"(khóc|cay|đau|buồn)", comment_text.lower()):
                # Criticizing those mourning = KHONG_PHAN_DONG
                return comment_text, "LIKELY_KHONG_PHAN_DONG"
    return enriched_text, None

def verify_sensitive_labels(labels_dict, batch_df, post_analysis):
    """Second pass verification for politically sensitive comments"""
    updated_labels = labels_dict.copy()
    
    for idx in batch_df.index:
        if idx not in updated_labels:
            continue
            
        comment_text = batch_df.loc[idx, "comment_raw"]
        # Check for special cases that frequently get mislabeled
        enriched, suggestion = enrich_comment_context(comment_text, batch_df.loc[idx, "post_raw"])
        if suggestion and updated_labels[idx] != suggestion.replace("LIKELY_", ""):
            print(f"Correction: {comment_text} → {suggestion}")
            updated_labels[idx] = suggestion.replace("LIKELY_", "")
    
    return updated_labels

# Add this function for demo analysis
def demo_post_analysis(post_text, df=None, num_comments=10):
    """Demo phân tích và gán nhãn cho một bài đăng với comment thật
    
    Args:
        post_text: Nội dung bài đăng cần phân tích
        df: DataFrame chứa dữ liệu comments
        num_comments: Số lượng comment muốn gán nhãn (mặc định: 10)
    """
    if df is None:
        # Đọc file excel nếu DataFrame chưa được cung cấp
        try:
            input_file = "output/merged_minimal_cleaned.xlsx"
            df = pd.read_excel(input_file)
            print(f"Đã đọc {len(df)} dòng từ {input_file}")
        except Exception as e:
            print(f"Lỗi khi đọc file excel: {e}")
            return
    
    print("\n==== DEMO PHÂN TÍCH BÀI ĐĂNG ====")
    print(f"Bài đăng: {post_text[:100]}...")
    
    # Nén post để tiết kiệm token
    post_short = compress_post(post_text)
    
    # Phân tích ngữ cảnh của post
    post_analysis = analyze_post_context_enhanced(post_text)
    
    # Kiểm tra sarcasm
    flag_sarcasm, sarcasm_score = check_sarcasm_enhanced(post_text, post_analysis)
    
    # Hiển thị kết quả phân tích
    print("\nKết quả phân tích:")
    print(f"- Stance: {post_analysis['stance']}")
    print(f"- Topic: {post_analysis['topic']}")
    print(f"- Tone: {post_analysis['tone']}")
    print(f"- Key terms: {post_analysis['key_terms']}")
    print(f"- Sarcasm risk: {post_analysis['sarcasm_risk']}")
    print(f"- Sarcasm score: {sarcasm_score}")
    
    # Nếu có dấu hiệu mỉa mai
    if flag_sarcasm:
        original_stance = post_analysis['stance']
        flipped_stance = "POST_ANTI" if original_stance == "POST_PRO" else "POST_PRO"
        print(f"\n⚠️ CẢNH BÁO: Bài đăng có dấu hiệu mỉa mai!")
        print(f"  → Đề xuất xem xét đổi stance thành: {flipped_stance}")
    
    # Yêu cầu người dùng xác nhận stance cuối
    print("\nBạn muốn sử dụng stance nào cho gán nhãn comment?")
    print(f"1. Giữ nguyên: {post_analysis['stance']}")
    if flag_sarcasm:
        print(f"2. Đổi thành: {flipped_stance}")
    
    choice = input("Chọn (1 hoặc 2): ").strip()
    
    # Xử lý lựa chọn stance
    if choice == "2" and flag_sarcasm:
        post_analysis['stance'] = flipped_stance
        print(f"Đã đổi stance thành: {flipped_stance}")
    else:
        print(f"Giữ nguyên stance: {post_analysis['stance']}")
    
    # Tìm comment thực tế liên quan đến post này
    real_comments_df = df[df["post_raw"] == post_text]
    
    if len(real_comments_df) > 0:
        print(f"\nTìm thấy {len(real_comments_df)} comment thực tế cho post này")
        # Lấy số lượng comment theo người dùng chọn
        demo_df = real_comments_df.head(num_comments).copy()
        print(f"Lấy {len(demo_df)} comment để demo")
    else:
        print("\nKhông tìm thấy comment thực tế cho post này, tìm ngẫu nhiên comments từ dataset")
        # Lấy ngẫu nhiên số lượng comment theo người dùng chọn
        demo_df = df.sample(min(num_comments, len(df))).copy()
        print(f"Lấy {len(demo_df)} comment ngẫu nhiên để demo")
    
    print("\n==== DEMO GÁN NHÃN COMMENTS ====")
    print("Các comment được chọn:")
    for i, comment in enumerate(demo_df["comment_raw"]):
        print(f"{i+1}. {comment[:70]}..." if len(str(comment)) > 70 else f"{i+1}. {comment}")
    
    # Gán nhãn cho batch comments với prompt được cải thiện
    csv_text = label_comments_enhanced_v3(demo_df, post_analysis, post_short)
    
    # Parse kết quả
    raw_labels = parse_csv(csv_text)
    
    # Phân loại giai đoạn 2 - dựa trên hệ thống điểm và ngữ cảnh
    labels = second_stage_classification(demo_df, raw_labels, post_analysis)
    
    # Áp dụng quy tắc hậu kiểm cải tiến
    labels = apply_enhanced_override_rules(labels, demo_df)
    
    # Cập nhật DataFrame với các nhãn nhận được
    for idx, label in labels.items():
        demo_df.loc[idx, "label"] = label
    
    print("\nKết quả gán nhãn:")
    for idx, row in demo_df.iterrows():
        comment = row["comment_raw"]
        label = row["label"]
        confidence = row.get("confidence", 0.0)
        ambiguous = row.get("ambiguous", 0)
        
        ambiguous_tag = " (AMBIGUOUS)" if ambiguous else ""
        print(f"{idx}. {comment[:50]}... → {label} (conf: {confidence:.2f}){ambiguous_tag}")
    
    return demo_df

# Cập nhật phần main để thêm mode mới
if __name__ == "__main__":
    print("ENHANCED GEMINI LABELING PIPELINE")
    print("--------------------------------")
    
    print("Chọn chế độ:")
    print("1. Phân tích và gán nhãn toàn bộ dataset")
    print("2. Demo với bài đăng từ dataset")
    mode = input("Chọn (1 hoặc 2): ").strip()
    
    if mode == "2":
        # Đọc dữ liệu từ file
        INPUT_FILE = "output/merged_minimal_cleaned.xlsx"
        if not os.path.exists(INPUT_FILE):
            print(f"Không tìm thấy file {INPUT_FILE}!")
            exit(1)
            
        df = pd.read_excel(INPUT_FILE)
        print(f"Đã đọc {len(df)} dòng từ {INPUT_FILE}")
        
        # Lấy danh sách các post duy nhất
        unique_posts = df["post_raw"].unique()
        print(f"Tìm thấy {len(unique_posts)} post duy nhất")
        
        # Tùy chọn tìm kiếm post theo từ khóa
        search_option = input("\nBạn muốn tìm post theo từ khóa không? (y/n): ").strip().lower()
        
        selected_post = None
        if search_option == "y":
            keyword = input("Nhập từ khóa tìm kiếm: ").strip().lower()
            matching_posts = [post for post in unique_posts if keyword in str(post).lower()]
            
            if matching_posts:
                print(f"\nTìm thấy {len(matching_posts)} post chứa từ khóa '{keyword}':")
                # Hiển thị tối đa 10 post phù hợp
                for i, post in enumerate(matching_posts[:10]):
                    print(f"{i+1}. {post[:100]}...")
                    
                if len(matching_posts) > 10:
                    print(f"... và {len(matching_posts) - 10} post khác")
                    
                choice = input("\nChọn số từ 1-10 (hoặc nhập 0 để chọn ngẫu nhiên): ")
                
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(matching_posts[:10]):
                        selected_post = matching_posts[idx]
                    else:
                        # Chọn ngẫu nhiên từ các post phù hợp
                        import random
                        selected_post = random.choice(matching_posts)
                        print(f"Đã chọn post ngẫu nhiên có chứa từ khóa '{keyword}'")
                except:
                    # Chọn ngẫu nhiên nếu nhập không hợp lệ
                    import random
                    selected_post = random.choice(matching_posts)
                    print(f"Đã chọn post ngẫu nhiên có chứa từ khóa '{keyword}'")
            else:
                print(f"Không tìm thấy post nào chứa từ khóa '{keyword}'")
        
        # Nếu không tìm theo từ khóa hoặc tìm không thấy, hiển thị danh sách mặc định
        if selected_post is None:
            # Số lượng post hiển thị trong danh sách
            num_display = 10
            print(f"\nChọn một post để demo (hiển thị {num_display} post đầu tiên):")
            for i, post in enumerate(unique_posts[:num_display]):
                print(f"{i+1}. {str(post)[:100]}...")
                
            choice = input(f"\nChọn số từ 1-{num_display} (hoặc nhập số khác để chọn ngẫu nhiên): ")
            
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(unique_posts[:num_display]):
                    selected_post = unique_posts[idx]
                else:
                    # Chọn ngẫu nhiên
                    import random
                    selected_post = random.choice(unique_posts)
                    print(f"Đã chọn post ngẫu nhiên: {str(selected_post)[:100]}...")
            except:
                # Chọn ngẫu nhiên nếu nhập không hợp lệ
                import random
                selected_post = random.choice(unique_posts)
                print(f"Đã chọn post ngẫu nhiên: {str(selected_post)[:100]}...")
        
        # Chọn số lượng comment để demo
        print(f"\nBài đăng đã chọn: {str(selected_post)[:100]}...")
        num_comments = input("Nhập số lượng comment muốn gán nhãn (mặc định: 10): ").strip()
        try:
            num_comments = int(num_comments)
            if num_comments <= 0:
                num_comments = 10
        except:
            num_comments = 10
        print(f"Sẽ gán nhãn cho {num_comments} comment")
        
        # Chạy demo với post đã chọn và DataFrame đã đọc
        demo_post_analysis(selected_post, df, num_comments)
    
    else:
        # Chế độ 1: Tiếp tục với pipeline gán nhãn đầy đủ
        INPUT_FILE = "output/merged_minimal_cleaned.xlsx"
        if not os.path.exists(INPUT_FILE):
            raise FileNotFoundError(f"Không tìm thấy file {INPUT_FILE}!")
            
        df = pd.read_excel(INPUT_FILE)
        print(f"Đã đọc {len(df)} dòng từ {INPUT_FILE}")
        
        # Thực hiện phân tích token trước
        print("\n=== PHÂN TÍCH YÊU CẦU TOKEN & THỜI GIAN ===")
        
        # Ước tính tokens với prompt lớn hơn
        avg_tokens_per_post = 800  # Estimated average for enhanced post analysis
        avg_tokens_per_batch = 1600  # Estimated average for enhanced comment batch
        unique_posts = len(df["post_raw"].unique())
        total_comments = len(df)
        avg_batch_size = 35  # Reduced from 40 to account for larger prompt
        
        # Calculate estimates
        total_batches = math.ceil(total_comments / avg_batch_size)
        total_post_tokens = unique_posts * avg_tokens_per_post
        total_comment_tokens = total_batches * avg_tokens_per_batch
        total_tokens = total_post_tokens + total_comment_tokens
        
        # Calculate time estimates
        requests_per_minute = 25  # Conservative estimate with rate limiting
        est_minutes = math.ceil((unique_posts + total_batches) / requests_per_minute)
        est_hours = est_minutes / 60
        
        print(f"- Số post cần phân loại: {unique_posts}")
        print(f"- Số comment cần gán nhãn: {total_comments}")
        print(f"- Số batch (mỗi batch {avg_batch_size} comments): {total_batches}")
        print(f"- Tổng token dự kiến: {total_tokens:,} / 1,000,000 (free tier)")
        print(f"- Thời gian ước tính: {est_minutes} phút (~{est_hours:.1f} giờ)")
        
        # Hỏi người dùng có muốn tiếp tục không
        proceed = input("\nBắt đầu gán nhãn? (y/n): ").strip().lower()
        if proceed == "y":
            # Thực hiện pipeline gán nhãn cải tiến
            labeled_df = run_enhanced_labeling_pipeline(df)
        else:
            print("Đã hủy quá trình gán nhãn.")