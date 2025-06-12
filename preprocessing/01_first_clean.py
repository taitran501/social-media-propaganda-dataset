import pandas as pd
import os
import re
import unicodedata
from pathlib import Path

# Cài đặt thư mục và đường dẫn
INPUT_FOLDER = Path(r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\output")
INPUT_FILE = INPUT_FOLDER / "merged_raw.xlsx"
OUTPUT_FILE = INPUT_FOLDER / "merged_minimal_cleaned.xlsx"

def remove_emojis(text):
    """Xóa tất cả emoji khỏi văn bản"""
    if not isinstance(text, str):
        return ""
        
    try:
        # Sử dụng regex để xóa emoji
        emoji_pattern = re.compile(
            "["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F700-\U0001F77F"  # alchemical symbols
            u"\U0001F780-\U0001F7FF"  # Geometric Shapes
            u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
            u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            u"\U0001FA00-\U0001FA6F"  # Chess Symbols
            u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            u"\U00002702-\U000027B0"  # Dingbats
            u"\U000024C2-\U0001F251" 
            "]+", flags=re.UNICODE
        )
        return emoji_pattern.sub(r'', text)
    except:
        # Fallback nếu có lỗi với regex
        return text

def remove_vn_emoticons(text):
    """Xóa các icon cảm xúc kiểu Việt Nam"""
    if not isinstance(text, str):
        return text
        
    # Danh sách các pattern icon cần xóa
    vn_emoticon_patterns = [
        r':[)]+',  # Matches :)), :))), etc.
        r'=[)]+',  # Matches =)), =))), etc.
        r':\(\(',  # Matches :((
        r'=\(\(',  # Matches =((
        r':>+',    # Matches :>, :>>, etc.
        r':<+',    # Matches :<, :<<, etc.
        r':v+',    # Matches :v, :vv, etc.
        r':V+',    # Matches :V, :VV, etc.
        r'=\)+',   # Matches =), =)), etc.
        r'=\(+',   # Matches =(, =((, etc.
    ]
    
    # Áp dụng các pattern để xóa icon
    for pattern in vn_emoticon_patterns:
        text = re.sub(pattern, '', text)
    
    return text

def minimal_clean(text):
    """
    Thực hiện minimal cleaning cho text:
    1. Chuẩn hóa Unicode (UTF-8)
    2. Loại bỏ URL, tag, emoji và các chỉ báo phổ biến
    3. Chuyển về chữ thường
    """
    if not isinstance(text, str):
        return ""
    
    # Chuẩn hóa Unicode
    text = unicodedata.normalize('NFC', text)
    
    # Loại bỏ URLs - cải thiện để bắt cả domain trơn như facebook.com
    text = re.sub(r'https?://\S+|www\.\S+|\S+\.(com|org|net|co|vn|io)(/\S*)?', '', text)
    
    # Loại bỏ HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Loại bỏ mentions @username và các tham chiếu mạng xã hội
    text = re.sub(r'@[\w\._]+', '', text)
    text = re.sub(r'\(\s*ig\s+[\w\._]+\s*\)', '', text)  # (ig username)
    text = re.sub(r'\(\s*instagram\s+[\w\._]+\s*\)', '', text)  # (instagram username)
    
    # Loại bỏ emoji
    text = remove_emojis(text)
    
    # Loại bỏ icon kiểu Việt Nam
    text = remove_vn_emoticons(text)
    
    # Loại bỏ các chỉ báo phổ biến
    patterns = [
        r'(?i)\[Đã chỉnh sửa\]',
        r'(?i)\(Đã chỉnh sửa\)',
        r'(?i)Đã chỉnh sửa',
        r'(?i)See Translation',
        r'(?i)Xem bản dịch',
        r'(?i)See more',
        r'(?i)Xem thêm',
        r'(?i)Ẩn bớt',
        r'(?i)Xem ít hơn',
        r'(?i)Dịch',
        r'(?i)Translated',
        r'(?i)more',
        r'(?i)less'
    ]
    
    for pattern in patterns:
        text = re.sub(pattern, '', text)
    
    # Loại bỏ dấu câu riêng lẻ hoặc các ký tự đặc biệt đứng một mình
    text = re.sub(r'(?<!\w)[\^\'\`\~\"\,\.]+(?!\w)', ' ', text)
    
    # Làm sạch khoảng trắng
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Chuyển thành chữ thường
    text = text.lower()
    
    return text

def is_special_pattern(text):
    """Check if text contains special patterns that should be kept despite length"""
    if not isinstance(text, str):
        return False
    # Keep comments like "///" or other special patterns
    special_patterns = ["///", "---", "***", "???"]
    return any(pattern in text for pattern in special_patterns)

def clean_data():
    """Đọc file merged_raw.xlsx và thực hiện cleaning"""
    
    # Kiểm tra file input có tồn tại không
    if not INPUT_FILE.exists():
        print(f"Lỗi: Không tìm thấy file input: {INPUT_FILE}")
        print("Vui lòng chạy merge.py trước!")
        return None
    
    print(f"Đang đọc dữ liệu từ: {INPUT_FILE}")
    
    try:
        # Đọc file Excel
        df = pd.read_excel(INPUT_FILE)
        print(f"Đã đọc {len(df)} dòng dữ liệu")
        
        initial_rows = len(df)
        
        # Thực hiện minimal cleaning cho post_raw và comment_raw
        print("Đang thực hiện cleaning...")
        
        if 'post_raw' in df.columns:
            print("  - Cleaning cột post_raw...")
            df['post_raw'] = df['post_raw'].apply(minimal_clean)
        
        if 'comment_raw' in df.columns:
            print("  - Cleaning cột comment_raw...")
            df['comment_raw'] = df['comment_raw'].apply(minimal_clean)
        
        # Lọc các dòng có comment_raw độ dài <= 3 ký tự
        print("\nLọc các dòng comment có độ dài <= 3 ký tự...")
        rows_before = len(df)
        
        # Tạo list các index cần giữ lại
        keep_indices = []
        for idx, row in df.iterrows():
            comment = row.get('comment_raw', '')
            if isinstance(comment, str) and (len(comment.strip()) > 3 or is_special_pattern(comment)):
                keep_indices.append(idx)
        
        # Lọc DataFrame theo indices
        df = df.loc[keep_indices]
        
        # Reset index sau khi lọc
        df = df.reset_index(drop=True)
        
        filtered_rows = rows_before - len(df)
        print(f"Đã lọc bỏ {filtered_rows} dòng có độ dài <= 3 ký tự ({filtered_rows/rows_before*100:.1f}%)")
        
        # Lưu kết quả với điều chỉnh độ rộng cột
        print(f"\nĐang lưu kết quả vào: {OUTPUT_FILE}")
        
        # Lưu file với writer để có thể điều chỉnh định dạng
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
            
            # Lấy worksheet để điều chỉnh độ rộng cột
            worksheet = writer.sheets['Sheet1']
            
            # Điều chỉnh độ rộng cho mỗi cột
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).apply(len).max(),  # Độ dài max của dữ liệu
                    len(str(col))  # Độ dài của tên cột
                ) + 2  # Thêm padding
                
                # Đặt độ rộng cột (max 50 để tránh cột quá rộng)
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)

        print(f"Đã hoàn thành cleaning! Dữ liệu đã được lưu vào: {OUTPUT_FILE}")
        print(f"Tổng số dòng ban đầu: {initial_rows}")
        print(f"Tổng số dòng sau khi cleaning và lọc: {len(df)}")
        
        # Hiển thị số lượng dữ liệu theo platform
        if 'platform' in df.columns:
            platform_counts = df['platform'].value_counts()
            print("\nPhân bố theo platform sau cleaning:")
            for platform, count in platform_counts.items():
                print(f"  - {platform}: {count} dòng ({count/len(df)*100:.1f}%)")
        
        return df
        
    except Exception as e:
        print(f"Lỗi khi xử lý file: {str(e)}")
        return None

if __name__ == "__main__":
    print("Bắt đầu quá trình cleaning dữ liệu...")
    cleaned_data = clean_data()
    if cleaned_data is not None:
        print("Hoàn tất cleaning!")
    else:
        print("Cleaning thất bại!")