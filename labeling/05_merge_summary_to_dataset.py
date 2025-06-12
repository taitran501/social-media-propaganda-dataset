import pandas as pd
import re

def extract_summaries_from_txt(file_path):
    """Trích xuất các summary từ file txt"""
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Tìm tất cả các post và summary của chúng
    pattern = r"POST (\d+):\s*-+\s*ORIGINAL:\s*(.*?)\s*SUMMARY:\s*(.*?)\s*={80}"
    matches = re.findall(pattern, content, re.DOTALL)
    
    summaries = {}
    for match in matches:
        post_num = int(match[0])
        original_text = match[1].strip()
        summary_text = match[2].strip()
        summaries[original_text] = summary_text
    
    return summaries

def update_excel_with_summaries(excel_path, output_path, summaries):
    """Cập nhật file Excel với các summary và đổi tên cột"""
    # Đọc file Excel
    df = pd.read_excel(excel_path)
    
    print(f"Cột hiện tại: {list(df.columns)}")
    print(f"Số dòng: {len(df)}")
    
    # Đổi tên cột theo yêu cầu
    column_mapping = {
        'orig_idx': 'id',
        'post_raw': 'post',
        'comment_raw': 'comment'
        # label, platform, created_date giữ nguyên
    }
    
    df = df.rename(columns=column_mapping)
    
    # Đổi giá trị label
    label_mapping = {
        1: 'PHAN_DONG',
        2: 'KHONG_PHAN_DONG',
        3: 'KHONG_LIEN_QUAN'
    }
    
    print(f"Giá trị label hiện tại: {df['label'].value_counts()}")
    df['label'] = df['label'].map(label_mapping)
    print(f"Giá trị label sau khi đổi: {df['label'].value_counts()}")
    
    # Thêm cột summary và map với post
    df['summary'] = df['post'].map(summaries)
    
    # Sắp xếp lại thứ tự cột theo yêu cầu: id, post, summary, comment, label, platform, created_date
    desired_order = ['id', 'post', 'summary', 'comment', 'label', 'platform', 'created_date']
    
    # Kiểm tra và sắp xếp cột
    available_columns = [col for col in desired_order if col in df.columns]
    df = df[available_columns]
    
    print(f"Cột sau khi đổi tên: {list(df.columns)}")
    
    # Lưu lại file Excel đã cập nhật
    df.to_excel(output_path, index=False)
    
    return df

# Đường dẫn đến file
txt_file = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\dataset\posts_comparison.txt"
excel_file = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\dataset\dataset_labeled.xlsx"
output_file = r"C:\Users\trant\Documents\Test\scrape\preprocessing\merged_all_platform\test_clean\dataset\dataset_with_summaries.xlsx"

try:
    # Trích xuất summaries từ file txt
    summaries = extract_summaries_from_txt(txt_file)
    print(f"Đã trích xuất {len(summaries)} summaries từ file txt")
    
    # Cập nhật file Excel và lưu ra file mới
    updated_df = update_excel_with_summaries(excel_file, output_file, summaries)
    
    # Thống kê kết quả
    summary_count = updated_df['summary'].notna().sum()
    print(f"\nKết quả:")
    print(f"- Đã đổi tên cột: orig_idx → id, post_raw → post, comment_raw → comment")
    print(f"- Đã đổi giá trị label: 1 → PHAN_DONG, 2 → KHONG_PHAN_DONG, 3 → KHONG_LIEN_QUAN")
    print(f"- Đã thêm cột summary")
    print(f"- Đã map được {summary_count}/{len(updated_df)} summaries")
    print(f"- Thứ tự cột: {list(updated_df.columns)}")
    print(f"- File đã được lưu tại: {output_file}")
    
except FileNotFoundError as e:
    print(f"Không tìm thấy file: {e}")
except Exception as e:
    print(f"Lỗi: {str(e)}")