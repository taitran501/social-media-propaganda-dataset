import pandas as pd
import os
import re
import argparse
import unicodedata
import sys
from pathlib import Path
from datetime import datetime

# Điều chỉnh đường dẫn import
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Thêm thư mục cha vào path để import config
from utils.file_utils import save_excel_file
import config

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Clean data for social media analysis')
    parser.add_argument('--version', '-v', help='Version to process (e.g., v1, v2)')
    
    # Thêm các tùy chọn mới
    parser.add_argument('--source', '-s', choices=['output', 'merge', 'platform_split'],
                        default='output', help='Source directory to read data from')
    parser.add_argument('--input-file', '-i', 
                        help='Specific input file name (relative to source directory)')
    parser.add_argument('--output-file', '-o', 
                        help='Output file name (without directory)')
    parser.add_argument('--target', '-t', choices=['output', 'merge', 'platform_split', 'pre_summarize'],
                        default='output', help='Target directory to save results')
    
    return parser.parse_args()

def clean_data(version, source='output', input_filename=None, target='output', output_filename=None):
    """Đọc file từ nguồn được chỉ định và thực hiện cleaning"""
    
    # Lấy đường dẫn cho version
    paths = config.get_version_paths(version)
    
    # Xác định đường dẫn nguồn dựa trên tham số source
    source_dir = paths.get(f"{source}_dir", paths['output_dir'])
    if source == 'platform_split':
        source_dir = paths['raw_dir'].parent / "platform_split"
    
    # Xác định file input
    if input_filename:
        input_file = source_dir / input_filename
    else:
        if source == 'output':
            input_file = source_dir / "merged_raw.xlsx"
        else:
            # Nếu không chỉ định file cụ thể, tìm tất cả file Excel để xử lý
            excel_files = list(source_dir.glob("*.xlsx"))
            if not excel_files:
                print(f"Không tìm thấy file Excel nào trong {source_dir}")
                return None
            
            if len(excel_files) > 1:
                print(f"Tìm thấy nhiều file Excel trong {source_dir}. Vui lòng chỉ định file cụ thể.")
                for i, file in enumerate(excel_files):
                    print(f"{i+1}. {file.name}")
                choice = input("Chọn file (số thứ tự hoặc 'all' để xử lý tất cả): ")
                
                if choice.lower() == 'all':
                    # Xử lý tất cả các file
                    results = []
                    for file in excel_files:
                        print(f"\n----- Đang xử lý {file.name} -----")
                        result = clean_single_file(file, version, target, None)
                        if result is not None:
                            results.append(result)
                    return results
                else:
                    try:
                        idx = int(choice) - 1
                        input_file = excel_files[idx]
                    except:
                        print("Lựa chọn không hợp lệ.")
                        return None
            else:
                input_file = excel_files[0]
    
    # Xác định đường dẫn đích và tên file output
    target_dir = paths.get(f"{target}_dir", paths['output_dir'])
    if target == 'platform_split':
        target_dir = paths['raw_dir'].parent / "platform_split"
    elif target == 'pre_summarize':
        target_dir = paths['output_dir'] / "pre_summarize"
    
    # Đảm bảo thư mục đích tồn tại
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Xác định tên file output
    if not output_filename:
        base_name = input_file.stem
        if "_cleaned" not in base_name:
            base_name += "_cleaned"
        output_file = target_dir / f"{base_name}.xlsx"
    else:
        output_file = target_dir / output_filename
    
    # Xử lý file
    return clean_single_file(input_file, version, target, output_file)
    
def filter_long_comments(df, max_words=300, save_filtered=True, output_dir=None):
    """
    Lọc bỏ các comment quá dài và thống kê
    
    Args:
        df: DataFrame chứa dữ liệu
        max_words: Số từ tối đa cho phép (default: 300)
        save_filtered: Có lưu file backup các comment dài không
        output_dir: Thư mục để lưu backup file
    
    Returns:
        tuple: (filtered_df, removed_df)
    """
    print(f"\n{'='*50}")
    print(f"BƯỚC LỌC COMMENT DÀI (>{max_words} từ)")
    print(f"{'='*50}")
    
    if 'comment_raw' not in df.columns:
        print("Không tìm thấy cột 'comment_raw', bỏ qua bước lọc comment dài")
        return df, pd.DataFrame()
    
    # Tính số từ cho comment_raw
    print("Đang tính số từ cho tất cả comments...")
    df['word_count_temp'] = df['comment_raw'].apply(count_words)
    
    # Thống kê trước khi lọc
    total_comments = len(df)
    long_comments_mask = df['word_count_temp'] > max_words
    long_comments = df[long_comments_mask].copy()
    long_count = len(long_comments)
    
    print(f"\nTHỐNG KÊ COMMENT DÀI:")
    print(f"  - Tổng comments: {total_comments:,}")
    print(f"  - Comments dài (>{max_words} từ): {long_count:,} ({long_count/total_comments*100:.2f}%)")
    print(f"  - Comments giữ lại: {total_comments - long_count:,} ({(total_comments - long_count)/total_comments*100:.2f}%)")
    
    if long_count > 0:
        print(f"\nPHÂN TÍCH COMMENTS DÀI:")
        print(f"  - Từ ngắn nhất trong nhóm dài: {long_comments['word_count_temp'].min()}")
        print(f"  - Từ dài nhất: {long_comments['word_count_temp'].max()}")
        print(f"  - Trung bình từ trong nhóm dài: {long_comments['word_count_temp'].mean():.1f}")
        print(f"  - Median từ trong nhóm dài: {long_comments['word_count_temp'].median():.1f}")
        
        # Hiển thị một vài ví dụ comment dài nhất
        print(f"\nVÍ DỤ COMMENTS DÀI NHẤT (top 3):")
        top_long = long_comments.nlargest(3, 'word_count_temp')
        for i, (idx, row) in enumerate(top_long.iterrows()):
            word_count = row['word_count_temp']
            comment_preview = str(row['comment_raw'])[:150] + "..." if len(str(row['comment_raw'])) > 150 else str(row['comment_raw'])
            print(f"  {i+1}. {word_count} từ: {comment_preview}")
        
        # Lưu các comment dài để kiểm tra
        if save_filtered and output_dir:
            try:
                long_comments_file = output_dir / f"long_comments_removed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                # Thêm cột lý do xóa
                long_comments['removal_reason'] = f'Comment longer than {max_words} words'
                long_comments[['comment_raw', 'word_count_temp', 'removal_reason']].to_excel(long_comments_file, index=False)
                print(f"  - Đã lưu {long_count} comments dài vào: {long_comments_file}")
            except Exception as e:
                print(f"  - Không thể lưu file backup: {e}")
    
    # Lọc bỏ comments dài
    filtered_df = df[~long_comments_mask].copy()
    
    # Xóa cột word_count tạm thời
    filtered_df = filtered_df.drop('word_count_temp', axis=1)
    long_comments = long_comments.drop('word_count_temp', axis=1) if long_count > 0 else pd.DataFrame()
    
    print(f"\nĐÃ LỌC: Còn lại {len(filtered_df):,} comments để xử lý")
    
    return filtered_df, long_comments

def balance_comments_advanced(df, max_comments_per_post=1000):
    """
    Cân bằng số lượng comment cho mỗi bài post với logic mới:
    1. Tìm comments có từ khóa nhạy cảm
    2. Cho phép user chọn số lượng random từ comments có từ khóa
    3. Random sampling các comment còn lại để đạt max_comments_per_post
    """
    print(f"Đang cân bằng số lượng comment (chỉ xử lý posts có > {max_comments_per_post} comments)...")
    
    # Kiểm tra xem DataFrame có các cột cần thiết không
    required_cols = ['post_id', 'comment_raw']
    if not all(col in df.columns for col in required_cols):
        print("Không tìm thấy cột cần thiết để cân bằng comment")
        return df, None
    
    # DANH SÁCH TỪ KHÓA NHẠY CẢM
    preserve_keywords = [
        "phản động", "phản quốc", "phản bội", "đảng cướp", 
        "ba que", "việt cộng", "bò đỏ", "tàu cộng", "đồ đĩ",
        "cộng sản", "cộng phỉ", "xứ vẹm", "barwhere",
        "độc tài", "đàn áp", "nhân quyền",
        "xhcn", "dcs", "dcsvn", "vnch", "Hồ Tặc", "hochochet", "redbull",
        "vndcch", "cs", "csvn", "vn", "bọn chệt", "tàu khựa", "v+", "+san"
    ]
    
    # Tạo pattern để tìm từ khóa nhạy cảm
    pattern = '|'.join([re.escape(keyword) for keyword in preserve_keywords])
    
    # Thêm cột đánh dấu comment có từ khóa nhạy cảm
    df['has_preserve_keywords'] = df['comment_raw'].astype(str).str.contains(
        pattern, na=False, regex=True, case=False
    )
    
    # Thống kê trước khi cân bằng
    post_counts = df['post_id'].value_counts()
    total_comments = len(df)
    posts_over_limit = sum(post_counts > max_comments_per_post)
    total_preserve_keywords = df['has_preserve_keywords'].sum()
    
    print(f"Tổng số comment ban đầu: {total_comments}")
    print(f"Comments có từ khóa nhạy cảm: {total_preserve_keywords} ({total_preserve_keywords/total_comments*100:.1f}%)")
    print(f"Số bài post có > {max_comments_per_post} comment: {posts_over_limit}")
    
    if posts_over_limit == 0:
        print("Không có bài post nào vượt quá giới hạn comment, giữ nguyên dataset")
        df = df.drop('has_preserve_keywords', axis=1)
        return df, None
    
    # HỎI USER VỀ CHIẾN LƯỢC LẤY COMMENTS CÓ TỪ KHÓA
    print(f"\nCHIẾN LƯỢC LẤY COMMENTS CÓ TỪ KHÓA NHẠY CẢM:")
    print(f"  1. Lấy tất cả comments có từ khóa (như trước)")
    print(f"  2. Chỉ lấy một số lượng random từ comments có từ khóa")
    
    strategy_choice = input("Chọn chiến lược (1 hoặc 2): ").strip()
    
    max_priority_comments = None
    if strategy_choice == "2":
        max_priority_input = input("Nhập số lượng tối đa comments có từ khóa muốn giữ mỗi post (VD: 50): ").strip()
        try:
            max_priority_comments = int(max_priority_input)
            max_priority_comments = max(1, max_priority_comments)  # Tối thiểu 1
            print(f"Sẽ chỉ lấy tối đa {max_priority_comments} comments có từ khóa mỗi post")
        except ValueError:
            print("Giá trị không hợp lệ, sử dụng chiến lược 1 (lấy tất cả)")
            strategy_choice = "1"
    
    # Xử lý từng post riêng biệt
    balanced_dfs = []
    removed_comments_df = []
    total_removed = 0
    posts_processed = 0
    posts_unchanged = 0
    
    for post_id, post_df in df.groupby('post_id'):
        comment_count = len(post_df)
        
        # CHỈ XỬ LÝ CÁC POST CÓ > max_comments_per_post COMMENTS
        if comment_count <= max_comments_per_post:
            balanced_dfs.append(post_df)
            posts_unchanged += 1
            continue
        
        posts_processed += 1
        
        # Phân loại comments
        priority_comments = post_df[post_df['has_preserve_keywords']].copy()
        normal_comments = post_df[~post_df['has_preserve_keywords']].copy()
        
        priority_count = len(priority_comments)
        normal_count = len(normal_comments)
        
        print(f"  Post {post_id}: {comment_count} comments ({priority_count} có từ khóa + {normal_count} thường) → cần giảm xuống {max_comments_per_post}")
        
        # Xác định số lượng priority comments sẽ giữ
        if strategy_choice == "2" and max_priority_comments and priority_count > max_priority_comments:
            # Random select từ priority comments
            selected_priority = priority_comments.sample(max_priority_comments, random_state=42)
            removed_priority = priority_comments.drop(selected_priority.index)
            priority_count_keep = max_priority_comments
            print(f"    → Chỉ giữ {max_priority_comments}/{priority_count} comments có từ khóa (random)")
        else:
            # Giữ tất cả priority comments
            selected_priority = priority_comments.copy()
            removed_priority = pd.DataFrame()
            priority_count_keep = priority_count
            print(f"    → Giữ tất cả {priority_count} comments có từ khóa")
        
        # Tính toán remaining slots cho normal comments
        remaining_slots = max_comments_per_post - priority_count_keep
        
        if remaining_slots <= 0:
            # Không còn chỗ cho normal comments
            kept_comments = selected_priority
            removed_normal = normal_comments
            print(f"    → Không còn chỗ cho comments thường, loại bỏ tất cả {normal_count} comments thường")
        elif remaining_slots >= normal_count:
            # Đủ chỗ cho tất cả normal comments
            kept_comments = pd.concat([selected_priority, normal_comments])
            removed_normal = pd.DataFrame()
            print(f"    → Đủ chỗ cho tất cả {normal_count} comments thường")
        else:
            # Phải random normal comments
            kept_normal = normal_comments.sample(remaining_slots, random_state=42)
            kept_comments = pd.concat([selected_priority, kept_normal])
            removed_normal = normal_comments.drop(kept_normal.index)
            print(f"    → Giữ {remaining_slots}/{normal_count} comments thường (random)")
        
        # Combine removed comments
        removed_comments = pd.concat([removed_priority, removed_normal]) if len(removed_priority) > 0 or len(removed_normal) > 0 else pd.DataFrame()
        
        # Thêm lý do xóa cho removed comments
        if len(removed_comments) > 0:
            removed_comments = removed_comments.copy()
            removed_comments['post_id_removed'] = post_id
            removed_comments_df.append(removed_comments)
        
        balanced_dfs.append(kept_comments)
        total_removed += len(removed_comments)
        
        print(f"    → Kết quả: Giữ {len(kept_comments)}, loại bỏ {len(removed_comments)} comments")
    
    # Kết hợp tất cả lại
    balanced_df = pd.concat(balanced_dfs, ignore_index=True)
    balanced_df = balanced_df.drop('has_preserve_keywords', axis=1)
    
    # Tạo DataFrame cho removed comments
    removed_df_combined = None
    if removed_comments_df:
        removed_df_combined = pd.concat(removed_comments_df, ignore_index=True)
        removed_df_combined = removed_df_combined.drop('has_preserve_keywords', axis=1)
    
    # Thống kê sau khi cân bằng
    final_post_counts = balanced_df['post_id'].value_counts()
    print(f"\nKết quả cân bằng:")
    print(f"  - Số posts được xử lý: {posts_processed}")
    print(f"  - Số posts giữ nguyên (≤{max_comments_per_post} comments): {posts_unchanged}")
    print(f"  - Đã loại bỏ {total_removed} comments ({total_removed/total_comments*100:.1f}%)")
    print(f"  - Dataset mới: {len(balanced_df)} comments")
    print(f"  - Trung bình comments/post: {final_post_counts.mean():.1f}")
    print(f"  - Max comments/post: {final_post_counts.max()}")
    print(f"  - Min comments/post: {final_post_counts.min()}")
    
    return balanced_df, removed_df_combined

def clean_single_file(input_file, version, target, output_file=None):
    """Xử lý một file đơn lẻ"""
    print(f"Version: {version}")
    print(f"Input file: {input_file}")
    
    if output_file is None:
        paths = config.get_version_paths(version)
        target_dir = paths.get(f"{target}_dir", paths['output_dir'])
        if target == 'platform_split':
            target_dir = paths['raw_dir'].parent / "platform_split"
        elif target == 'pre_summarize':
            target_dir = paths['output_dir'] / "pre_summarize"
            
        # Đảm bảo thư mục đích tồn tại
        target_dir.mkdir(parents=True, exist_ok=True)
        
        base_name = input_file.stem
        if "_cleaned" not in base_name:
            base_name += "_cleaned"
        output_file = target_dir / f"{base_name}.xlsx"
    
    print(f"Output file: {output_file}")
    
    # Kiểm tra file input có tồn tại không
    if not input_file.exists():
        print(f"Lỗi: Không tìm thấy file input: {input_file}")
        return None
    
    print(f"Đang đọc dữ liệu từ: {input_file}")
    
    try:
        # Đọc file Excel
        df = pd.read_excel(input_file)
        print(f"Đã đọc {len(df)} dòng dữ liệu")
        
        initial_rows = len(df)
        all_removed_records = []  # List để lưu tất cả records bị xóa
        
        # ===== BƯỚC 1: LỌC COMMENT DÀI TRƯỚC =====
        print("\n" + "="*60)
        print("BƯỚC 1: LỌC COMMENT QUÁ DÀI")
        print("="*60)
        
        # Hỏi người dùng về ngưỡng từ
        max_words_input = input(f"Nhập số từ tối đa cho comment (mặc định 300, Enter để bỏ qua): ").strip()
        
        if max_words_input.lower() in ['', 'skip', 'bỏ qua']:
            print("Bỏ qua bước lọc comment dài")
            long_removed_df = pd.DataFrame()
        else:
            try:
                max_words = int(max_words_input) if max_words_input else 300
                max_words = max(50, max_words)  # Tối thiểu 50 từ
                print(f"Sử dụng ngưỡng: {max_words} từ")
                
                df, long_removed_df = filter_long_comments(
                    df, 
                    max_words=max_words, 
                    save_filtered=True,
                    output_dir=output_file.parent
                )
                
                if len(long_removed_df) > 0:
                    all_removed_records.append(long_removed_df)
                    
            except ValueError:
                print("Giá trị không hợp lệ, bỏ qua bước lọc comment dài")
                long_removed_df = pd.DataFrame()
        
        # ===== BƯỚC 2: CLEANING CONTENT =====
        print("\n" + "="*60)
        print("BƯỚC 2: CLEANING NỘI DUNG COMMENT")
        print("="*60)
        
        # CHẠY CHỈ CLEANING CHO comment_raw (KHÔNG CLEAN post_raw)
        print("Đang thực hiện cleaning cho comment_raw...")
        
        if 'comment_raw' in df.columns:
            print("  - Cleaning cột comment_raw...")
            df['comment_raw'] = df['comment_raw'].apply(minimal_clean)
        else:
            print("  - Không tìm thấy cột comment_raw")
            return None
        
        # ===== BƯỚC 3: LỌC COMMENT NGẮN =====
        print("\n" + "="*60)
        print("BƯỚC 3: LỌC COMMENT QUÁ NGẮN")
        print("="*60)
        
        # Lọc các dòng có comment_raw có số từ <= 3
        print("Lọc các dòng comment có số từ <= 3...")
        rows_before = len(df)
        
        # Tạo list các index cần giữ lại
        keep_indices = []
        removed_indices = []  # Lưu các index bị xóa
        
        for idx, row in df.iterrows():
            comment = row.get('comment_raw', '')
            if isinstance(comment, str):
                # Đếm số từ trong comment sau khi làm sạch
                word_count = count_words(comment)
                if word_count > 3 or is_special_pattern(comment):
                    keep_indices.append(idx)
                else:
                    removed_indices.append(idx)
            else:
                removed_indices.append(idx)
        
        # Tạo DataFrame chứa các record bị xóa để backup
        short_removed_df = df.loc[removed_indices].copy() if removed_indices else pd.DataFrame()
        
        # Thêm thông tin chi tiết về lý do xóa
        if len(short_removed_df) > 0:
            short_removed_df['removal_reason'] = short_removed_df.apply(
                lambda row: get_removal_reason(row.get('comment_raw', '')), axis=1
            )
            all_removed_records.append(short_removed_df)
        
        # Lọc DataFrame theo indices
        df = df.loc[keep_indices] if keep_indices else pd.DataFrame()
        
        # Reset index sau khi lọc
        df = df.reset_index(drop=True)
        
        filtered_rows = rows_before - len(df)
        print(f"Đã lọc bỏ {filtered_rows} dòng có số từ <= 3 ({filtered_rows/rows_before*100:.1f}%)")
        
        # ===== BƯỚC 4: CÂN BẰNG COMMENTS CHO MỖI POST =====
        print("\n" + "="*60)
        print("BƯỚC 4: CÂN BẰNG COMMENTS CHO MỖI POST")
        print("="*60)
        
        # Kiểm tra xem có cột post_id không
        if 'post_id' in df.columns:
            # Hỏi người dùng về giới hạn comments per post
            max_comments_input = input(f"Nhập số comment tối đa mỗi post (mặc định 1000, Enter để bỏ qua): ").strip()
            
            if max_comments_input.lower() in ['', 'skip', 'bỏ qua']:
                print("Bỏ qua bước cân bằng comments")
                balance_removed_df = pd.DataFrame()
            else:
                try:
                    max_comments_per_post = int(max_comments_input) if max_comments_input else 1000
                    max_comments_per_post = max(100, max_comments_per_post)  # Tối thiểu 100
                    
                    df, balance_removed_df = balance_comments_advanced(df, max_comments_per_post=max_comments_per_post)
                    
                    # Thêm balance_removed_df vào removed_df tổng
                    if balance_removed_df is not None and len(balance_removed_df) > 0:
                        balance_removed_df['removal_reason'] = f'Balanced - exceeded max {max_comments_per_post} comments per post'
                        all_removed_records.append(balance_removed_df)
                        
                except ValueError:
                    print("Giá trị không hợp lệ, bỏ qua bước cân bằng comments")
                    balance_removed_df = pd.DataFrame()
        else:
            print("Không tìm thấy cột post_id, bỏ qua bước cân bằng comments")
            balance_removed_df = pd.DataFrame()
        
        # ===== TỔNG HỢP VÀ LƯU KẾT QUẢ =====
        print("\n" + "="*60)
        print("TỔNG HỢP KẾT QUẢ")
        print("="*60)
        
        # Tạo file backup cho tất cả records bị xóa
        if all_removed_records:
            combined_removed_df = pd.concat(all_removed_records, ignore_index=True)
            
            if len(combined_removed_df) > 0:
                backup_file = output_file.parent / f"{output_file.stem}_removed_records.xlsx"
                print(f"\nĐang tạo file backup cho {len(combined_removed_df)} record bị xóa: {backup_file}")
                save_excel_file(combined_removed_df, backup_file)
                print(f"File backup đã được lưu: {backup_file}")
                
                # In thống kê chi tiết về lý do xóa
                print("\nThống kê lý do xóa:")
                reason_counts = combined_removed_df['removal_reason'].value_counts()
                for reason, count in reason_counts.items():
                    print(f"  - {reason}: {count:,} records")
        
        # Lưu kết quả
        print(f"\nĐang lưu kết quả vào: {output_file}")
        save_excel_file(df, output_file)
        
        print(f"\nCleaning done!")
        print(f"Tổng số dòng ban đầu: {initial_rows:,}")
        print(f"Tổng số dòng sau khi cleaning: {len(df):,}")
        total_removed = sum(len(removed_df) for removed_df in all_removed_records) if all_removed_records else 0
        print(f"Tổng số record bị xóa: {total_removed:,} ({total_removed/initial_rows*100:.1f}%)")
        
        # Hiển thị số lượng dữ liệu theo platform
        if 'platform' in df.columns:
            platform_counts = df['platform'].value_counts()
            print("\nPhân bố theo platform sau cleaning:")
            for platform, count in platform_counts.items():
                print(f"  - {platform}: {count:,} dòng ({count/len(df)*100:.1f}%)")
        
        # Hiển thị thống kê comments per post
        if 'post_id' in df.columns and 'post_raw' in df.columns:
            # Thống kê theo unique post_raw (số bài post thực tế)
            unique_posts = df['post_raw'].nunique()
            print(f"\nThống kê posts sau cleaning:")
            print(f"  - Số lượng unique posts (post_raw): {unique_posts:,}")
            print(f"  - Tổng số comments: {len(df):,}")
            print(f"  - Trung bình comments/post: {len(df)/unique_posts:.1f}")
            
            # Đếm comments cho mỗi unique post_raw
            post_raw_counts = df['post_raw'].value_counts()
            print(f"  - Median comments/post: {post_raw_counts.median():.1f}")
            print(f"  - Max comments/post: {post_raw_counts.max():,}")
            print(f"  - Min comments/post: {post_raw_counts.min():,}")
            
            # Kiểm tra consistency giữa post_raw và post_id
            unique_post_ids = df['post_id'].nunique()
            if unique_posts != unique_post_ids:
                print(f"  Có sự khác biệt giữa unique post_raw ({unique_posts}) và post_id ({unique_post_ids})!")
            
            # Phân bố số comment per unique post_raw
            print(f"\nPhân bố số comments per unique post:")
            ranges = [(1, 10), (11, 50), (51, 100), (101, 500), (501, 1000), (1001, float('inf'))]
            for min_val, max_val in ranges:
                if max_val == float('inf'):
                    count = sum((post_raw_counts > min_val))
                    label = f">{min_val}"
                else:
                    count = sum((post_raw_counts >= min_val) & (post_raw_counts <= max_val))
                    label = f"{min_val}-{max_val}"
                percentage = count / len(post_raw_counts) * 100
                print(f"  - {label} comments: {count:,} posts ({percentage:.1f}%)")
        
        elif 'post_raw' in df.columns:
            # Nếu chỉ có post_raw mà không có post_id
            unique_posts = df['post_raw'].nunique()
            print(f"\nThống kê posts sau cleaning:")
            print(f"  - Tổng số unique posts (post_raw): {unique_posts:,}")
            print(f"  - Tổng số comments: {len(df):,}")
            print(f"  - Trung bình comments/post: {len(df)/unique_posts:.1f}")
            
            # Đếm comments cho mỗi unique post_raw
            post_raw_counts = df['post_raw'].value_counts()
            print(f"  - Median comments/post: {post_raw_counts.median():.1f}")
            print(f"  - Max comments/post: {post_raw_counts.max():,}")
            print(f"  - Min comments/post: {post_raw_counts.min():,}")
        
        return df
        
    except Exception as e:
        print(f"Lỗi khi xử lý file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def count_words(text):
    """Đếm số từ trong văn bản"""
    if not isinstance(text, str):
        return 0
    
    # Loại bỏ khoảng trắng thừa và tách thành các từ
    words = text.strip().split()
    
    # Lọc bỏ các "từ" chỉ chứa dấu câu hoặc ký tự đặc biệt
    meaningful_words = []
    for word in words:
        # Kiểm tra xem từ có chứa ít nhất một chữ cái hoặc số không
        if re.search(r'[a-zA-ZÀ-ỹ0-9]', word):
            meaningful_words.append(word)
    
    return len(meaningful_words)

def get_removal_reason(comment):
    """Xác định lý do xóa record"""
    if not isinstance(comment, str):
        return "Empty or invalid comment"
    
    word_count = count_words(comment)
    
    if word_count == 0:
        return "Empty comment after cleaning"
    elif word_count == 1:
        return "Single word comment"
    elif word_count == 2:
        return "Two words comment"
    elif word_count == 3:
        return "Three words comment"
    else:
        return "Other reason"

def is_special_pattern(text):
    """Check if text contains special patterns that should be kept despite word count"""
    if not isinstance(text, str):
        return False
    
    # Giữ lại các pattern đặc biệt và từ khóa có ý nghĩa
    special_patterns = [
        # Các ký hiệu đặc biệt
        "///", "3/", "3///", "3//", "3|||", "\\\\","\\|/",
        
        # Các từ viết tắt chính trị quan trọng
        "cs", "csvn", "dcsvn", "xhcn", "dcs", "vc", "vnch", "vndcch", 
        "redbull", "bò đỏ", "ba que", "ba sọc", "việt cộng", "vn cộng",
        "phản động", "đảng trị", "barwhere", "cộng sản", "cộng phỉ", 
        "tàu cộng", "tàu khựa", "bọn chệt"
    ]
    
    text_lower = text.lower().strip()
    return any(pattern in text_lower for pattern in special_patterns)

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
    3. Lowercase 
    """
    if not isinstance(text, str):
        return ""
    
    # Chuẩn hóa Unicode
    text = unicodedata.normalize('NFC', text)
    
    # Loại bỏ URLs - cải thiện để bắt cả domain trơn như facebook.com
    text = re.sub(r'https?://\S+|www\.\S+|\S+\.(com|org|net|co|vn|io)(/\S*)?', '', text)
    
    # Loại bỏ các MEDIA+N.GIPHY.COM, VD: MEDIA1.GIPHY.COM, MEDIA2.GIPHY.COM
    text = re.sub(r'media\d*\.giphy\.com', '', text)

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

if __name__ == "__main__":
    try:
        print("="*60)
        print("STARTING DATA CLEANING PROCESS")
        print("="*60)
        
        args = parse_args()
        print(f"Arguments received:")
        print(f"  - Version: {args.version}")
        print(f"  - Source: {args.source}")
        print(f"  - Input file: {args.input_file}")
        print(f"  - Output file: {args.output_file}")
        print(f"  - Target: {args.target}")
        
        if not args.version:
            print("ERROR: Version argument is required!")
            sys.exit(1)
        
        print(f"\nStarting cleaning process...")
        result = clean_data(
            version=args.version,
            source=args.source,
            input_filename=args.input_file,
            target=args.target,
            output_filename=args.output_file
        )
        
        if result is None:
            print("ERROR: Cleaning process failed!")
            sys.exit(1)
        else:
            print("\n" + "="*60)
            print("DATA CLEANING COMPLETED SUCCESSFULLY!")
            print("="*60)
            
    except Exception as e:
        print(f"\nFATAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)