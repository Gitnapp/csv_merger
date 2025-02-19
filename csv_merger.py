import os
import pandas as pd
import chardet
import logging
from pathlib import Path

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def detect_encoding(file_path):
    try:
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
        return result['encoding']
    except Exception as e:
        logging.error(f"Error detecting encoding for {file_path}: {e}")
        return None

def get_file_path(input_dir, file_name):
    return Path(input_dir) / file_name

def merge_csv_files(input_dir, output_dir, output_filename='merged.csv'):
    # 确保输出目录存在，如果不存在则创建
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 获取输入目录下的所有csv文件
    input_dir = Path(input_dir)
    csv_files = list(input_dir.glob('*.csv'))

    # 初始化一个空的DataFrame来存储合并后的数据
    merged_df = pd.DataFrame()

    # 读取并合并所有的csv文件
    for csv_file in csv_files:
        file_path = get_file_path(input_dir, csv_file)
        try:
            # 检测文件编码
            encoding = detect_encoding(file_path)
            print(f"Detected encoding for {csv_file}: {encoding}")

            # 读取文件
            df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
            merged_df = pd.concat([merged_df, df])
        except pd.errors.ParserError as e:
            print(f"Error reading file {csv_file}: {e}")
        except UnicodeDecodeError as e:
            print(f"Encoding error in file {csv_file}: {e}")

    # 去除重复行
    merged_df.drop_duplicates(inplace=True)

    # 定义通用排除关键词列表
    universal_exclude_keyword = [
        'toy', 'amazon', 'seller', 'review', 
        'artist', 'illustrator', 'draw', 'fat',
        'ñ', 'á', 'é', 'í', 'ó', 'ú', 'ü',
        'ب', 'ت', 'ث', 'ج', 'ح', 'خ', 'د', 'ذ', 'ر', 'ز', 'س', 'ش', 'ص', 'ض', 'ط', 'ظ', 'ع', 'غ', 'ف', 'ق', 'ك', 'ل', 'م', 'ن', 'و', 'ي',
        'あ', 'い', 'う', 'え', 'か', 'き', 'く', 'け', 'こ', 'さ', 'し', 'す', 'せ', 'そ', 'た', 'ち', 'つ', 'て', 'と', 'な', 'に', 'ぬ', 'ね', 'の', 'は', 'ひ', 'ふ', 'へ', 'ほ', 'ま', 'み', 'む', 'め', 'も', 'や', 'ゆ', 'よ', 'ら', 'り', 'る', 'れ', 'ろ','わ', 'を', 'ん',
        'ア', 'イ', 'ウ', 'エ', 'オ', 'カ', 'キ', 'ク', 'ケ', 'コ', 'サ', 'シ', 'ス', 'セ', 'ソ', 'タ', 'チ', 'ツ', 'テ', 'ト', 'ナ', 'ニ', 'ヌ', 'ネ', 'ハ', 'ヒ', 'フ', 'ヘ', 'ホ', 'マ', 'ミ', 'ム', 'メ', 'モ', 'ヤ', 'ユ', 'ヨ', 'ラ', 'リ', 'ル', 'レ', 'ロ', 'ワ', 'ヲ', 'ン',
        'ㄱ', 'ㄴ', 'ㄷ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅅ', 'ㅇ', 'ㅈ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ', 'ㅏ', 'ㅑ', 'ㅓ', 'ㅕ', 'ㅗ', 'ㅛ', 'ㅜ', 'ㅠ', 'ㅡ', 'ㅣ'
    ]

    # 定义具体的排除关键词列表
    exclusion_keywords_name_username = universal_exclude_keyword + []
    exclusion_keywords_bio = universal_exclude_keyword + []
    exclusion_keywords_location = universal_exclude_keyword + [
        'mexico', 'méxico', 'mx', 'fr', 'spain', 'España', 'á', 'é', 'í', 
        'ó', 'ú', 'ü', 'United Kingdom', 'UK', 'England', 'Netherlands', 
        'NL', 'china', 'New Zealand', 'Portugal', 'ç', 'Germany', 'Brazil', 
        'Poland', 'Japan', 'Belgium', 'Denmark', 'Australia', 'Brasil', 'Ireland', 
        'chile', 'dubai', 'pakistan'
    ]

    # 筛选出符合条件的行
    def filter_row(row):
        # 将所有文本列内容转换为小写形式
        name = str(row['Name']).lower()
        username = str(row['Username']).lower()
        bio = str(row['Bio']).lower()
        location = str(row['Location']).lower()
        
        # 检查 Name, Username, Bio, Location 列中是否包含任何排除关键词
        for kw in exclusion_keywords_name_username:
            if kw.lower() in name or kw.lower() in username:
                return False
                
        for kw in exclusion_keywords_bio:
            if kw.lower() in bio:
                return False
                
        for kw in exclusion_keywords_location:
            if kw.lower() in location:
                return False
                
        return True

    # 应用过滤函数
    filtered_df = merged_df[merged_df.apply(filter_row, axis=1)]

    # 应用额外的条件
    filtered_df = filtered_df[
        (filtered_df["Tweet Count"].astype(int) > 20) &
        (filtered_df["Follower Count"].astype(int) > 500) &
        (filtered_df["Follower Count"].astype(int) < 100000) &
        (filtered_df["Media Count"].astype(int) > 18) &
        (filtered_df["Can DM"] == 'Yes')
    ]

    #日期过滤
    filtered_df["Created At"] = pd.to_datetime(
        filtered_df["Created At"], 
        format='%a %b %d %H:%M:%S %z %Y',  # 根据实际日期格式修改
        utc=True, 
        errors='coerce'
    )

    # 重新格式化 "Created At" 列为字符串，保持原始格式
    filtered_df["Created At"] = filtered_df["Created At"].dt.strftime('%a %b %d %H:%M:%S %z %Y')

    # 创建当前日期并转换为 UTC
    current_date_utc = pd.to_datetime("now", utc=True)
    
    # 进行日期比较（使用 pd.to_datetime 转换）
    created_at_dt = pd.to_datetime(filtered_df["Created At"], format='%a %b %d %H:%M:%S %z %Y', utc=True)

    filtered_df = filtered_df[
        (created_at_dt < (current_date_utc - pd.DateOffset(months=3)))
    ]

    # 检查输出文件是否存在，如果存在则增加后缀
    output_path = os.path.join(output_dir, output_filename)
    counter = 1
    while os.path.exists(output_path):
        output_path = os.path.join(output_dir, f"{output_filename.split('.')[0]}_{counter}.csv")
        counter += 1

    # 保存合并后的文件
    filtered_df.to_csv(output_path, index=False, encoding='utf_8_sig')
    print(f"Merged CSV saved to {output_path}")

if __name__ == "__main__":
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, 'py_tomerge')
    output_dir = os.path.join(script_dir, 'py_mergedcsv')

    # 调用合并函数
    merge_csv_files(input_dir, output_dir)
