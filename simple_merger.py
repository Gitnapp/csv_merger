import pandas as pd
import argparse
import glob
import os
import chardet

def detect_encoding(file_path):
    """
    使用chardet检测文件编码
    """
    with open(file_path, 'rb') as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        return result['encoding']

def merge_csv_files(input_dir, output_dir, output_filename='merged.csv', original=False):
    """
    合并CSV文件
    
    参数:
    input_dir: 输入文件夹路径
    output_dir: 输出文件夹路径
    output_filename: 输出文件名
    original: 是否保持原始数据（不去重）
    """
    try:
        # 创建输出目录（如果不存在）
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 获取所有CSV文件
        all_files = glob.glob(os.path.join(input_dir, '*.csv'))
        
        if not all_files:
            print("未找到匹配的CSV文件")
            return
        
        # 读取所有CSV文件
        df_list = []
        for file in all_files:
            try:
                # 检测文件编码
                encoding = detect_encoding(file)
                print(f"文件 {file} 的编码格式为: {encoding}")
                
                # 使用检测到的编码读取文件
                df = pd.read_csv(file, encoding=encoding)
                df_list.append(df)
                print(f"成功读取文件: {file}")
            except Exception as e:
                print(f"读取文件 {file} 时出错: {str(e)}")
                continue
        
        if not df_list:
            print("没有成功读取任何CSV文件")
            return
        
        # 合并所有数据框
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # 如果不是original模式，则去重
        if not original:
            original_length = len(combined_df)
            # combined_df = combined_df.drop_duplicates()

            ### drop_duplication_start
            if not df_list:
                print("没有成功读取任何CSV文件")
                return
            
            # 合并所有数据框
            combined_df = pd.concat(df_list, ignore_index=True)
            
            # 如果不是original模式，则去重
            if not original:
                duplicate_columns = ['review_id']  # 在这里指定你要去重的列名
                
                try:
                    original_length = len(combined_df)
                    combined_df = combined_df.drop_duplicates(subset=duplicate_columns, keep='first')
                    print(f"根据列 {duplicate_columns} 去重后从 {original_length} 行减少到 {len(combined_df)} 行")
                except KeyError:
                    print(f"指定的列名 {duplicate_columns} 未找到，跳过去重步骤")
            
            # 确定输出文件路径（自动重命名）
            output_path = os.path.join(output_dir, output_filename)
            counter = 1
            while os.path.exists(output_path):
                base_name, extension = os.path.splitext(output_filename)
                output_path = os.path.join(output_dir, f"{base_name}_{counter}{extension}")
                counter += 1
            ### drop_duplication_end
        
        # 确定输出文件路径（自动重命名）
        output_path = os.path.join(output_dir, output_filename)
        counter = 1
        while os.path.exists(output_path):
            base_name, extension = os.path.splitext(output_filename)
            output_path = os.path.join(output_dir, f"{base_name}_{counter}{extension}")
            counter += 1
        
        # 保存合并后的文件，使用utf-8-sig编码
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"合并完成，已保存到: {output_path}")
        
    except Exception as e:
        print(f"处理过程中出错: {str(e)}")

def main():
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(script_dir, 'py_tomerge')
    output_dir = os.path.join(script_dir, 'py_mergedcsv')
    
    # 创建参数解析器
    parser = argparse.ArgumentParser(
        description='CSV文件合并工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  %(prog)s                 # 合并CSV文件并去重
  %(prog)s -original      # 合并CSV文件但不去重

说明:
  - 输入文件夹: ./py_tomerge/
  - 输出文件夹: ./py_mergedcsv/
  - 自动检测文件编码
  - 输出文件使用UTF-8-SIG编码
  - 自动处理文件名冲突
'''
    )
    
    parser.add_argument('-original', 
                       action='store_true', 
                       help='保持原始数据（不去重）')
    
    # 解析参数
    args = parser.parse_args()
    
    # 执行合并
    merge_csv_files(input_dir, output_dir, original=args.original)

if __name__ == '__main__':
    main()
