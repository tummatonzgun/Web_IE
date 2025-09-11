import pandas as pd
import os
import glob
import numpy as np
from pathlib import Path
import time
from datetime import datetime  
import tempfile
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))



# ---------- ฟังก์ชันสำหรับเรียกใช้งานในเว็บ ----------
import pandas as pd
import os
from typing import Union, List

def find_input_files(input_pattern: str):
    if os.path.isdir(input_pattern):
        files = glob.glob(os.path.join(input_pattern, "*.txt")) + \
        glob.glob(os.path.join(input_pattern, "*.TXT"))
        files = list(set(os.path.abspath(f) for f in files))
    else:
        files = glob.glob(input_pattern)
    return files

def load_and_parse_file(input_file: str) -> pd.DataFrame:
    try:
        with open(input_file, 'r', encoding='latin-1') as file:
            lines = file.readlines()
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        return pd.DataFrame()
    rows = []
    max_values_len = 0
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) < 3:
            continue
        timestamp, data_type, data_values = parts[0], parts[1], parts[2]
        try:
            date_part, time_part = timestamp.split(' ')
            time_part = time_part.replace('AM', '').replace('PM', '').strip()
        except ValueError:
            continue
        values = data_values.split(',')
        if len(values) > max_values_len:
            max_values_len = len(values)
        row = [date_part, time_part, data_type] + values
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    max_len = 6 + max_values_len
    for row in rows:
        row += [''] * (max_len - len(row))
    columns = ['date', 'time', 'step', 'frame', 'G', 'No_strip'] + [f'value_{i}' for i in range(1, max_values_len + 1)]
    df = pd.DataFrame(rows, columns=columns)
    pattern = r'(FU|FR|FA|FW|FN|FJ|F1|F2|F3|F4|F5|F6|F7|F8|F9|F0)(\w{4})'
    df['frame'] = df['frame'].astype(str)
    df['frame'] = df['frame'].str.extract(pattern).fillna('').agg(''.join, axis=1)
    return df

def extract_pro_and_speed(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df_pro = df[df['step'] == 'PRO'].copy()
    if df_pro.empty:
        return pd.DataFrame()
    df_pro['speed'] = None
    for idx in df_pro.index:
        speed_value = None
        pos = df.index.get_loc(idx)
        for j in range(pos + 1, len(df)):
            if df.loc[df.index[j], 'step'] == 'CUC':
                if 'value_5' in df.columns and len(df.columns) > df.columns.get_loc('value_5'):
                    speed_value = df.loc[df.index[j], 'value_5']
                break
        df_pro.at[idx, 'speed'] = speed_value
    df_pro['speed'] = pd.to_numeric(df_pro['speed'], errors='coerce')
    df_pro['speed'] = df_pro['speed'] / 10 / 25.4
    df_pro['speed'] = df_pro['speed'].apply(lambda x: int(x) if x % 1 == 0 else round(x, 2))
    return df_pro

def mark_errors(df: pd.DataFrame, df_pro: pd.DataFrame) -> pd.DataFrame:
    if df.empty or df_pro.empty:
        return df_pro
    df_pro['MC'] = None
    pro_indices = df.index[df['step'] == 'PRO'].tolist()
    for i in range(1, len(pro_indices)):
        current_idx = pro_indices[i]
        prev_idx = pro_indices[i - 1]
        start_idx = min(prev_idx, current_idx)
        end_idx = max(prev_idx, current_idx)
        df_slice = df.loc[start_idx:end_idx]
        error_steps = ['ERRSET', 'ERRRCV', 'ERRCLR', 'DMC', 'DMW']
        has_error = df_slice['step'].isin(error_steps).any()
        if has_error and prev_idx in df_pro.index:
            df_pro.at[prev_idx, 'MC'] = 'MC error'
    return df_pro

def insert_blank_rows(df_pro: pd.DataFrame) -> pd.DataFrame:
    if df_pro.empty:
        return df_pro
    new_rows = []
    for i in range(len(df_pro)):
        row = df_pro.iloc[i]
        new_rows.append(row)
        try:
            first_strip = float(row['No_strip']) == 1
        except (ValueError, TypeError):
            first_strip = False
        if first_strip:
            empty_row = pd.Series([None] * len(df_pro.columns), index=df_pro.columns)
            new_rows.append(empty_row)
    df_with_blank = pd.DataFrame(new_rows).reset_index(drop=True)
    return df_with_blank

def calculate_time_diff(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
    df['minute'] = df['datetime'] - df['datetime'].shift(-1)
    df['seconds'] = df['minute'].dt.total_seconds()
    df = df[df['seconds'].isna() | ((df['seconds'] >= 0) & (df['seconds'] <= 86400))]
    df['minute'] = df['minute'].astype(str)
    df.loc[df['minute'] == 'NaT', 'minute'] = ''
    df['minute'] = df['minute'].str.replace('0 days ', '')
    df = df.drop(columns=['datetime'])
    return df

def assign_subgroups_and_insert_empty_rows(df, column_strip='No_strip', frame_group='frame'):
    subgroup_id = 0
    subgroups = []
    prev_val = None
    for val in df[column_strip]:
        if pd.isna(val):
            subgroups.append(np.nan)
            prev_val = None
            continue
        if prev_val is None:
            subgroup_id += 1
        elif val > prev_val:
            subgroup_id += 1
        subgroups.append(subgroup_id)
        prev_val = val
    df['subgroup_id'] = subgroups
    result_rows = []
    subgroup_keys = df['subgroup_id'].dropna().unique()
    for group in subgroup_keys:
        group_df = df[df['subgroup_id'] == group].reset_index(drop=True)
        result_rows.append(group_df.iloc[[0]])
        for i in range(1, len(group_df)):
            prev_frame = group_df.loc[i - 1, frame_group]
            curr_frame = group_df.loc[i, frame_group]
            if prev_frame != curr_frame:
                empty_row = pd.DataFrame({col: [np.nan] for col in df.columns})
                result_rows.append(empty_row)
            result_rows.append(group_df.iloc[[i]])
        empty_row = pd.DataFrame({col: [np.nan] for col in df.columns})
        result_rows.append(empty_row)
    result_df = pd.concat(result_rows, ignore_index=True).reset_index(drop=True)
    return result_df

def mark_outlier_subgroups(df, subgroup_col='subgroup_id', no_strip_col='No_strip'):
    outlier_groups = []
    for subgroup, group_df in df.groupby(subgroup_col):
        if 1 not in group_df[no_strip_col].values:
            outlier_groups.append(subgroup)
    df['outlier_subgroup'] = df[subgroup_col].isin(outlier_groups)
    return df

def detect_outliers_combined(df, group_col='frame', value_col='seconds', no_strip_col='No_strip',
                             iqr_factor=1, zscore_threshold=2, min_diff_seconds=90):
    df['is_outlier'] = False
    df_filtered = df[~((df[no_strip_col] == 2) & (df[no_strip_col].shift(-1) == 1))]
    for group_name, group in df_filtered.groupby(group_col):
        values = group[value_col].dropna().values
        if len(values) == 0:
            continue
        median = np.median(values)
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        upper_bound = q3 + iqr_factor * iqr
        mean = np.mean(values)
        std = np.std(values)
        for idx in group.index:
            val = df.loc[idx, value_col]
            if pd.isna(val):
                continue
            if val > upper_bound and abs(val - median) > min_diff_seconds:
                df.at[idx, 'is_outlier'] = True
            elif std > 0:
                z_score = (val - mean) / std
                if z_score > zscore_threshold and abs(val - mean) > min_diff_seconds:
                    df.at[idx, 'is_outlier'] = True
    return df

def add_avg_exclude_outliers_by_frame(
    df, 
    value_col='seconds', 
    group_col='frame', 
    outlier_col='is_outlier',
    outlier_subgroup_col='outlier_subgroup',
    outlier_mc='MC'
):
    df['avg_ex_outliers'] = pd.NA
    df['count_avg'] = pd.NA
    df['count_outliers'] = pd.NA
    for frame_val in df[group_col].dropna().unique():
        group_df = df[df[group_col] == frame_val]
        good_values = group_df[(group_df[outlier_col] != True) & (group_df[outlier_subgroup_col] != True) & (group_df[outlier_mc] != 'MC error')][value_col].dropna()
        if len(good_values) < 5:
            continue
        avg_val = good_values.mean()
        count_avg_val = len(good_values)
        count_all = group_df[value_col].count()
        count_outliers = count_all - count_avg_val
        idx = df[df[group_col] == frame_val].index
        if len(idx) > 0:
            first_idx = idx[0]
            df.at[first_idx, 'avg_ex_outliers'] = round(avg_val, 2)
            df.at[first_idx, 'count_avg'] = count_avg_val
            df.at[first_idx, 'count_outliers'] = count_outliers
    return df

def summarize_by_frame(df):
    summary = df.groupby(['frame', 'speed']).agg({
        'sec/strip': 'first',
    }).reset_index()
    return summary

def process_single_file_complete(input_file: str, output_dir: str):
    print(f"กำลังประมวลผล: {input_file}")
    input_path = Path(input_file)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # สร้างชื่อไฟล์ใหม่ด้วยวันที่และเวลา
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"{input_path.stem}_{timestamp}.xlsx"
    
    try:
        df = load_and_parse_file(input_file)
        if df.empty:
            return False, f"ไม่สามารถโหลดข้อมูลจาก {input_file}"
        
        df_pro = extract_pro_and_speed(df)
        if df_pro.empty:
            return False, f"ไม่พบข้อมูล PRO ในไฟล์ {input_file}"
        
        df_pro = mark_errors(df, df_pro)
        
        # เลือกคอลัมน์ที่ต้องการ
        available_value_cols = [col for col in df_pro.columns if col.startswith('value_')]
        value_cols = available_value_cols[:1] if available_value_cols else []
        selected_cols = ['date', 'time', 'step', 'package', 'frame', 'No_strip'] + value_cols + ['speed','MC']
        existing_cols = [col for col in selected_cols if col in df_pro.columns]
        df_pro = df_pro[existing_cols]
        
        # ประมวลผลข้อมูล
        df_with_blank = insert_blank_rows(df_pro)
        df_time = calculate_time_diff(df_with_blank)
        
        # แปลงชนิดข้อมูล
        for col in ['frame', 'speed','value_1']:
            if col in df_time.columns:
                if col == 'frame':
                    df_time[col] = df_time[col].astype(str).str.strip()
                else:
                    df_time[col] = pd.to_numeric(df_time[col], errors='coerce')
        
        if 'No_strip' in df_time.columns:
            df_time['No_strip'] = pd.to_numeric(df_time['No_strip'], errors='coerce')
        
        df_filtered = df_time[df_time['frame'].notna()]
        if df_filtered.empty:
            return False, f"ไม่พบข้อมูล frame ที่ใช้งานได้ในไฟล์ {input_file}"
        
        # วิเคราะห์ข้อมูล
        df_analyzed = assign_subgroups_and_insert_empty_rows(df_filtered, 'No_strip', 'frame')
        df_analyzed = mark_outlier_subgroups(df_analyzed, 'subgroup_id', 'No_strip')
        df_analyzed = detect_outliers_combined(df_analyzed, 'frame', 'seconds', 'No_strip')
        df_analyzed = add_avg_exclude_outliers_by_frame(df_analyzed, value_col='seconds', group_col='frame')
        
        # จัดการ Error columns
        if 'outlier_subgroup' in df_analyzed.columns and 'is_outlier' in df_analyzed.columns and 'MC' in df_analyzed.columns:
            df_analyzed['Error'] = (df_analyzed['outlier_subgroup'] | df_analyzed['is_outlier'] | (df_analyzed['MC'] == 'MC error'))
        elif 'outlier_subgroup' in df_analyzed.columns and 'is_outlier' in df_analyzed.columns:
            df_analyzed['Error'] = df_analyzed['outlier_subgroup'] | df_analyzed['is_outlier']
        else:
            df_analyzed['Error'] = False
        
        # ลบคอลัมน์ที่ไม่ต้องการ
        df_analyzed.drop(columns=['outlier_subgroup', 'is_outlier','MC'], inplace=True, errors='ignore')
        df_analyzed['Error'] = df_analyzed['Error'].apply(lambda x: "MC ERROR" if x else "")
        df_analyzed.drop(columns=['subgroup_id'], inplace=True, errors='ignore')
        df_analyzed['sec/strip'] = df_analyzed['avg_ex_outliers']
        
        # สร้าง Summary
        summary = summarize_by_frame(df_analyzed)
        df_final = df_analyzed.drop(columns=['avg_ex_outliers'])
        
        # บันทึกไฟล์ Excel
        with pd.ExcelWriter(output_file) as writer:
            df_final.to_excel(writer, index=False, sheet_name='Processed_Data')
            summary.to_excel(writer, index=False, sheet_name='Summary')
        
        return True, str(output_file)
        
    except Exception as e:
        return False, f"เกิดข้อผิดพลาดในการประมวลผล {input_file}: {str(e)}"

def process_multiple_files_complete(input_pattern: str, output_dir: str):
    files = find_input_files(input_pattern)
    if not files:
        print(f" ไม่พบไฟล์ที่ตรงกับ pattern: {input_pattern}")
        return
    print(f" พบไฟล์ทั้งหมด {len(files)} ไฟล์")
    print("=" * 60)
    successful = 0
    failed = 0
    start_time = time.time()
    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] ", end="")
        success, message = process_single_file_complete(file_path, output_dir)
        if success:
            print(f" สำเร็จ: {message}")
            successful += 1
        else:
            print(f" ล้มเหลว: {message}")
            failed += 1
    end_time = time.time()
    print("\n" + "=" * 60)
    print(f" ใช้เวลา: {end_time - start_time:.2f} วินาที")
    print(f" ผลลัพธ์: สำเร็จ {successful} ไฟล์, ล้มเหลว {failed} ไฟล์")

# ---------- 2. รวม Summary ----------

def load_sec_strip_by_frame(filepath, sheet_name='Processed_Data'):
    print(f"         📄 อ่านไฟล์: {os.path.basename(filepath)}")
    
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        print(f"         ✅ อ่าน sheet '{sheet_name}' ได้: {df.shape}")
    except ValueError as e:
        print(f"         ⚠️  ไม่พบ sheet '{sheet_name}', ลองอ่าน Sheet1")
        try:
            df = pd.read_excel(filepath, sheet_name='Sheet1')
            print(f"         ✅ อ่าน Sheet1 ได้: {df.shape}")
        except Exception as e2:
            print(f"         ❌ ไม่สามารถอ่าน Sheet1: {str(e2)}")
            raise ValueError(f"ไม่สามารถอ่านไฟล์ได้: {str(e2)}")
    except Exception as e:
        print(f"         ❌ ไม่สามารถอ่านไฟล์: {str(e)}")
        raise
    
    print(f"         📋 คอลัมน์ที่มี: {list(df.columns)}")
    
    required_cols = ['frame', 'speed', 'sec/strip']
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        print(f"         ❌ ไม่มีคอลัมน์ที่ต้องการ: {missing_cols}")
        raise ValueError(f"ไม่มีคอลัมน์ที่ต้องการ: {missing_cols}")
    
    print(f"         ✅ มีครบทุกคอลัมน์ที่ต้องการ")
    
    df['frame'] = df['frame'].astype(str)
    df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
    df['sec/strip'] = pd.to_numeric(df['sec/strip'], errors='coerce')
    
    # นับจำนวนข้อมูลก่อนกรอง
    before_filter = len(df)
    df = df[df['sec/strip'].notna() & df['speed'].notna()]
    after_filter = len(df)
    
    print(f"         📊 ข้อมูลก่อนกรอง: {before_filter} แถว")
    print(f"         📊 ข้อมูลหลังกรอง: {after_filter} แถว")
    
    if df.empty:
        print(f"         ⚠️  ไม่มีข้อมูลที่ใช้งานได้หลังกรอง")
    
    return df

def summarize_sec_strip(files_folder, file_list):
    print(f"   📁 กำลังประมวลผลไฟล์จาก folder: {files_folder}")
    print(f"   📋 รายการไฟล์: {file_list}")
    
    data = {}
    successful_files = 0
    failed_files = 0
    
    for filename in file_list:
        filepath = os.path.join(files_folder, filename)
        print(f"   🔍 ประมวลผล: {filename}")
        
        try:
            # ตรวจสอบว่าไฟล์มีอยู่จริง
            if not os.path.exists(filepath):
                print(f"   ❌ ไม่พบไฟล์: {filepath}")
                failed_files += 1
                continue
                
            df = load_sec_strip_by_frame(filepath)
            print(f"      📊 โหลดข้อมูลได้: {df.shape[0]} แถว")
            
            if df.empty:
                print(f"      ⚠️  ไฟล์ว่างเปล่า: {filename}")
                failed_files += 1
                continue
                
            summary = df.groupby(['frame', 'speed'])['sec/strip'].mean()
            summary.index = summary.index.map(lambda x: f"{x[0]}_speed{x[1]}")
            file_key = os.path.splitext(filename)[0]
            data[file_key] = summary
            
            print(f"      ✅ สำเร็จ: {len(summary)} กลุ่มข้อมูล")
            successful_files += 1
            
        except Exception as e:
            print(f"      ❌ ข้ามไฟล์ {filename} : {str(e)}")
            failed_files += 1
            continue
    
    print(f"   📊 สรุป: สำเร็จ {successful_files} ไฟล์, ล้มเหลว {failed_files} ไฟล์")
    
    if not data:
        print(f"   ❌ ไม่มีข้อมูลใดๆ จากไฟล์ทั้งหมด")
        return pd.DataFrame()
        
    result_df = pd.DataFrame(data)
    result_df = result_df.sort_index()
    print(f"   ✅ สร้าง result DataFrame: {result_df.shape}")
    
    return result_df

def save_summary(df, output_path):
    df.index.name = "FRAME_STOCK"
    df.to_excel(output_path, index=True)
    print(f"✅ Saved comparison summary to: {output_path}")

# ---------- 3. Export CSV ----------

def remove_outliers(data):
    if not data:
        return []
    arr = np.array(data)
    q1 = np.percentile(arr, 25)
    q3 = np.percentile(arr, 75)
    iqr = q3 - q1
    upper_bound = q3 + 1.5 * iqr
    filtered = arr[(arr <= upper_bound)]
    return filtered.tolist()

def filtered_mean(lst):
    filtered = remove_outliers(lst)
    if len(filtered) == 0:
        return float('nan')
    return sum(filtered) / len(filtered)

# กำหนด mapping เงื่อนไข Package group → Lead frame
MAPPING = {
    ('QFN', '5.0'): 'Copper ',
    ('QFN', '4.0'): 'Selective PPF',
    ('QFN', '3.0'): 'Full PPF',
}

def analyze_and_export_csv(summary_path, package_path, output_csv):
    df = pd.read_excel(summary_path)
    df2 = pd.read_excel(package_path)
    df['non_null_values'] = df.loc[:, df.columns != 'FRAME_STOCK'].apply(
        lambda row: row.dropna().tolist(), axis=1)
    df = df[['FRAME_STOCK', 'non_null_values']]
    df['TIME/STRIP'] = df['non_null_values'].apply(filtered_mean)
    df = df[['FRAME_STOCK', 'TIME/STRIP']]
    df['SPEED'] = df['FRAME_STOCK'].astype(str).str[-3:]
    df['X'] = df['FRAME_STOCK'].astype(str).str[0:6]
    df = df[['X', 'SPEED', 'TIME/STRIP', 'FRAME_STOCK']]
    df = df.drop(columns='FRAME_STOCK')
    df['TIME/STRIP'] = df['TIME/STRIP'].round(2)
    df.rename(columns={'X': 'FRAME_STOCK'}, inplace=True)
    df.rename(columns={'SPEED': 'SPEED (IPS)'},inplace=True)
    df_merged = pd.merge(df, df2[['FRAME_STOCK', 'PACKAGE_CODE']], on='FRAME_STOCK', how='left')
    df_merged.to_csv(output_csv, index=False)
    print(f"✅ Exported summary CSV: {output_csv}")

def analyze_and_export_csv_from_df(summary_df, package_path, output_csv):
    """
    วิเคราะห์และส่งออกข้อมูลเป็นไฟล์ CSV
    """
    print("📊 เริ่มวิเคราะห์และส่งออก CSV...")
    
    df = summary_df.reset_index()
    # ตรวจสอบและเปลี่ยนชื่อคอลัมน์ index ให้เป็น 'FRAME_STOCK'
    if df.columns[0] != 'FRAME_STOCK':
        df = df.rename(columns={df.columns[0]: 'FRAME_STOCK'})
    
    # โหลดข้อมูล package
    print(f"📁 โหลดข้อมูล package จาก: {package_path}")
    df2 = pd.read_excel(package_path, sheet_name="Export Worksheet")
    
    # ตรวจสอบคอลัมน์ที่มีอยู่ในไฟล์ package
    print("🔍 ตรวจสอบคอลัมน์ที่มีอยู่ในไฟล์ package...")
    required_cols = ['FRAME_STOCK', 'PACKAGE_CODE', 'Package size ', 'Package group', 'Frame type ', 'Unit/strip']
    available_cols = ['FRAME_STOCK']  # FRAME_STOCK เป็นคอลัมน์หลักที่ต้องมี
    
    for col in required_cols[1:]:  # ข้าม FRAME_STOCK
        if col in df2.columns:
            available_cols.append(col)
            print(f"   ✅ พบคอลัมน์: {col}")
        else:
            print(f"   ⚠️  ไม่พบคอลัมน์: {col}")
    
    print(f"📊 คอลัมน์ที่จะใช้ในการ merge: {available_cols}")
    
    # ประมวลผลข้อมูล
    print("🔄 กำลังประมวลผลข้อมูล...")
    df['non_null_values'] = df.loc[:, df.columns != 'FRAME_STOCK'].apply(
        lambda row: row.dropna().tolist(), axis=1)
    df = df[['FRAME_STOCK', 'non_null_values']]
    df['TIME/STRIP'] = df['non_null_values'].apply(filtered_mean)
    df = df[['FRAME_STOCK', 'TIME/STRIP']]
    df['SPEED (IPS)'] = df['FRAME_STOCK'].astype(str).str[-3:]
    df['X'] = df['FRAME_STOCK'].astype(str).str[0:6]
    df = df[['X', 'SPEED (IPS)', 'TIME/STRIP', 'FRAME_STOCK']]
    df = df.drop(columns='FRAME_STOCK')
    df['TIME/STRIP'] = df['TIME/STRIP'].round(2)
    df.rename(columns={'X': 'FRAME_STOCK'}, inplace=True)
    
    # Merge กับข้อมูล package
    print("🔗 รวมข้อมูลกับ package data...")
    df_merged = pd.merge(
        df,
        df2[available_cols],
        on='FRAME_STOCK',
        how='left'
    )
    
    # แก้ไขการใช้ MAPPING (เฉพาะเมื่อมีคอลัมน์ที่เกี่ยวข้อง)
    if 'Frame type ' in df_merged.columns and 'Package group' in df_merged.columns:
        print("🔧 ปรับปรุง Frame type ตาม mapping...")
        df_merged['Frame type '] = df_merged.apply(
            lambda row: MAPPING.get((str(row['Package group']), str(row['SPEED (IPS)'])), row['Frame type ']),
            axis=1
        )
    else:
        print("⚠️  ข้าม MAPPING เนื่องจากไม่มีคอลัมน์ที่จำเป็น")
    
    # สร้างคอลัมน์ Process (เฉพาะเมื่อมีคอลัมน์ Package group)
    if 'Package group' in df_merged.columns:
        print("⚙️ กำหนด Process...")
        df_merged['Process'] = None
        
        # ทำความสะอาดข้อมูล Package group
        df_merged['Package group'] = df_merged['Package group'].astype(str).str.strip().str.upper()
        # แปลง SPEED ให้เป็นตัวเลข
        df_merged['SPEED (IPS)'] = pd.to_numeric(df_merged['SPEED (IPS)'], errors='coerce')

        # แสดงข้อมูลก่อนกำหนด Process เพื่อ debug
        print("   🔍 ตรวจสอบข้อมูลก่อนกำหนด Process:")
        debug_summary = df_merged.groupby(['Package group', 'SPEED (IPS)']).size().reset_index(name='count')
        for _, row in debug_summary.iterrows():
            print(f"      - Package group: '{row['Package group']}' | Speed: {row['SPEED (IPS)']} | จำนวน: {row['count']}")

        # ✅ แก้ไขเงื่อนไข Process - ให้ชัดเจนว่าใช้กับ SLP เท่านั้น
        print("   🔍 กำหนดเงื่อนไข Process สำหรับ SLP เท่านั้น...")
        
        # สร้างเงื่อนไขแยกชัดเจน
        condition_slp_speed5 = (df_merged['SPEED (IPS)'] == 5) & (df_merged['Package group'] == 'SLP')
        condition_slp_speed3 = (df_merged['SPEED (IPS)'] == 3) & (df_merged['Package group'] == 'SLP')
        
        # แสดงจำนวนข้อมูลที่ตรงเงื่อนไข
        print(f"      - SLP Speed 5: {condition_slp_speed5.sum()} รายการ")
        print(f"      - SLP Speed 3: {condition_slp_speed3.sum()} รายการ")
        
        # กำหนด Process ด้วย .loc[] แทน np.select()
        df_merged.loc[condition_slp_speed5, 'Process'] = 'Full Cut'
        df_merged.loc[condition_slp_speed3, 'Process'] = 'Step Cut'
        
        # แสดงสรุปการกำหนด Process
        process_summary = df_merged.groupby(['Package group', 'SPEED (IPS)', 'Process']).size().reset_index(name='count')
        print("   📊 สรุปการกำหนด Process:")
        for _, row in process_summary.iterrows():
            process_value = row['Process'] if pd.notna(row['Process']) else 'None'
            print(f"      - {row['Package group']} | Speed {row['SPEED (IPS)']} → {process_value} ({row['count']} รายการ)")
            
        # ตรวจสอบ QFN ที่อาจได้ Process ผิดพลาด
        qfn_with_process = df_merged[(df_merged['Package group'] == 'QFN') & (df_merged['Process'].notna())]
        if not qfn_with_process.empty:
            print(f"   ⚠️  พบ QFN ที่ได้ Process ผิดพลาด: {len(qfn_with_process)} รายการ")
            for _, row in qfn_with_process.iterrows():
                print(f"      - {row['FRAME_STOCK']}: Package group = '{row['Package group']}', Speed = {row['SPEED (IPS)']}, Process = '{row['Process']}'")
        else:
            print("   ✅ QFN ไม่มี Process (ถูกต้อง)")
            
    else:
        print("⚠️  ข้าม Process เนื่องจากไม่มีคอลัมน์ Package group")
        # แปลง SPEED ให้เป็นตัวเลขอย่างน้อย
        df_merged['SPEED (IPS)'] = pd.to_numeric(df_merged['SPEED (IPS)'], errors='coerce')
    
    # ✅ ขั้นตอนสุดท้าย: เรียกใช้ group_and_average_across_frames_unique_frame
    print("🎯 ขั้นตอนสุดท้าย: การจัดกลุ่มและคำนวณค่าเฉลี่ยข้าม frame...")
    df_final = group_and_average_across_frames_unique_frame(df_merged)
    
    # บันทึกไฟล์ CSV
    print(f"💾 บันทึกไฟล์ CSV: {output_csv}")
    df_final.to_csv(output_csv, index=False)
    print(f"✅ Exported summary CSV: {output_csv}")
    
    return df_final

def group_and_average_across_frames_unique_frame(df_merged):
    print("🔄 กำลังจัดกลุ่มและคำนวณค่าเฉลี่ย...")
    
    # รายการคอลัมน์ที่ต้องการสำหรับจัดกลุ่ม
    potential_grouping_cols = [
        'Package size ',
        'Package group',
        'Frame type ',
        'Unit/strip',
        'SPEED (IPS)'
    ]
    
    grouping_cols = [col for col in potential_grouping_cols if col in df_merged.columns]
    
    if 'SPEED (IPS)' not in grouping_cols:
        print("❌ ไม่พบคอลัมน์ SPEED (IPS) ที่จำเป็นสำหรับการจัดกลุ่ม")
        return df_merged
    
    print(f"📊 ข้อมูลเริ่มต้น: {df_merged.shape[0]} แถว")
    
    # ✅ วิธีที่ดีที่สุด: รวมข้อมูลแทนการลบ duplicate
    print("🔄 รวมข้อมูล FRAME_STOCK ที่ซ้ำกัน...")
    
    # ตรวจสอบ duplicate ก่อน
    duplicated_frames = df_merged[df_merged.duplicated(subset=['FRAME_STOCK'], keep=False)]
    if not duplicated_frames.empty:
        print(f"⚠️  พบ FRAME_STOCK ที่ซ้ำกัน: {duplicated_frames['FRAME_STOCK'].nunique()} ตัว")
        duplicate_analysis = {}
        for frame in duplicated_frames['FRAME_STOCK'].unique():
            frame_data = duplicated_frames[duplicated_frames['FRAME_STOCK'] == frame]
            time_values = frame_data['TIME/STRIP'].dropna().tolist()
            duplicate_analysis[frame] = {
                'count': len(frame_data),
                'time_values': time_values,
                'non_null_count': len(time_values)
            }
            print(f"   - {frame}: {len(frame_data)} แถว → TIME/STRIP: {time_values}")
    
    # ✅ รวมข้อมูลโดยใช้กลยุทธ์ที่ดีที่สุด
    def smart_aggregation(group):
        # สำหรับ TIME/STRIP: ใช้ค่าเฉลี่ยของค่าที่ไม่เป็น NaN
        time_values = group['TIME/STRIP'].dropna()
        if len(time_values) > 0:
            time_result = time_values.mean()
        else:
            time_result = np.nan
        
        # สำหรับคอลัมน์อื่นๆ: ใช้ค่าแรกที่ไม่เป็น NaN
        result = {}
        for col in df_merged.columns:
            if col == 'TIME/STRIP':
                result[col] = time_result
            elif col == 'FRAME_STOCK':
                result[col] = group[col].iloc[0]  # ใช้ค่าเดิม
            else:
                # ใช้ค่าแรกที่ไม่เป็น NaN
                non_null_values = group[col].dropna()
                if len(non_null_values) > 0:
                    result[col] = non_null_values.iloc[0]
                else:
                    result[col] = group[col].iloc[0]
        
        return pd.Series(result)
    
    # รวมข้อมูลตาม FRAME_STOCK
    df_unique = df_merged.groupby('FRAME_STOCK').apply(smart_aggregation).reset_index(drop=True)
    
    print(f"📊 ข้อมูลหลังรวม: {df_unique.shape[0]} แถว")
    
    # แสดงการเปลี่ยนแปลง TIME/STRIP
    if not duplicated_frames.empty:
        print("📊 การรวม TIME/STRIP สำหรับ FRAME_STOCK ที่ซ้ำ:")
        for frame, info in duplicate_analysis.items():
            if info['non_null_count'] > 1:
                new_value = df_unique[df_unique['FRAME_STOCK'] == frame]['TIME/STRIP'].iloc[0]
                if not pd.isna(new_value):
                    print(f"   - {frame}: {info['time_values']} → {round(new_value, 2)} (เฉลี่ยจาก {info['non_null_count']} ค่า)")
    
    # ✅ ติดตาม FRAME_STOCK ที่ไม่มีข้อมูล TIME/STRIP
    frames_without_time = df_unique[df_unique['TIME/STRIP'].isna()]['FRAME_STOCK'].tolist()
    if frames_without_time:
        print(f"⚠️  FRAME_STOCK ที่ไม่มีข้อมูล TIME/STRIP: {len(frames_without_time)} ตัว")
        for frame in frames_without_time[:5]:  # แสดงแค่ 5 ตัวแรก
            print(f"   - {frame}")
        if len(frames_without_time) > 5:
            print(f"   ... และอีก {len(frames_without_time) - 5} ตัว")
    
    # ประมวลผลต่อไป...
    group_avg_map = {}
    group_before_map = {}
    group_after_map = {}
    total_groups = 0
    processed_groups = 0
    total_outliers_removed = 0
    excluded_frames = []
    excluded_reasons = []
    
    print("🔍 วิเคราะห์กลุ่มข้อมูล...")
    print("=" * 80)
    
    for group_key, group_df in df_unique.groupby(grouping_cols):
        total_groups += 1
        group_name = " | ".join([f"{col}={val}" for col, val in zip(grouping_cols, group_key)])
        total_frames_in_group = len(group_df)
        
        # นับเฉพาะข้อมูลที่มีค่า TIME/STRIP
        values = group_df['TIME/STRIP'].dropna().tolist()
        frames_with_data = group_df[group_df['TIME/STRIP'].notna()]['FRAME_STOCK'].tolist()
        frames_without_data = group_df[group_df['TIME/STRIP'].isna()]['FRAME_STOCK'].tolist()

        # ✅ ใช้ len(values) เพื่อนับเฉพาะที่มีข้อมูล
        group_before_map[group_key] = len(values)
    
        # บันทึก FRAME_STOCK ที่ไม่มีข้อมูล TIME/STRIP
        for frame in frames_without_data:
            excluded_frames.append(frame)
            excluded_reasons.append(f"กลุ่ม: {group_name} | เหตุผล: ไม่มีข้อมูล TIME/STRIP")

        # ✅ แก้ไข: ไม่ให้ Before_Outlier และ After_Outlier เป็น 0
        if len(values) < 2:
            print(f"❌ กลุ่ม: {group_name}")
            print(f"   📊 Frame Stock ทั้งหมด: {total_frames_in_group} ตัว")
            print(f"   📊 มีข้อมูล TIME/STRIP: {len(values)} ค่า")
            print(f"   📋 FRAME_STOCK ที่มีข้อมูล: {frames_with_data}")
            if frames_without_data:
                print(f"   ❌ FRAME_STOCK ที่ไม่มีข้อมูล: {frames_without_data}")
            
            # กำหนดค่าเฉลี่ยแม้มีข้อมูลน้อย
            if len(values) == 1:
                group_avg_map[group_key] = values[0]
                group_after_map[group_key] = 1  # ✅ ใช้ค่าเดียวที่มี
                print(f"   ✅ ใช้ค่าเดียว: {values[0]}")
            elif len(values) == 0:
                group_avg_map[group_key] = np.nan
                group_after_map[group_key] = np.nan  # ✅ เปลี่ยนจาก 0 เป็น NaN
                print(f"   ⚠️  ไม่มีข้อมูล: ใช้ NaN")
            else:
                group_avg_map[group_key] = np.mean(values)
                group_after_map[group_key] = len(values)  # ✅ ใช้จำนวนข้อมูลจริง
                print(f"   ✅ ใช้ค่าเฉลี่ยโดยไม่กรอง: {np.mean(values)}")
            
            print(f"   📊 จำนวน: {len(values)}/{group_after_map[group_key]} (มีข้อมูล/หลังตัด)")
            print()
            
        else:
            # กรองข้อมูล outliers
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            
            filtered = [v for v in values if lower <= v <= upper]
            outliers = [v for v in values if v < lower or v > upper]
            
            if filtered:
                avg_val = round(np.mean(filtered), 2)
                group_avg_map[group_key] = avg_val
                group_after_map[group_key] = len(filtered)  # ✅ ใช้จำนวนข้อมูลที่เหลือหลังกรอง
                processed_groups += 1
                total_outliers_removed += len(outliers)
                
                print(f"✅ กลุ่ม: {group_name}")
                print(f"   📊 Frame Stock ทั้งหมด: {total_frames_in_group} ตัว")
                print(f"   📊 มีข้อมูล TIME/STRIP: {len(values)} ค่า")
                print(f"   📈 ช่วงปกติ: {round(lower, 2)} - {round(upper, 2)}")
                print(f"   ✅ ข้อมูลที่ใช้: {len(filtered)} ค่า → {filtered}")
                
                if outliers:
                    print(f"   ❌ Outliers ที่ตัดออก: {len(outliers)} ค่า → {outliers}")
                    print(f"   📊 เปอร์เซ็นต์ที่ตัด: {round(len(outliers)/len(values)*100, 1)}%")
                else:
                    print(f"   ✨ ไม่มี outliers")
                
                print(f"   🎯 ค่าเฉลี่ยสุดท้าย: {avg_val}")
                print(f"   📊 จำนวน: {len(values)}/{len(filtered)} (มีข้อมูล/หลังตัด)")
                print()
            else:
                print(f"⚠️  กลุ่ม: {group_name}")
                print(f"   📊 Frame Stock ทั้งหมด: {total_frames_in_group} ตัว")
                print(f"   📊 ข้อมูลทั้งหมด: {len(values)} ค่า → {values}")
                print(f"   ❌ ทุกค่าเป็น outliers")
                print(f"   📈 ช่วงปกติ: {round(lower, 2)} - {round(upper, 2)}")
                
                group_avg_map[group_key] = round(np.mean(values), 2)
                group_after_map[group_key] = len(values)  # ✅ เปลี่ยนจาก 0 เป็น len(values) เพราะใช้ค่าเฉลี่ยดิบ
                print(f"   ⚠️  ใช้ค่าเฉลี่ยดิบ: {round(np.mean(values), 2)}")
                print(f"   📊 จำนวน: {len(values)}/{len(values)} (มีข้อมูล/หลังตัด)")
                print()

    # แสดงสรุป
    print("=" * 80)
    print(f"📈 สรุปการประมวลผล:")
    print(f"   🔢 กลุ่มทั้งหมด: {total_groups} กลุ่ม")
    print(f"   ✅ กลุ่มที่ประมวลผลได้: {processed_groups} กลุ่ม")
    print(f"   ❌ กลุ่มที่ข้ามไป: {total_groups - processed_groups} กลุ่ม")
    print(f"   🗑️  Outliers ที่ตัดออกทั้งหมด: {total_outliers_removed} ค่า")
    print(f"   📊 อัตราสำเร็จ: {round(processed_groups/total_groups*100, 1)}%")
    
    # แสดงรายงาน FRAME_STOCK ที่ไม่ถูกนำมาคิด
    if excluded_frames:
        print(f"\n❌ FRAME_STOCK ที่ไม่ถูกนำมาคิด: {len(excluded_frames)} ตัว")
        print("=" * 80)
        for i, (frame, reason) in enumerate(zip(excluded_frames, excluded_reasons), 1):
            print(f"   {i:2d}. {frame} → {reason}")
        print("=" * 80)
    else:
        print(f"\n✅ FRAME_STOCK ทุกตัวถูกนำมาคิดแล้ว")
    
    # ฟังก์ชันสำหรับ apply
    def assign_before_count(row):
        key = tuple(row[col] for col in grouping_cols)
        count = group_before_map.get(key, np.nan)
        # ✅ ไม่ให้เป็น 0 - ใช้ NaN แทน
        return count if pd.notna(count) and count > 0 else np.nan

    def assign_after_count(row):
        key = tuple(row[col] for col in grouping_cols)
        count = group_after_map.get(key, np.nan)
        # ✅ ไม่ให้เป็น 0 - ใช้ NaN แทน
        return count if pd.notna(count) and count > 0 else np.nan

    def assign_avg(row):
        key = tuple(row[col] for col in grouping_cols)
        original_value = row['TIME/STRIP']
        new_value = group_avg_map.get(key, original_value)
        
        if pd.notna(original_value) and pd.notna(new_value) and abs(original_value - new_value) > 0.01:
            change_type = "📈" if new_value > original_value else "📉"
            diff = abs(new_value - original_value)
            before_count = group_before_map.get(key, np.nan)
            after_count = group_after_map.get(key, np.nan)
            print(f"   {change_type} {row['FRAME_STOCK']}: {original_value} → {new_value} (เปลี่ยน {round(diff, 2)}) [{before_count}/{after_count}]")
            
        return new_value

    print("🔄 กำลังอัปเดตค่า TIME/STRIP...")
    df_unique['TIME/STRIP'] = df_unique.apply(assign_avg, axis=1)

    print("📊 เพิ่มคอลัมน์จำนวนข้อมูลก่อนและหลังตัด...")
    df_unique['Before_Outlier'] = df_unique.apply(assign_before_count, axis=1)
    df_unique['After_Outlier'] = df_unique.apply(assign_after_count, axis=1)

    print("✅ เสร็จสิ้นการจัดกลุ่มและคำนวณค่าเฉลี่ย")
    return df_unique

def LOGVIEW(input_path, output_dir):
    """
    ฟังก์ชันหลักสำหรับประมวลผลไฟล์ LOGVIEW
    """
    print(f"🚀 เริ่มประมวลผล LOGVIEW")
    print(f"📁 Input: {input_path}")
    print(f"📁 Output: {output_dir}")
    
    # 1. ประมวลผลไฟล์ input และเก็บชื่อไฟล์ .xlsx ที่สร้างใหม่
    print("📊 ขั้นตอนที่ 1: ประมวลผลไฟล์ input...")
    before_files = set(f for f in os.listdir(output_dir) if f.lower().endswith('.xlsx'))
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # ประมวลผลในไฟล์ชั่วคราว
        process_multiple_files_complete(input_path, temp_dir)
        # หาไฟล์ใหม่ที่สร้างขึ้น
        temp_files = set(f for f in os.listdir(temp_dir) if f.lower().endswith('.xlsx'))
        
        if not temp_files:
            print(" ไม่พบไฟล์ .xlsx ใหม่")
            return

        print(f" สร้างไฟล์ใหม่ {len(temp_files)} ไฟล์")
        
        # คัดลอกไฟล์จาก temp_dir ไปยัง output_dir
        import shutil
        new_files = []
        for filename in temp_files:
            src = os.path.join(temp_dir, filename)
            dst = os.path.join(output_dir, filename)
            shutil.copy2(src, dst)
            new_files.append(filename)
            print(f"   ✅ คัดลอก: {filename}")
    
    # ตรวจสอบไฟล์ที่คัดลอกมาแล้ว
    if not new_files:
        print(" ไม่สามารถคัดลอกไฟล์ได้")
        return

    # 2. สร้าง summary DataFrame
    print("📊 ขั้นตอนที่ 2: สร้าง summary...")
    try:
        summary_df = summarize_sec_strip(output_dir, new_files)
        print(f"   ✅ ข้อมูล summary: {summary_df.shape}")
        
        if summary_df.empty:
            print("   ❌ Summary DataFrame ว่างเปล่า")
            return
            
    except Exception as e:
        print(f"   ❌ เกิดข้อผิดพลาดในการสร้าง summary: {str(e)}")
        return
    
    # 3. ตรวจสอบไฟล์ package
    print("📊 ขั้นตอนที่ 3: ตรวจสอบไฟล์ package...")
    package_path = os.path.join(BASE_DIR, "..", "data_MAP", "export package and frame stock Rev.06.xlsx")
    package_path = os.path.abspath(package_path)
    
    print(f"   📁 ตรวจสอบ package path: {package_path}")
    
    if not os.path.exists(package_path):
        print("   ❌ ไม่พบไฟล์ export package and frame stock Rev.04.xlsx ใน data_MAP")
        print(f"   📁 ตรวจสอบ directory: {os.path.dirname(package_path)}")
        if os.path.exists(os.path.dirname(package_path)):
            upload_files = os.listdir(os.path.dirname(package_path))
            print(f"   📋 ไฟล์ใน Upload folder: {upload_files}")
        return
    else:
        print(f"   ✅ พบไฟล์ package: {package_path}")
    
    # 4. สร้างไฟล์ Summary.csv ด้วย timestamp
    print("📊 ขั้นตอนที่ 4: สร้างไฟล์ CSV สุดท้าย...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = os.path.join(output_dir, f"Summary_{timestamp}.csv")
    
    try:
        # ✅ ฟังก์ชันนี้จะเรียกใช้ group_and_average_across_frames_unique_frame ในขั้นตอนสุดท้าย
        final_df = analyze_and_export_csv_from_df(summary_df, package_path, output_csv)
        print(f"🎉 ประมวลผลเสร็จสิ้น!")
        print(f"   📄 ไฟล์ผลลัพธ์: {output_csv}")
        print(f"   📊 ข้อมูลสุดท้าย: {final_df.shape[0]} แถว")
        if os.path.exists(output_csv):
            return output_csv
        else:
            print(f"   ❌ ไม่พบไฟล์ผลลัพธ์: {output_csv}")
            return None
    except Exception as e:
        print(f"   ❌ เกิดข้อผิดพลาดในการสร้าง CSV: {str(e)}")
        return None





