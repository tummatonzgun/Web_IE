import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

def apply_zscore(df, uph_col):
    """ตัด outliers ด้วย Z-Score (±3 std)"""
    mean = df[uph_col].mean()
    std = df[uph_col].std()
    if std == 0:
        return df
    z_scores = (df[uph_col] - mean) / std
    filtered = df[(z_scores >= -3) & (z_scores <= 3)].copy()
    filtered['Outlier_Method'] = 'Z-Score ±3'
    return filtered

def apply_iqr(df, uph_col):
    """ตัด outliers ด้วย IQR"""
    Q1 = df[uph_col].quantile(0.25)
    Q3 = df[uph_col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    filtered = df[(df[uph_col] >= lower) & (df[uph_col] <= upper)].copy()
    filtered['Outlier_Method'] = 'IQR'
    return filtered

def has_outlier(df, uph_col):
    """ตรวจสอบ outliers"""
    Q1 = df[uph_col].quantile(0.25)
    Q3 = df[uph_col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    return ((df[uph_col] < lower) | (df[uph_col] > upper)).sum() > 0

def remove_outliers_auto(df_model, uph_col, max_iter=20):
    """ตัด outliers อัตโนมัติ"""
    df_model[uph_col] = pd.to_numeric(df_model[uph_col], errors='coerce')
    df_model = df_model.dropna(subset=[uph_col])

    if len(df_model) < 15:
        df_model['Outlier_Method'] = 'ไม่ตัด (ข้อมูลน้อย)'
        return df_model

    current_df = df_model.copy()
    for i in range(max_iter):
        z_df = apply_zscore(current_df, uph_col)
        if not has_outlier(z_df, uph_col):
            z_df['Outlier_Method'] = f'Z-Score Loop ×{i+1}'
            return z_df

        iqr_df = apply_iqr(z_df, uph_col)
        if not has_outlier(iqr_df, uph_col):
            iqr_df['Outlier_Method'] = f'IQR Loop ×{i+1}'
            return iqr_df
        current_df = iqr_df

    current_df['Outlier_Method'] = f'IQR-Z-Score Loop ×{max_iter}+'
    return current_df

def get_column_names(df):
    """หาชื่อคอลัมน์ที่ต้องการ"""
    col_map = {col.lower(): col for col in df.columns}
    
    uph_col = col_map.get('uph')
    if not uph_col:
        raise KeyError("ไม่พบคอลัมน์ UPH")
    
    model_col = col_map.get('machine model') or col_map.get('machine_model')
    if not model_col:
        raise KeyError("ไม่พบคอลัมน์ Machine Model")
    
    bom_col = col_map.get('bom_no') or col_map.get('bom no')
    if not bom_col:
        raise KeyError("ไม่พบคอลัมน์ bom_no")
    
    date_col = None
    for col_name in df.columns:
        if any(keyword in col_name.lower() for keyword in ['date', 'time', 'วัน', 'เวลา']):
            date_col = col_name
            break
    
    return uph_col, model_col, bom_col, date_col

def load_file(file_path):
    """อ่านไฟล์ตามประเภท"""
    if file_path.endswith('.xlsx'):
        return pd.read_excel(file_path, engine='openpyxl')
    elif file_path.endswith('.xls'):
        return pd.read_excel(file_path, engine='xlrd')
    elif file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.json'):
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        if isinstance(json_data, list):
            return pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            for key in ['data', 'results', 'items', 'records']:
                if key in json_data and isinstance(json_data[key], list):
                    return pd.DataFrame(json_data[key])
            return pd.DataFrame([json_data])
    else:
        return pd.read_excel(file_path, engine='openpyxl')

def remove_outliers(df):
    """ตัด outliers ตามกลุ่ม"""
    uph_col, model_col, bom_col, _ = get_column_names(df)
    result_dfs = []
    
    # เพิ่ม optn_code ใน groupby
    for (bom_no, machine_model, optn_code, device, package_code, bom_rev), group_df in df.groupby([bom_col, model_col, 'optn_code', 'device', 'package_code', 'bom_rev']):
        before_count = len(group_df)
        cleaned_group = remove_outliers_auto(group_df, uph_col)
        after_count = len(cleaned_group)
        cleaned_group['DataPoints_Before'] = before_count
        cleaned_group['DataPoints_After'] = after_count
        cleaned_group['Outliers_Removed'] = before_count - after_count
        result_dfs.append(cleaned_group)
    
    return pd.concat(result_dfs, ignore_index=True)

def process_date_column(df):
    """ประมวลผลคอลัมน์วันที่"""
    _, _, _, date_col = get_column_names(df)
    
    if not date_col:
        print("ไม่พบคอลัมน์วันที่")
        return df
    
    print(f"ใช้คอลัมน์วันที่: {date_col}")
    df['date_time_start'] = pd.to_datetime(df[date_col], errors='coerce')
    df['date_time_start'] = df['date_time_start'].dt.strftime('%Y/%m/%d')
    
    invalid_dates = df['date_time_start'].isna().sum()
    if invalid_dates > 0:
        print(f"พบวันที่ที่แปลงไม่ได้: {invalid_dates} แถว")
        df = df.dropna(subset=['date_time_start'])
    
    return df

def get_date_range(df, start_date=None, end_date=None):
    """ได้ช่วงวันที่"""
    if start_date and end_date:
        return start_date, end_date
    
    max_date = df['date_time_start'].max()
    min_date = df['date_time_start'].min()
    print(f"ใช้ช่วงวันที่ทั้งหมด: {min_date} ถึง {max_date}")
    return min_date, max_date

def filter_by_date_range(df, start_date, end_date):
    """กรองข้อมูลตามช่วงวันที่"""
    filtered_df = df[df['date_time_start'].between(start_date, end_date)].copy()
    
    if len(filtered_df) == 0:
        raise Exception("ไม่พบข้อมูลในช่วงวันที่ที่เลือก")
    
    print(f"กรองข้อมูล: {len(filtered_df)}/{len(df)} แถว")
    return filtered_df

def calculate_group_average(df, start_date, end_date):
    uph_col, model_col, bom_col, _ = get_column_names(df)
    # เพิ่ม optn_code ใน groupby
    grouped = df.groupby([bom_col, model_col, 'optn_code', 'device', 'package_code', 'bom_rev'], as_index=False).agg({uph_col: 'mean'})
    grouped[uph_col] = grouped[uph_col].round(3)
    other_cols = ['operation', 'optn_code'] + (['DataPoints_Before', 'DataPoints_After','Outliers_Removed'] if 'DataPoints_Before' in df.columns else [])
    if other_cols:
        firsts = df.groupby([bom_col, model_col, 'optn_code'], as_index=False)[other_cols].first()
        grouped = pd.merge(grouped, firsts, on=[bom_col, model_col, 'optn_code'], how='left')
    print(f"=== ค่าเฉลี่ย UPH ({start_date} ถึง {end_date}) ===")
    grouped = grouped.rename(columns={uph_col: 'UPH', model_col: 'Machine Model', bom_col: 'Bom No','operation':"Operation",
                                      'optn_code':'Optn_Code', 'device':'Device', 'package_code':'Package Code', 'bom_rev':'Bom Rev'})

    return grouped

def save_results(df_cleaned, grouped_average, start_date, end_date, output_dir):
    """บันทึกผลลัพธ์"""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_range = f"{start_date.replace('/', '')}_to_{end_date.replace('/', '')}"
    
    cleaned_file = os.path.join(output_dir, f"cleaned_data_{date_range}_{timestamp}.xlsx")
    average_file = os.path.join(output_dir, f"group_average_{date_range}_{timestamp}.xlsx")
    
    df_cleaned.to_excel(cleaned_file, index=False, engine='openpyxl')
    grouped_average.to_excel(average_file, index=False, engine='openpyxl')
    
    print(f"บันทึกไฟล์: {cleaned_file}")
    print(f"บันทึกไฟล์: {average_file}")
    
    return cleaned_file, average_file

def process_die_attack_data(file_path, start_date=None, end_date=None):
    """ประมวลผลข้อมูล Die Attack"""
    print("=== ประมวลผลข้อมูล Die Attack ===")
    
    df = load_file(file_path)
    print(f"ข้อมูลเริ่มต้น: {len(df)} แถว")
    
    df = process_date_column(df)
    
    if start_date and end_date:
        start_date = start_date.replace("-", "/")
        end_date = end_date.replace("-", "/")
    else:
        start_date, end_date = get_date_range(df)
    
    df_filtered = filter_by_date_range(df, start_date, end_date)
    
    print("ตัด outliers...")
    df_cleaned = remove_outliers(df_filtered)
    print(f"ข้อมูลหลังตัด outliers: {len(df_cleaned)} แถว")
    
    grouped_average = calculate_group_average(df_cleaned, start_date, end_date)
    
    return df_cleaned, grouped_average, start_date, end_date

def preview_date_range(file_path):
    """แสดงข้อมูลวันที่ในไฟล์"""
    try:
        df = load_file(file_path)
        print(f"ไฟล์มีข้อมูล: {len(df):,} แถว")
        
        date_cols = [col for col in df.columns 
                    if any(keyword in col.lower() for keyword in ['date', 'time', 'วัน', 'เวลา'])]
        
        if not date_cols:
            print("ไม่พบคอลัมน์วันที่")
            return None
        
        date_col = date_cols[0]
        df['temp_date'] = pd.to_datetime(df[date_col], errors='coerce')
        valid_dates = df.dropna(subset=['temp_date'])
        
        if len(valid_dates) == 0:
            print("ไม่มีข้อมูลวันที่ที่ถูกต้อง")
            return None
        
        min_date = valid_dates['temp_date'].min()
        max_date = valid_dates['temp_date'].max()
        
        print(f"วันที่: {min_date.strftime('%Y-%m-%d')} ถึง {max_date.strftime('%Y-%m-%d')}")
        print(f"ข้อมูลถูกต้อง: {len(valid_dates):,} แถว")
        
        return {
            'min_date': min_date.strftime('%Y-%m-%d'),
            'max_date': max_date.strftime('%Y-%m-%d'),
            'valid_records': len(valid_dates),
            'total_records': len(df)
        }
        
    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {str(e)}")
        return None


def map_data(average_file):
    """Map #OF DIE: (BOM+PKG+PN) → (BOM+PKG+PN_BASE) → (BOM+PKG) → (BOM) → CODE(ทั้ง PN/PN_BASE)"""
    print("=== Map #OF DIE: BOM+PKG+PN → BOM+PKG+PN_BASE → BOM+PKG → BOM → CODE ===")
    try:
        df_left = pd.read_excel(average_file, engine='openpyxl').copy()
        if 'Product Number' not in df_left.columns and 'Device' in df_left.columns:
            df_left['Product Number'] = df_left['Device']

        current_dir = os.path.dirname(os.path.abspath(__file__))
        map_folder = os.path.join(current_dir, "..", "data_MAP")
        mapping_file = os.path.join(map_folder, "Part bom pkg.xlsx")
        if not os.path.exists(mapping_file):
            print(f"⚠️ ไม่พบไฟล์: {mapping_file}")
            return average_file

        df_map = pd.read_excel(mapping_file, engine='openpyxl').copy()

        # รีเนมคอลัมน์สำคัญ
        def nn(s): return s.replace('_','').replace(' ','').lower()
        ren = {}
        for c in df_map.columns:
            k = nn(c)
            if k == 'productnumber': ren[c] = 'Product Number'
            elif k in ('packagecode','pkgcode'): ren[c] = 'Package Code'
            elif k in ('bomno','bom'): ren[c] = 'Bom No'
            elif k == 'bomrev': ren[c] = 'Bom Rev'
            elif k in ('custcode','customercode','cust'): ren[c] = 'Cust Code'
            elif k in ('#ofdie','ofdie'): ren[c] = '#OF DIE'
        if ren: df_map.rename(columns=ren, inplace=True)
        if '#OF DIE' not in df_map.columns: df_map['#OF DIE'] = np.nan

        # ทำให้เป็น upper/strip
        def upperize(df, cols):
            for c in cols:
                if c in df.columns:
                    df[c] = df[c].astype(str).str.strip().str.upper()

        # คีย์ normalized A–Z0–9
        def make_norm_cols(df, keys, suffix='_K'):
            for k in keys:
                if k in df.columns:
                    df[k + suffix] = (
                        df[k].astype(str)
                            .str.upper().str.strip()
                            .str.replace(r'[^A-Z0-9]', '', regex=True)
                    )

        # หา PN_BASE = ตัดท้ายห้อยชุดสุดท้าย (แยกด้วย -, _, *, /, +) แล้ว normalize
        def build_pn_base_col(df, pn_col='Product Number'):
            if pn_col not in df.columns:
                return
            raw = df[pn_col].astype(str).str.upper().str.strip()
            # เอาเว้นวรรคออกก่อน
            raw = raw.str.replace(r'\s+', '', regex=True)
            # แยกตาม delimiter ที่พบบ่อยในท้ายห้อย
            parts = raw.str.split(r'[-_\*\+\/]', expand=False)
            # ถ้ามีมากกว่า 1 ส่วน ตัดส่วนสุดท้ายทิ้ง แล้วต่อกลับ
            base = parts.apply(lambda arr: ''.join(arr[:-1]) if isinstance(arr, list) and len(arr) > 1
                               else (arr[0] if isinstance(arr, list) and len(arr) == 1 else np.nan))
            df['PN_BASE'] = base
            # normalized
            df['PN_BASE_K'] = df['PN_BASE'].astype(str).str.replace(r'[^A-Z0-9]', '', regex=True)

        # ทำความสะอาดค่า #OF DIE ว่าง/#N/A เป็น NaN
        for d in (df_left, df_map):
            if '#OF DIE' in d.columns:
                d['#OF DIE'] = d['#OF DIE'].replace(
                    [r'^\s*$', r'^#N/?A$', r'^N/?A$', r'^NA$', r'^\s*nan\s*$'],
                    np.nan, regex=True
                )

        upperize(df_left, ['Bom No', 'Package Code', 'Product Number'])
        upperize(df_map,  ['Bom No', 'Package Code', 'Product Number', 'Cust Code'])

        # คีย์ normalized + PN_BASE
        base_keys = ['Bom No', 'Package Code', 'Product Number']
        make_norm_cols(df_left, base_keys)
        make_norm_cols(df_map,  base_keys)
        build_pn_base_col(df_left, 'Product Number')
        build_pn_base_col(df_map,  'Product Number')

        out = df_left.copy()
        if '#OF DIE' not in out.columns:
            out['#OF DIE'] = np.nan
        make_norm_cols(out, base_keys)
        build_pn_base_col(out, 'Product Number')

        # เลือกค่าที่ไม่ว่างก่อนเวลาจอยน์
        def build_pref_join(df_map, keysK):
            tmp = df_map[keysK + ['#OF DIE']].copy()
            tmp['#OF DIE'] = tmp['#OF DIE'].replace(
                [r'^\s*$', r'^#N/?A$', r'^N/?A$', r'^NA$', r'^\s*nan\s*$'],
                np.nan, regex=True
            )
            tmp['die_num'] = pd.to_numeric(tmp['#OF DIE'], errors='coerce')
            agg = (tmp.groupby(keysK, dropna=False)['die_num']
                     .max()
                     .reset_index()
                     .rename(columns={'die_num': 'die_tmp'}))
            return agg

        # แผนจอยน์เรียงลำดับ: เพิ่ม PN_BASE เข้ามาก่อนลดเหลือ PKG/BOM
        plans = [
            ['Bom No_K','Package Code_K','Product Number_K'],
            ['Bom No_K','Package Code_K','PN_BASE_K'],
            ['Bom No_K','Package Code_K'],
            ['Bom No_K'],
        ]

        for keysK in plans:
            # ให้แน่ใจว่ามีคีย์ใน map
            for raw in [k.replace('_K','') for k in keysK if not k.startswith('PN_BASE')]:
                if raw not in df_map.columns:
                    continue
                if raw + '_K' not in df_map.columns:
                    make_norm_cols(df_map, [raw])
            if 'PN_BASE_K' in keysK and 'PN_BASE_K' not in df_map.columns:
                build_pn_base_col(df_map, 'Product Number')

            if not all(k in out.columns for k in keysK) or not all(k in df_map.columns for k in keysK):
                continue

            need = out['#OF DIE'].isna()
            if not need.any():
                break

            joinR = build_pref_join(df_map, keysK)
            merged = out.loc[need, keysK].merge(joinR, on=keysK, how='left')
            idx_need = out.index[need]
            prev = out.loc[idx_need, '#OF DIE'].to_numpy()
            cand = merged['die_tmp'].to_numpy()
            out.loc[idx_need, '#OF DIE'] = np.where(pd.isna(prev), cand, prev)

        # Fallback CODE: PKG + Cust + PN (และลอง PN_BASE หากยังไม่เจอ)
        if out['#OF DIE'].isna().any() and all(c in df_map.columns for c in ['Package Code','Cust Code','Product Number']):
            # หา Cust Code จาก BOM ก่อน ถ้าไม่มีค่อยใช้ 3 ตัวแรก
            make_norm_cols(out, ['Bom No'])
            make_norm_cols(df_map, ['Bom No'])
            cust_from_bom = df_map[['Bom No_K','Cust Code']].drop_duplicates(subset=['Bom No_K'])
            out = out.merge(cust_from_bom, on='Bom No_K', how='left', suffixes=('',''))
            if 'Cust Code_y' in out.columns:
                if 'Cust Code' in out.columns:
                    out['Cust Code'] = out['Cust Code'].fillna(out['Cust Code_y'])
                else:
                    out.rename(columns={'Cust Code_y':'Cust Code'}, inplace=True)
                out.drop(columns=['Cust Code_y'], inplace=True, errors='ignore')
            out['Cust Code'] = out.get('Cust Code', np.nan)
            out['Cust Code'] = out['Cust Code'].fillna(out.get('Bom No', '').astype(str).str[:3])
            upperize(out, ['Cust Code'])

            # code จาก PN เต็ม
            df_map['__codeK'] = (
                df_map['Package Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                df_map['Cust Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                df_map['Product Number'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)
            )
            out['__code_targetK'] = (
                out['Package Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                out['Cust Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                out['Product Number'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)
            )

            need = out['#OF DIE'].isna()
            if need.any():
                code_join = df_map[['__codeK','#OF DIE']].drop_duplicates(subset=['__codeK']).copy()
                code_join = code_join.rename(columns={'#OF DIE':'die_tmp'})
                merged_code = out.loc[need, ['__code_targetK']].merge(
                    code_join, left_on='__code_targetK', right_on='__codeK', how='left'
                )
                idx_need = out.index[need]
                prev = out.loc[idx_need, '#OF DIE'].to_numpy()
                cand = merged_code['die_tmp'].to_numpy()
                out.loc[idx_need, '#OF DIE'] = np.where(pd.isna(prev), cand, prev)

            # ถ้ายังไม่เจอ ลอง CODE ด้วย PN_BASE
            if out['#OF DIE'].isna().any() and 'PN_BASE_K' in out.columns and 'PN_BASE_K' in df_map.columns:
                df_map['__codeBaseK'] = (
                    df_map['Package Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                    df_map['Cust Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                    df_map['PN_BASE_K'].astype(str)
                )
                out['__code_targetBaseK'] = (
                    out['Package Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                    out['Cust Code'].astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True) + '_' +
                    out['PN_BASE_K'].astype(str)
                )
                need2 = out['#OF DIE'].isna()
                code_join2 = df_map[['__codeBaseK','#OF DIE']].drop_duplicates(subset=['__codeBaseK']).copy()
                code_join2 = code_join2.rename(columns={'#OF DIE':'die_tmp'})
                merged_code2 = out.loc[need2, ['__code_targetBaseK']].merge(
                    code_join2, left_on='__code_targetBaseK', right_on='__codeBaseK', how='left'
                )
                idx_need2 = out.index[need2]
                prev2 = out.loc[idx_need2, '#OF DIE'].to_numpy()
                cand2 = merged_code2['die_tmp'].to_numpy()
                out.loc[idx_need2, '#OF DIE'] = np.where(pd.isna(prev2), cand2, prev2)

            # ลบคอลัมน์ช่วย
            out.drop(columns=[c for c in ['__code_targetK','__code_targetBaseK','Bom No_K'] if c in out.columns], inplace=True, errors='ignore')
            df_map.drop(columns=[c for c in ['__codeK','__codeBaseK','Bom No_K'] if c in df_map.columns], inplace=True, errors='ignore')

        # สรุปผลลัพธ์
        if 'Bom No' in out.columns:
            out['Cust'] = out['Bom No'].astype(str).str[:3]
        if 'Device' in out.columns and 'Product Number' not in out.columns:
            out['Product Number'] = out['Device']
        if 'Device' in out.columns:
            out.drop(columns=['Device'], inplace=True, errors='ignore')

        # จัดคอลัมน์และบันทึก
        column_order = [
            'Cust','Package Code','Product Number','Bom No','Bom Rev',
            'Machine Model','Operation','Optn_Code','#OF DIE',
            'UPH','DataPoints_Before','DataPoints_After','Outliers_Removed'
        ]
        column_order = [c for c in column_order if c in out.columns]
        out = out[column_order]
        out['#OF DIE'] = pd.to_numeric(out['#OF DIE'], errors='coerce')

        output_dir = os.path.dirname(average_file)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        mapped_file = os.path.join(output_dir, f"Da_Web_{ts}.xlsx")
        out.to_excel(mapped_file, index=False, engine='openpyxl')

        print(f"✅ เติม #OF DIE: {int(out['#OF DIE'].notna().sum())}/{len(out)} แถว → {mapped_file}")
        return mapped_file

    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดในการ map ข้อมูล: {e}")
        return average_file

def DA_AUTO_UPH(file_path, temp_root, start_date=None, end_date=None):
    """ฟังก์ชันหลักสำหรับประมวลผล Die Attack"""
    try:
        # ตรวจสอบ input type
        if isinstance(file_path, list):
            if len(file_path) == 0:
                print("❌ ไม่มีไฟล์ในรายการ")
                return None
            actual_file_path = file_path[0]  # ใช้ไฟล์แรก
            print(f"⚠️ รับรายการไฟล์ ({len(file_path)} ไฟล์) ใช้ไฟล์แรก: {actual_file_path}")
        else:
            actual_file_path = file_path

        df_cleaned, grouped_average, used_start_date, used_end_date = process_die_attack_data(
            actual_file_path, start_date, end_date)

        cleaned_file, average_file = save_results(
            df_cleaned, grouped_average, used_start_date, used_end_date, temp_root)

        print(f"ช่วงวันที่: {used_start_date} ถึง {used_end_date}")
        
        if not os.path.exists(average_file):
            print("❌ ไม่พบไฟล์ average_file")
            return None

        # Map ข้อมูลเพิ่มเติม
        mapped_file = map_data(average_file)      
        print(f"📁 ส่งคืนไฟล์: {mapped_file}")
        return mapped_file

    except Exception as e:
        print(f"❌ DA_AUTO_UPH error: {e}")
        return None

