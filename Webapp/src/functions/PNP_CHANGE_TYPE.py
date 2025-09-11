import os
import glob
import pandas as pd
import re

def run_all_years(input_path_or_file, output_dir):
    # เพิ่มรองรับ list ของไฟล์
    if isinstance(input_path_or_file, list):
        all_files = input_path_or_file
    elif os.path.isfile(input_path_or_file):
        all_files = [input_path_or_file]
    else:
        all_files = glob.glob(os.path.join(input_path_or_file, "WF size* (UTL1).*"))
    # ไม่ต้องมี target_years
    print(f"กำลังประมวลผลไฟล์จาก {input_path_or_file}")
    print(f"เจอไฟล์ทั้งหมด {len(all_files)} ไฟล์")

    # แยกไฟล์ตามปี
    files_by_year = {}
    for filepath in all_files:
        filename = os.path.basename(filepath)
        match = re.search(r"'(\d{2})", filename)
        if match:
            file_year = 2000 + int(match.group(1))
            files_by_year.setdefault(file_year, []).append(filepath)
        else:
            print(f"⚠️ ไฟล์ {filename} ไม่มีปีในชื่อ")

    df_list = []

    for year in sorted(files_by_year):
        for filepath in files_by_year[year]:
            filename = os.path.basename(filepath)
            month_match = re.search(r"WF size ([^ ]+)", filename)
            month = month_match.group(1) if month_match else "Unknown"

            try:
                if filepath.endswith(('.xls', '.xlsx')):
                    df = pd.read_excel(filepath, engine="openpyxl" if filepath.endswith('.xlsx') else None)
                elif filepath.endswith('.csv'):
                    df = pd.read_csv(filepath)
                else:
                    print(f"❌ ไม่รู้จักฟอร์แมต: {filename}")
                    continue
            except Exception as e:
                print(f"❌ อ่านไฟล์ {filename} ผิดพลาด: {e}")
                continue

            df['month'] = month
            df['file_year'] = year
            df_list.append(df)

    if not df_list:
        print("❌ ไม่มีไฟล์ที่โหลดได้เลย")
        return None  # หรือ return None, "❌ ไม่มีไฟล์ที่โหลดได้เลย"

    df_all = pd.concat(df_list, ignore_index=True)

    # คอลัมน์ที่ต้องใช้
    required_cols = ['cust_code', 'package_code', 'product_no', 'bom_no', 'assy_pack_type', 'start_date', 'month']
    missing = [c for c in required_cols if c not in df_all.columns]
    if missing:
        print(f"❌ คอลัมน์หายไป: {missing}")
        return

    df_all = df_all[required_cols + ['file_year']]

    # แปลง start_date เป็น datetime เพื่อเรียงตามวันที่
    df_all['start_date'] = pd.to_datetime(df_all['start_date'], errors='coerce')

    # จัดเรียงเดือน
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_map = {m: i for i, m in enumerate(month_order, 1)}
    df_all['month_short'] = df_all['month'].str[:3]
    df_all['month_num'] = df_all['month_short'].map(month_map)

    # เรียงตาม BOM → เวลา (ปี → เดือน → วันที่)
    df_all = df_all.sort_values(by=[
        'bom_no', 'package_code', 'product_no', 'cust_code',
        'file_year', 'month_num', 'start_date'
    ]).reset_index(drop=True)

    # จัดข้อมูลสำหรับแสดงผล: วิเคราะห์การเปลี่ยนแปลง assy_pack_type
    group_cols = ['cust_code', 'package_code', 'product_no', 'bom_no']
    
    result_list = []
    
    for name, group in df_all.groupby(group_cols):
        group = group.sort_values('start_date').reset_index(drop=True)
        
        # หาวันที่แรกสุดและวันที่ล่าสุดของ BOM นั้นๆ
        first_record = group.iloc[0]   # record แรกสุด (วันที่เก่าสุด)
        last_record = group.iloc[-1]   # record ล่าสุด (วันที่ใหม่สุด)
        
        # ตรวจสอบว่ามีการเปลี่ยนแปลง assy_pack_type หรือไม่
        unique_types = group['assy_pack_type'].unique()
        
        if len(unique_types) == 1:
            # ไม่มีการเปลี่ยนแปลง assy_pack_type
            row_data = {
                'cust_code': first_record['cust_code'],
                'package_code': first_record['package_code'],
                'product_no': first_record['product_no'],
                'bom_no': first_record['bom_no'],
                'prev_assy_pack_type': first_record['assy_pack_type'],
                'assy_pack_type': first_record['assy_pack_type'],
                'prev_start_date': first_record['start_date'],     # วันที่เจอครั้งแรก
                'start_date': last_record['start_date'],           # วันที่เจอครั้งสุดท้าย
                'prev_month_name': first_record['start_date'].strftime('%b'),
                'curr_month_name': last_record['start_date'].strftime('%b'),
                'change_status': 'No Change'
            }
            result_list.append(row_data)
        else:
            # มีการเปลี่ยนแปลง assy_pack_type
            # หา assy_pack_type แรกและสุดท้าย
            first_assy_type = first_record['assy_pack_type']
            last_assy_type = last_record['assy_pack_type']
            
            row_data = {
                'cust_code': first_record['cust_code'],
                'package_code': first_record['package_code'],
                'product_no': first_record['product_no'],
                'bom_no': first_record['bom_no'],
                'prev_assy_pack_type': first_assy_type,           # assy_pack_type แรก
                'assy_pack_type': last_assy_type,                 # assy_pack_type สุดท้าย
                'prev_start_date': first_record['start_date'],    # วันที่เจอครั้งแรก
                'start_date': last_record['start_date'],          # วันที่เจอครั้งสุดท้าย
                'prev_month_name': first_record['start_date'].strftime('%b'),
                'curr_month_name': last_record['start_date'].strftime('%b'),
                'change_status': 'Changed'
            }
            result_list.append(row_data)
    
    # สร้าง DataFrame จากผลลัพธ์
    summary_df = pd.DataFrame(result_list)
    
    # เรียงเดือนให้ถูกต้องด้วย Categorical
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    summary_df['prev_month_name'] = pd.Categorical(summary_df['prev_month_name'], categories=month_order, ordered=True)
    summary_df['curr_month_name'] = pd.Categorical(summary_df['curr_month_name'], categories=month_order, ordered=True)

    # เรียงตามวันเวลา
    summary_df = summary_df.sort_values(by=['start_date']).reset_index(drop=True)

    # เลือกคอลัมน์ส่งออก
    output_cols = group_cols + [
        'prev_assy_pack_type', 'assy_pack_type',
        'prev_start_date', 'start_date',
        'prev_month_name', 'curr_month_name',
        'change_status'
    ]

    # บันทึกผลลัพธ์ - ✅ เปลี่ยนชื่อไฟล์เป็น Last_Type.xlsx
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "Last_Type.xlsx")  # ✅ เปลี่ยนชื่อไฟล์
    
    # ส่งออก Excel พร้อมจัดความกว้างคอลัมน์
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        summary_df[output_cols].to_excel(writer, index=False, sheet_name='BOM Summary')
        worksheet = writer.sheets['BOM Summary']
        worksheet.set_column('A:K', 15)
    print(f"✅ Output file saved at: {output_file}")  # เพิ่มบรรทัดนี้

    # นับข้อมูล
    changed_count = len(summary_df[summary_df['change_status'] == 'Changed'])
    no_change_count = len(summary_df[summary_df['change_status'] == 'No Change'])

    print(f"✅ บันทึกไฟล์สรุปการเปลี่ยนแปลงไว้ที่: {output_file}")
    print(f"📊 BOM ที่มีการเปลี่ยนแปลง: {changed_count} รายการ")
    print(f"📋 BOM ที่ไม่มีการเปลี่ยนแปลง: {no_change_count} รายการ")
    print(f"📈 รวมทั้งหมด: {len(summary_df)} รายการ")
    
    return output_file  # เปลี่ยนจาก return summary_df เป็น return output_file

def PNP_CHANGE_TYPE(input_path, output_dir):
    return run_all_years(input_path, output_dir)
