import os
import pandas as pd 

def PNP_BOM_TYPE(input_bom_file, output_dir):
    
    last_type_path = os.path.join(output_dir, "Last_Type.xlsx")
    if not os.path.exists(last_type_path):
        print(f"❌ ไม่พบไฟล์ {last_type_path}")
        return

    df_last = pd.read_excel(last_type_path)
    cols = ['bom_no', 'assy_pack_type']
    df_last = df_last[cols].drop_duplicates()

    # ตรวจสอบ input_bom_file เป็น list หรือไม่
    if isinstance(input_bom_file, list):
        if not input_bom_file:
            print("❌ ไม่พบไฟล์ BOM ที่อัปโหลด")
            return
        input_bom_file = input_bom_file[0]  # ใช้ไฟล์แรก

    df_bom = pd.read_excel(input_bom_file) if input_bom_file.endswith('.xlsx') else pd.read_csv(input_bom_file)
    if 'bom_no' not in df_bom.columns:
        print("❌ ไฟล์ที่อัปโหลดไม่มีคอลัมน์ bom_no")
        return

    merge_cols = ['bom_no']
    if 'package_code' in df_bom.columns and 'product_no' in df_bom.columns:
        merge_cols += ['package_code', 'product_no']

    df_merged = pd.merge(df_bom, df_last, on=merge_cols, how='left')
    return df_merged