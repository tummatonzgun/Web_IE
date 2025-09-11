import pandas as pd
import os
from datetime import datetime
import re

class WireBondingAnalyzer:
    def __init__(self):
        self.nobump_df = None
        self.wb_data = None
        self.efficiency_df = None
        self.raw_data = None
    
    def normalize_model_name(self, model_name):
        """ทำความสะอาดและรวมชื่อรุ่นเครื่องที่คล้ายกัน"""
        if not isinstance(model_name, str):
            model_name = str(model_name)
        model_name = model_name.strip().upper()
        if 'WB3100' in model_name:
            return 'WB3100'
        elif 'WB3200' in model_name:
            return 'WB3200'
        elif 'WB3300' in model_name:
            return 'WB3300'
        else:
            return model_name
        
    def normalize_optn_code(self, optn_name):
        """ทำความสะอาดและรวมรหัส Option Code ที่คล้ายกัน"""
        if not isinstance(optn_name, str):
            optn_name = str(optn_name)

        optn_name = optn_name.strip().upper()

        mapping = {
         "L/B-ROV-CU": "W/B-ROV-CU",
         "L/B-ROVING": "W/B-ROV",
        }

        for key, value in mapping.items():
            if optn_name in key:
                return value
            
        for key, value in mapping.items():
            if key in optn_name:  # optn_name = "W/B-ROV-CU"
                return value

        return optn_name
        
    def clean_model_names(self, df):
        """ทำความสะอาดชื่อรุ่นเครื่อง"""
        df = df.copy()
        if 'machine_model' in df.columns:
            df['machine_model'] = df['machine_model'].apply(self.normalize_model_name)
        if 'optn_code' in df.columns:
            df['optn_code'] = df['optn_code'].apply(self.normalize_optn_code)
        return df
    
    def find_wire_data_file(self, directory_path=None):
        """หาไฟล์ Wire Data จาก data_MAP"""
        wire_data_path = r"C:\Users\41800558\Documents\GitHub\NEW_WEB\Webapp\src\data_MAP\Book6_Wire Data.xlsx"
        if os.path.exists(wire_data_path):
            return wire_data_path
        print(f"❌ ไม่พบไฟล์ Wire Data ที่ path: {wire_data_path}")
        return None
    
    def load_data(self, uph_path, wire_data_path=None):
        """โหลดข้อมูลที่จำเป็น"""
        try:
            # หา wire_data_path ถ้าไม่ระบุ
            if wire_data_path is None:
                wire_data_path = self.find_wire_data_file()
                if wire_data_path is None:
                    print("❌ ไม่พบไฟล์ Wire Data")
                    return False

            # โหลด Wire Data
            print(f"📊 Loading Wire data from: {os.path.basename(wire_data_path)}")
            self.nobump_df = pd.read_excel(wire_data_path)
            self.nobump_df.columns = (
                self.nobump_df.columns
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('-', '_')
            )

            # Map คอลัมน์ Wire Data
            col_map = {}
            for col in self.nobump_df.columns:
                norm = col.replace('_', '').replace(' ', '').lower()
                if norm in ['bomno', 'bom', 'bom_no']:
                    col_map[col] = 'bom_no'
                elif norm in ['numberrequired', 'number_required']:
                    col_map[col] = 'number_required'
                elif norm in ['nobump', 'no_bump']:
                    col_map[col] = 'no_bump'
                elif norm in ['itemno', 'item_no']:
                    col_map[col] = 'item_no'
                elif norm in ['matsize', 'mat_size']:
                    col_map[col] = 'mat_size'
                elif norm in ['optncode', 'optn_code']:
                    col_map[col] = 'optn_code'
            self.nobump_df.rename(columns=col_map, inplace=True)
            if 'bom_no' in self.nobump_df.columns:
                self.nobump_df['bom_no'] = self.nobump_df['bom_no'].astype(str).str.strip().str.upper()
            print(f"✅ Wire data loaded: {len(self.nobump_df)} rows")

            # โหลด UPH Data
            print(f"📊 Loading UPH data from: {os.path.basename(uph_path)}")
            ext = os.path.splitext(uph_path)[-1].lower()
            if ext == '.csv':
                self.raw_data = pd.read_csv(uph_path, encoding='utf-8-sig')
            elif ext in ['.xlsx', '.xls']:
                self.raw_data = pd.read_excel(uph_path)
            elif ext == '.json':
                self.raw_data = pd.read_json(uph_path)
            else:
                print(f"❌ Unsupported file type: {ext}")
                return False

            # ทำความสะอาดคอลัมน์ UPH
            self.raw_data.columns = (
                self.raw_data.columns
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('-', '_')
            )

            # Map คอลัมน์ UPH
            col_map = {}
            for col in self.raw_data.columns:
                norm = col.replace('_', '').lower()
                if norm in ['machinemodel', 'model']:
                    col_map[col] = 'machine_model'
                elif norm in ['bomno', 'bom', 'bom_no']:
                    col_map[col] = 'bom_no'
                elif norm == 'uph':
                    col_map[col] = 'uph'
                elif norm in ['optncode', 'optn_code']:
                    col_map[col] = 'optn_code'
                elif norm == 'operation':
                    col_map[col] = 'operation'
                elif norm in ['itemno', 'item_no']:
                    col_map[col] = 'item_no'
                elif norm in ['matsize', 'mat_size']:
                    col_map[col] = 'mat_size'
            self.raw_data.rename(columns=col_map, inplace=True)
            print(f"✅ UPH data loaded: {len(self.raw_data)} rows")

            # ตรวจสอบคอลัมน์ที่จำเป็น
            required_columns = ['uph', 'machine_model', 'bom_no']
            missing_columns = [col for col in required_columns if col not in self.raw_data.columns]
            if missing_columns:
                print(f"❌ Missing required columns: {missing_columns}")
                print(f"📋 Available columns: {list(self.raw_data.columns)}")
                return False

            print("✅ Data loaded successfully!")
            return True

        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def calculate_wire_per_unit(self, bom_no, optn_code=None):
        """คำนวณจำนวนสายต่อหน่วย โดย map optn_code ด้วย"""
        try:
            bom_no = str(bom_no).strip().upper()
            df = self.nobump_df.copy()
            if optn_code:
                optn_code = str(optn_code).strip().upper()
                df_bom = df[df['bom_no'].astype(str).str.strip().str.upper() == bom_no]
                match = df_bom[df_bom.apply(lambda row: self.match_mat_size_with_optn_code(str(row['mat_size']), optn_code), axis=1)]
                if match.empty:
                    match = df_bom
                bom_data = match
            else:
                bom_data = df[df['bom_no'].astype(str).str.strip().str.upper() == bom_no]
            if bom_data.empty:
                return 1.0
            no_bump = float(bom_data['no_bump'].iloc[0]) if 'no_bump' in bom_data.columns else 0
            num_required = float(bom_data['number_required'].iloc[0]) if 'number_required' in bom_data.columns else 0
            wire_per_unit = (no_bump / 2) + num_required
            return wire_per_unit if wire_per_unit > 0 else 1.0
        except Exception as e:
            print(f"❌ Error calculating wire per unit for BOM {bom_no}: {e}")
            return 1.0
    
    # ✅ เพิ่มฟังก์ชันที่ขาดหาย
    def get_no_bump_for_bom(self, bom_no):
        """ดึงค่า no_bump สำหรับ BOM ที่ระบุ"""
        try:
            bom_no = str(bom_no).strip().upper()
            df = self.nobump_df.copy()
            bom_data = df[df['bom_no'].astype(str).str.strip().str.upper() == bom_no]
            if bom_data.empty:
                return 0
            no_bump = float(bom_data['no_bump'].iloc[0]) if 'no_bump' in bom_data.columns else 0
            return int(no_bump) if no_bump.is_integer() else no_bump
        except Exception as e:
            print(f"❌ Error getting no_bump for BOM {bom_no}: {e}")
            return 0

    def get_number_required_for_bom(self, bom_no):
        """ดึงค่า number_required สำหรับ BOM ที่ระบุ"""
        try:
            bom_no = str(bom_no).strip().upper()
            df = self.nobump_df.copy()
            bom_data = df[df['bom_no'].astype(str).str.strip().str.upper() == bom_no]
            if bom_data.empty:
                return 0
            number_required = float(bom_data['number_required'].iloc[0]) if 'number_required' in bom_data.columns else 0
            return int(number_required) if number_required.is_integer() else number_required
        except Exception as e:
            print(f"❌ Error getting number_required for BOM {bom_no}: {e}")
            return 0

    def get_wire_info_for_bom_optn(self, bom_no, optn_code):
        """ดึง ITEM_NO, MAT_SIZE, NO_BUMP, NUMBER_REQUIRED สำหรับ BOM และ optn_code ที่ระบุ
        ถ้า optn_code ไม่ตรง ให้คืนค่าแรกที่เจอของ BOM นั้น"""
        try:
            bom_no = str(bom_no).strip().upper()
            optn_code = str(optn_code).strip().upper()
            df = self.nobump_df.copy()
            # กรอง BOM ก่อน
            df_bom = df[df['bom_no'].astype(str).str.strip().str.upper() == bom_no]
            # ใช้ match_mat_size_with_optn_code ในการกรอง
            match = df_bom[df_bom.apply(lambda row: self.match_mat_size_with_optn_code(str(row['mat_size']), optn_code), axis=1)]
            # ถ้าไม่เจอ optn_code ให้คืนค่าแรกของ BOM นั้น
            if match.empty:
                match = df_bom
            if not match.empty:
                item_no = match['item_no'].iloc[0] if 'item_no' in match.columns else None
                mat_size = match['mat_size'].iloc[0] if 'mat_size' in match.columns else None
                no_bump = match['no_bump'].iloc[0] if 'no_bump' in match.columns else None
                number_required = match['number_required'].iloc[0] if 'number_required' in match.columns else None
                return item_no, mat_size, no_bump, number_required
            return None, None, None, None
        except Exception as e:
            print(f"❌ Error getting wire info for BOM {bom_no} & optn_code {optn_code}: {e}")
            return None, None, None, None

    def remove_outliers(self, df):
        """ลบ outliers จากข้อมุล"""
        try:
            if df.empty:
                return df, {}
            df = self.clean_model_names(df)
            # ตรวจสอบคอลัมน์ที่จำเป็น
            required_cols = ['uph', 'machine_model', 'bom_no']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise KeyError(f"Missing required columns: {missing_cols}")
            # แบ่งข้อมูลตาม BOM, Machine Model, และ Optn_Code
            grouped = df.groupby(['bom_no', 'machine_model', 'optn_code'])
            cleaned_data = []
            outlier_info = {}
            for (bom_no, model, optn_code), group_data in grouped:
                group_data = group_data.copy()
                original_count = len(group_data)
                # ข้ามถ้าข้อมูลน้อยกว่า 15 จุด
                if len(group_data) < 15:
                    cleaned_data.append(group_data)
                    outlier_info[(bom_no, model, optn_code)] = {
                        'original_count': original_count,
                        'removed_count': 0,
                        'final_count': original_count
                    }
                    continue
                # ✅ ใช้ IQR อย่างเดียว
                current_data = group_data
                max_iterations = 10
                for iteration in range(max_iterations):
                    before_count = len(current_data)
                    Q1 = current_data['uph'].quantile(0.25)
                    Q3 = current_data['uph'].quantile(0.75)
                    IQR = Q3 - Q1
                    filtered_data = current_data[
                        (current_data['uph'] >= Q1 - 1.5*IQR) & 
                        (current_data['uph'] <= Q3 + 1.5*IQR)
                    ]
                    after_count = len(filtered_data)
                    if after_count == before_count or after_count < 5 or (before_count - after_count) / before_count > 0.5:
                        break
                    current_data = filtered_data
                cleaned_data.append(current_data)
                final_count = len(current_data)
                outlier_info[(bom_no, model, optn_code)] = {
                    'original_count': original_count,
                    'removed_count': original_count - final_count,
                    'final_count': final_count
                }
            result_df = pd.concat(cleaned_data) if cleaned_data else df
            return result_df, outlier_info
        except Exception as e:
            print(f"❌ Error in remove_outliers: {e}")
            return df, {}
        
    def match_mat_size_with_optn_code(self, mat_size, optn_code):
        # ดึงขนาดลวดจาก optn_code เช่น "2.0MIL"
        optn_mil = re.search(r'(\d+(\.\d+)?)', optn_code)
        mat_mil = re.search(r'(\d+(\.\d+)?)', mat_size)
        if optn_mil and mat_mil:
            return float(optn_mil.group(1)) == float(mat_mil.group(1))
        return False
    
    def preprocess_data(self, start_date=None, end_date=None):
        """ประมวลผลข้อมูลเบื้องต้น"""
        try:
            if self.raw_data is None:
                raise ValueError("No data loaded")
            df = self.raw_data.copy()
            # ตรวจสอบคอลัมน์ที่จำเป็น
            required_cols = ['uph', 'machine_model', 'bom_no']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise KeyError(f"Missing required columns: {missing_cols}")
            # ทำความสะอาดข้อมูล
            df['uph'] = pd.to_numeric(df['uph'], errors='coerce')
            df['bom_no'] = df['bom_no'].astype(str).str.strip().str.upper()
            df = df.dropna(subset=['uph', 'bom_no'])
            # กรองตามวันที่
            if start_date and end_date:
                print(f"📅 Filtering by date: {start_date} - {end_date}")
                date_cols = [col for col in df.columns if 'date' in col or 'time' in col]
                for col in date_cols:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        start_dt = pd.to_datetime(start_date)
                        end_dt = pd.to_datetime(end_date)
                        df = df[(df[col] >= start_dt) & (df[col] <= end_dt)]
                        print(f"✅ Date filter applied: {len(df)} rows remaining")
                        break
                    except Exception:
                        continue
            df = self.clean_model_names(df)
            self.wb_data = df
            return True
        except Exception as e:
            print(f"❌ Error in preprocess_data: {e}")
            return False
    
    def calculate_efficiency(self, start_date=None, end_date=None):
        """คำนวณประสิทธิภาพการทำงาน"""
        try:
            print(f"🔄 Starting efficiency calculation...")
            if not self.preprocess_data(start_date=start_date, end_date=end_date):
                print(f"❌ Preprocess failed")
                return None
            print(f"📊 Data shape: {self.wb_data.shape}")
            # ตัด Outliers
            cleaned_data, outlier_info = self.remove_outliers(self.wb_data)
            if cleaned_data.empty:
                print(f"❌ No data after outlier removal")
                return None
            print(f"📊 After outlier removal: {cleaned_data.shape}")
            # คำนวณประสิทธิภาพแต่ละกลุ่ม
            grouped = cleaned_data.groupby(['bom_no', 'machine_model', 'optn_code'])
            results = []
            print(f"📊 Processing {len(grouped)} groups...")
            for i, ((bom_no, model, optn_code), group) in enumerate(grouped):
                try:
                    if i > 0 and i % 50 == 0:
                        print(f"⏳ Progress: {i}/{len(grouped)} groups processed...")
                    mean_uph = group['uph'].mean()
                    count = len(group)
                    operation = group['operation'].iloc[0] if 'operation' in group.columns else 'WB'
                    optn_code_val = group['optn_code'].iloc[0] if 'optn_code' in group.columns else 'N/A'
                    # ดึงข้อมูลลวด
                    item_no, mat_size, no_bump, number_required = self.get_wire_info_for_bom_optn(bom_no, optn_code_val)
                    # เงื่อนไข: ถ้ามีตัวเลขใน optn_code และ mat_size แต่ตัวเลขไม่ตรงกัน ให้ข้ามกลุ่มนี้
                    optn_mil = re.search(r'(\d+(\.\d+)?)', str(optn_code_val))
                    mat_mil = re.search(r'(\d+(\.\d+)?)', str(mat_size))
                    if optn_mil and mat_mil:
                        if float(optn_mil.group(1)) != float(mat_mil.group(1)):
                            continue  # ข้ามกลุ่มนี้
                    # เงื่อนไข: ถ้า optn_code มี "CU" แต่ item_no ไม่ขึ้นต้นด้วย "WZ" ให้ข้ามกลุ่มนี้
                    if "CU" in str(optn_code_val).upper() and (item_no is not None) and (not str(item_no).upper().startswith("WZ")):
                        # ทำเหมือนกรณี map ไม่เจอ
                        item_no = None
                        mat_size = None
                        no_bump = None
                        number_required = None
                        wire_per_unit = None
                        efficiency = None
                    else:
                        if item_no is None or mat_size is None or no_bump is None or number_required is None:
                         wire_per_unit = None
                         efficiency = None
                        else:
                         wire_per_unit = self.calculate_wire_per_unit(bom_no, optn_code_val)
                         efficiency = mean_uph / wire_per_unit if wire_per_unit and wire_per_unit > 0 else None
                    # ถ้า map ไม่เจอ
                    if item_no is None or mat_size is None or no_bump is None or number_required is None:
                        wire_per_unit = None
                        efficiency = None
                    else:
                        wire_per_unit = self.calculate_wire_per_unit(bom_no, optn_code_val)
                        efficiency = mean_uph / wire_per_unit if wire_per_unit and wire_per_unit > 0 else None
                    outlier_data = outlier_info.get((bom_no, model, optn_code_val), {
                        'original_count': count,
                        'removed_count': 0,
                        'final_count': count
                    })
                    result_entry = {
                        'BOM': bom_no,
                        'Model': model,
                        'Operation': operation,
                        'Optn_Code': optn_code_val,
                        'ITEM_NO': item_no,
                        'MAT_SIZE': mat_size,
                        'NO_BUMP': no_bump,
                        'NO_WIRE': number_required,
                        'Wire_Per_Unit': round(wire_per_unit, 2) if wire_per_unit is not None else None,
                        'Wire Per Hour': round(mean_uph, 2),
                        'UPH': round(efficiency, 3) if efficiency is not None else None,
                        'Data_Points': count,
                        'Original_Count': outlier_data['original_count'],
                        'Outliers_Removed': outlier_data['removed_count']
                    }
                    results.append(result_entry)
                except Exception as group_error:
                    print(f"❌ Error processing group {bom_no}-{model}: {group_error}")
                    continue
            if not results:
                print(f"❌ No results generated")
                return None
            self.efficiency_df = pd.DataFrame(results)
            print(f"✅ Generated {len(self.efficiency_df)} results")
            return self.efficiency_df
        except Exception as e:
            print(f"❌ Error in calculate_efficiency: {e}")
            return None
    
    def export_to_excel(self, file_path=None):
        """ส่งออกผลลัพธ์เป็น Excel"""
        try:
            if self.efficiency_df is None or self.efficiency_df.empty:
                print(f"❌ No data to export")
                return False
            # สร้างโฟลเดอร์ output
            if file_path is None:
                output_dir = 'output_WB_AUTO_UPH'
                os.makedirs(output_dir, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_path = os.path.join(output_dir, f'wb_analysis_results_{timestamp}.xlsx')
            else:
                output_directory = os.path.dirname(file_path)
                if output_directory and not os.path.exists(output_directory):
                    os.makedirs(output_directory)
            print(f"💾 Exporting to: {file_path}")
            # สร้างไฟล์ Excel
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Sheet 1: ผลลัพธ์หลัก
                print(f"✏️ Writing UPH_Results sheet...")
                self.efficiency_df.to_excel(writer, sheet_name='UPH_Results', index=False)
        
            # ตรวจสอบไฟล์ที่สร้าง
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                file_size = os.path.getsize(file_path)
                print(f"✅ Excel file created successfully")
                print(f"✅ File created successfully: {file_path} (size: {file_size} bytes)")
                return True
            else:
                print(f"❌ File creation failed")
                return False

        except Exception as e:
            print(f"❌ Export error: {e}")
            return False

# === Web Interface Functions ===
def get_available_uph_files():
    """ดึงรายชื่อไฟล์ UPH สำหรับเว็บ"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(current_dir)
        uph_dir = os.path.join(src_dir, "data_WB")
        
        if not os.path.exists(uph_dir):
            return []
        
        uph_files = []
        for filename in os.listdir(uph_dir):
            if (filename.lower().endswith(('.xlsx', '.xls')) and 
                ('uph' in filename.lower() or 'wb' in filename.lower())):
                uph_files.append({
                    'filename': filename,
                    'filepath': os.path.join(uph_dir, filename),
                    'size': os.path.getsize(os.path.join(uph_dir, filename))
                })
        
        uph_files.sort(key=lambda x: x['filename'])
        return uph_files
        
    except Exception as e:
        print(f"❌ Error getting UPH files: {e}")
        return []

def get_wire_data_file():
    """ดึง path ของไฟล์ Wire Data"""
    try:
        wire_data_path = r"C:\Users\41800558\Documents\GitHub\NEW_WEB\Webapp\src\data_MAP\Book6_Wire Data.xlsx"
        if os.path.exists(wire_data_path):
            return {
                'filename': os.path.basename(wire_data_path),
                'filepath': wire_data_path
            }
        return None
    except Exception as e:
        print(f"❌ Error getting Wire data file: {e}")
        return None

def run_wb_auto_uph_web(selected_uph_file, output_filename=None, start_date=None, end_date=None):
    """รัน WB_AUTO_UPH สำหรับเว็บ"""
    try:
        print(f"🚀 Starting WB_AUTO_UPH Web Analysis...")
        
        # หาไฟล์ Wire Data
        wire_data = get_wire_data_file()
        if not wire_data:
            return {
                'success': False,
                'error': 'ไม่พบไฟล์ Wire Data'
            }
        
        # หา path ของไฟล์ UPH
        current_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(current_dir)
        uph_path = os.path.join(src_dir, "data_WB", selected_uph_file)
        
        if not os.path.exists(uph_path):
            return {
                'success': False,
                'error': f'ไม่พบไฟล์ UPH: {selected_uph_file}'
            }
        
        print(f"📁 Files: Wire Data: {wire_data['filename']}, UPH: {selected_uph_file}")
        
        # สร้าง analyzer
        analyzer = WireBondingAnalyzer()
        
        # โหลดข้อมูล
        if not analyzer.load_data(uph_path, wire_data['filepath']):
            return {
                'success': False,
                'error': 'ไม่สามารถโหลดข้อมูลได้'
            }
        
        # คำนวณประสิทธิภาพ
        efficiency_df = analyzer.calculate_efficiency(start_date=start_date, end_date=end_date)
        
        if efficiency_df is None or efficiency_df.empty:
            return {
                'success': False,
                'error': 'ไม่สามารถคำนวณประสิทธิภาพได้'
            }
        
        # สร้างชื่อไฟล์ output
        if not output_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"WB_Analysis_{timestamp}.xlsx"
        elif not output_filename.endswith('.xlsx'):
            output_filename += '.xlsx'
        
        # สร้างโฟลเดอร์ output
        temp_dir = os.path.join(src_dir, "temp")
        os.makedirs(temp_dir, exist_ok=True)
        output_path = os.path.join(temp_dir, output_filename)
        
        # Export ไฟล์
        if not analyzer.export_to_excel(output_path):
            return {
                'success': False,
                'error': 'ไม่สามารถส่งออกไฟล์ได้'
            }
        
        # สรุปผลลัพธ์
        total_groups = len(efficiency_df)
        avg_efficiency = efficiency_df['UPH'].mean()
        total_data_points = efficiency_df['Data_Points'].sum()
        total_outliers_removed = efficiency_df['Outliers_Removed'].sum()
        total_original_data = efficiency_df['Original_Count'].sum()
        
        print(f"✅ WB_AUTO_UPH completed successfully!")
        
        return {
            'success': True,
            'message': 'วิเคราะห์ข้อมูล Wire Bond สำเร็จ',
            'output_file': output_filename,
            'output_path': output_path,
            'summary': {
                'total_groups': total_groups,
                'average_efficiency': round(avg_efficiency, 3),
                'outliers_removed': total_outliers_removed,
                'total_original_data': total_original_data,
                'data_quality': round((1 - total_outliers_removed/total_original_data) * 100, 2) if total_original_data > 0 else 0,
                'total_data_points': total_data_points
            },
            'wire_data_file': wire_data['filename'],
            'uph_data_file': selected_uph_file
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return {
            'success': False,
            'error': f'เกิดข้อผิดพลาด: {str(e)}'
        }
def map_data(results_file):
    """Map ข้อมูลเพิ่มเติมจากไฟล์ Part bom pkg"""
    print("=== Map ข้อมูลเพิ่มเติม WB ===")
    
    try:
        # โหลดไฟล์ results
        if 'UPH_Results' in pd.ExcelFile(results_file).sheet_names:
            df_results = pd.read_excel(results_file, sheet_name='UPH_Results', engine='openpyxl')
        else:
            df_results = pd.read_excel(results_file, engine='openpyxl')
        
        print(f"📊 ข้อมูล WB results: {len(df_results)} แถว")

        # หาไฟล์ mapping
        current_dir = os.path.dirname(os.path.abspath(__file__))
        map_folder = os.path.join(current_dir, "..", "data_MAP")

        mapping_file = os.path.join(map_folder, "Part bom pkg.xlsx")
        mapping_file2 = os.path.join(map_folder, "DIE_ATTACH_Fallout_P08.xlsx")  # ✅ แก้ไขชื่อไฟล์

        if not os.path.exists(mapping_file):
            print(f"⚠️ ไม่พบไฟล์: {mapping_file}")
            return results_file

        # โหลดไฟล์ mapping
        df_map = pd.read_excel(mapping_file, engine='openpyxl')
        print(f"📊 ข้อมูล mapping: {len(df_map)} แถว")

        # ทำความสะอาดข้อมูล
        df_map.columns = df_map.columns.str.strip()
        
        # สร้าง bom_no column
        if 'bom_no' not in df_map.columns:
            if 'BOM_NO' in df_map.columns:
                df_map['bom_no'] = df_map['BOM_NO']
            elif 'BOM' in df_map.columns:
                df_map['bom_no'] = df_map['BOM']
            else:
                print(f"⚠️ ไม่พบคอลัมน์ BOM")
                return results_file

        # ทำความสะอาด BOM
        df_map['bom_no'] = df_map['bom_no'].astype(str).str.strip().str.upper()
        df_results['BOM'] = df_results['BOM'].astype(str).str.strip().str.upper()

        # ตรวจสอบคอลัมน์ที่จำเป็น
        required_cols = ["Package Code", "Cust Code", "Product Number"]
        if all(col in df_map.columns for col in required_cols):
            # สร้าง Device ID
            df_map["Device"] = df_map[required_cols].astype(str).agg('_'.join, axis=1)
            
            # เลือกคอลัมน์ที่ต้องการ
            map_cols = ["bom_no"] + required_cols + ["Device"]
            
            df_map_selected = df_map[map_cols]
        else:
            print(f"⚠️ ไม่พบคอลัมน์ที่จำเป็น")
            available_cols = ["bom_no"]
            for col in df_map.columns:
                if any(keyword in col.lower() for keyword in ['package', 'cust', 'product', 'device', 'wire']):
                    available_cols.append(col)
            df_map_selected = df_map[available_cols[:6]]

        # Merge ข้อมูล
        print(f"🔗 กำลัง merge ข้อมูล...")
        df_merged = df_results.merge(df_map_selected, left_on="BOM", right_on="bom_no", how="left")
        
        if 'bom_no' in df_merged.columns:
            df_merged = df_merged.drop('bom_no', axis=1)
            
        print(f"✅ Map ไฟล์แรกสำเร็จ: {len(df_merged)} แถว")
        
        if 'Device' in df_merged.columns:
            mapped_count = len(df_merged[df_merged['Device'].notna()])
            print(f"📊 Map สำเร็จ: {mapped_count}/{len(df_merged)} แถว")

        # Filter ด้วยไฟล์ที่สอง (ถ้ามี)
        """""
        if os.path.exists(mapping_file2):
            df_map2 = pd.read_excel(mapping_file2, engine='openpyxl')
            print(f"📊 ข้อมูล WB Fallout: {len(df_map2)} แถว")
            
            if "Device" in df_map2.columns and 'Device' in df_merged.columns:
                devices_in_file2 = set(df_map2['Device'].dropna().unique())
                print(f"🔍 Device ในไฟล์ WB Fallout: {len(devices_in_file2)} รายการ")
                
                before_filter = len(df_merged)
                df_merged = df_merged[df_merged['Device'].isin(devices_in_file2)].copy()
                after_filter = len(df_merged)
                
                
                print(f"✅ Filter Device สำเร็จ: {after_filter} แถว")
                print(f"🗑️ ข้อมูลที่ถูกตัดออก: {before_filter - after_filter} แถว")
                
                if after_filter == 0:
                    print("⚠️ ไม่มี Device ที่ตรงกัน - ส่งคืนไฟล์เดิม")
                    return results_file
        else:
            print(f"⚠️ ไม่พบไฟล์ WB Fallout: {mapping_file2}")
        """
        # บันทึกไฟล์ที่ map แล้ว
        output_dir = os.path.dirname(results_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        mapped_file = os.path.join(output_dir, f"WB_mapped_data_{timestamp}.xlsx")
        
        with pd.ExcelWriter(mapped_file, engine='openpyxl') as writer:
            # Sheet หลัก
            df_merged.to_excel(writer, sheet_name='WB_Results_Mapped', index=False)
            # Copy sheet อื่นๆ
            try:
                with pd.ExcelFile(results_file) as xls:
                    for sheet_name in xls.sheet_names:
                        if sheet_name not in ['UPH_Results', 'WB_Results_Mapped']:
                            df_sheet = pd.read_excel(results_file, sheet_name=sheet_name)
                            df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
            except:
                pass
            
            # Mapping Summary
            mapping_summary = {
                'Total_Records': len(df_merged),
                'Successfully_Mapped': len(df_merged[df_merged['Device'].notna()]) if 'Device' in df_merged.columns else 0,
                'Not_Mapped': len(df_merged[df_merged['Device'].isna()]) if 'Device' in df_merged.columns else len(df_merged),
                'Mapping_File': os.path.basename(mapping_file),
                'WB_Fallout_File': os.path.basename(mapping_file2) if os.path.exists(mapping_file2) else 'Not Found',
                'Mapping_Date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            mapping_df = pd.DataFrame.from_dict(mapping_summary, orient='index', columns=['Value'])
            mapping_df.to_excel(writer, sheet_name='Mapping_Summary')
        
        print(f"💾 บันทึกไฟล์ที่ map แล้ว: {mapped_file}")
        return mapped_file

    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาดในการ map ข้อมูล WB: {e}")
        return results_file

def run(input_dir, output_dir, uph_filename=None, wire_filename=None, **kwargs):
    """ฟังก์ชันหลักสำหรับรัน WB_AUTO_UPH"""
    print(f"🚀 Starting WB_AUTO_UPH execution...")
    
    try:
        if not uph_filename:
            raise Exception("ไม่ระบุชื่อไฟล์ UPH")

        # รับ start_date, end_date จาก kwargs
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)

        analyzer = WireBondingAnalyzer()
        
        # สร้าง path ของไฟล์
        uph_file = os.path.join(input_dir, uph_filename)
        wire_file = (os.path.join(input_dir, wire_filename) if wire_filename 
                    else r"C:\Users\41800558\Documents\GitHub\NEW_WEB\Webapp\src\data_MAP\Book6_Wire Data.xlsx")
        
        # ตรวจสอบไฟล์
        if not os.path.exists(uph_file):
            raise Exception(f"ไม่พบไฟล์ UPH: {uph_file}")
        if not os.path.exists(wire_file):
            raise Exception(f"ไม่พบไฟล์ Wire Data: {wire_file}")

        print(f"✅ Files validated")
        
        # โหลดข้อมูล
        if not analyzer.load_data(uph_file, wire_file):
            raise Exception("โหลดข้อมูลไม่สำเร็จ")

        # คำนวณประสิทธิภาพ
        efficiency_df = analyzer.calculate_efficiency(start_date=start_date, end_date=end_date)
        if efficiency_df is None or efficiency_df.empty:
            raise Exception("คำนวณประสิทธิภาพไม่สำเร็จ")

        # สร้างโฟลเดอร์ output
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "WB_AUTO_UPH_RESULT.xlsx")

        # Export ไฟล์
        if not analyzer.export_to_excel(output_path):
            raise Exception("ส่งออกไฟล์ไม่สำเร็จ")

        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise Exception("ไฟล์ผลลัพธ์ไม่ถูกต้อง")

        print(f"✅ WB_AUTO_UPH completed successfully!")
        return output_path

    except Exception as e:
        print(f"❌ WB_AUTO_UPH failed: {e}")
        raise e

def WB_AUTO_UPH(input_path, output_dir, start_date=None, end_date=None):
    """ฟังก์ชัน WB_AUTO_UPH หลัก"""
    try:
        # กรณีที่เป็น list ของไฟล์
        if isinstance(input_path, list):
            result_paths = []
            for f in input_path:
                if os.path.isfile(f):
                    input_dir = os.path.dirname(f)
                    uph_filename = os.path.basename(f)
                    result_path = run(input_dir, output_dir, uph_filename=uph_filename, 
                                    start_date=start_date, end_date=end_date)
                    
                    # เพิ่ม mapping
                    #mapped_path = map_data(result_path)
                    result_paths.append(result_path)

            return result_paths[0] if len(result_paths) == 1 else result_paths

        # กรณีที่เป็นโฟลเดอร์
        elif isinstance(input_path, str) and os.path.isdir(input_path):
            raise Exception("กรุณาเลือกไฟล์ที่ต้องการประมวลผล")

        # กรณีที่เป็นไฟล์เดี่ยว
        elif os.path.isfile(input_path):
            input_dir = os.path.dirname(input_path)
            uph_filename = os.path.basename(input_path)
            result_path = run(input_dir, output_dir, uph_filename=uph_filename, 
                            start_date=start_date, end_date=end_date)
            
            # เพิ่ม mapping
            mapped_path = map_data(result_path)

            print(f"WB_AUTO_UPH completed. Output: {result_path}")
            return mapped_path
        else:
            raise Exception("input_path ไม่ถูกต้อง")

    except Exception as e:
        print(f"❌ WB_AUTO_UPH workflow failed: {e}")
        raise e