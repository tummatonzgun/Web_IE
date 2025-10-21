from datetime import datetime
import os, re
import pandas as pd

class WireBondingAnalyzer:
    def __init__(self):
        self.nobump_df = None
        self.wb_data = None
        self.efficiency_df = None
        self.raw_data = None
        self._wire_size_map = None   # item_no -> size text
        self._wire_mat_map  = None   # item_no -> 'CU' or 'AU'
    
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
        """ทำความสะอาดและรวมรหัส Option Code ที่คล้ายกัน (L/B = W/B)"""
        if not isinstance(optn_name, str):
            optn_name = str(optn_name)

        optn_name = optn_name.strip().upper()

        # NEW: ให้ L/B เท่ากับ W/B (รองรับมี/ไม่มีช่องว่าง)
        optn_name = re.sub(r'\bL\s*/\s*B\b', 'W/B', optn_name)

        # Mapping เพิ่มเติมสำหรับชื่อที่ต้องการรวม
        mapping = {
            "W/B-ROVING": "W/B-ROV",   # รวมคำว่า ROVING ให้เป็น ROV
            "L/B-ROVING": "W/B-ROV",   # เผื่อกรณีที่ยังมี L/B-ROVING โผล่มา
        }

        # คงลูปเดิมไว้ (minimal change)
        for key, value in mapping.items():
            if optn_name in key:
                return value
            
        for key, value in mapping.items():
            if key in optn_name:
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
        """หาไฟล์ Wire Data: ใช้ Part bom pkg.xlsx ภายในโฟลเดอร์ data_MAP ของโปรเจกต์"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            src_dir = os.path.dirname(current_dir)
            wire_data_path = os.path.join(src_dir, "data_MAP", "Part bom pkg.xlsx")
            if os.path.exists(wire_data_path):
                return wire_data_path
            print(f"❌ ไม่พบไฟล์ Part bom pkg.xlsx ใน {os.path.join(src_dir, 'data_MAP')}")
            return None
        except Exception as e:
            print(f"❌ find_wire_data_file error: {e}")
            return None
    
    def load_data(self, uph_path, wire_data_path=None):
        """โหลดข้อมูลที่จำเป็น"""
        try:
            # หา wire_data_path ถ้าไม่ระบุ
            if wire_data_path is None:
                wire_data_path = self.find_wire_data_file()
                if wire_data_path is None:
                    print("❌ ไม่พบไฟล์ Part bom pkg")
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
                elif norm in ['#ofwire1', '#ofwire', 'ofwire1']:
                    col_map[col] = 'number_required'
                elif norm in ['#ofbump1', '#ofbump', 'ofbump1']:
                    col_map[col] = 'no_bump'
                elif norm in ['wire1', 'itemno1', 'item1', 'item_no1']:
                    col_map[col] = 'item_no'
                elif norm in ['#ofwire2', 'ofwire2']:
                    col_map[col] = 'number_required_2'
                elif norm in ['#ofbump2', 'ofbump2']:
                    col_map[col] = 'no_bump_2'            # NEW: bump ของฝั่ง Wire2
                elif norm in ['wire2', 'itemno2', 'item2', 'item_no2']:
                    col_map[col] = 'item_no_2'            # NEW: item ของฝั่ง Wire2
                elif norm in ['bomrev', 'bom_rev']:
                    col_map[col] = 'bom_rev'
                elif norm in ['packagecode', 'package_code', 'pkgcode', 'pkg_code']:
                    col_map[col] = 'package_code'
                elif norm in ['productnumber', 'product_number', 'productno', 'product_no']:
                    col_map[col] = 'product_number'
            self.nobump_df.rename(columns=col_map, inplace=True)
            # มาตรฐานคีย์
            if 'bom_no' in self.nobump_df.columns:
                self.nobump_df['bom_no'] = self.nobump_df['bom_no'].astype(str).str.strip().str.upper()

            # ปรับเป็นตัวพิมพ์ใหญ่ให้ครบ รวม cust_code ด้วย
            for k in ['bom_rev', 'package_code', 'product_number', 'cust_code']:
                if k in self.nobump_df.columns:
                    self.nobump_df[k] = self.nobump_df[k].astype(str).str.strip().str.upper()

            # ให้จำนวนเป็นตัวเลข
            for k in ['no_bump', 'number_required', 'number_required_2']:
                if k in self.nobump_df.columns:
                    self.nobump_df[k] = pd.to_numeric(self.nobump_df[k], errors='coerce')

            # NEW: สร้างคอลัมน์ Code = Package Code_Cust Code_Product Number (เฉพาะแถวที่ครบ)
            def _compose_row_code(row):
                pc = row.get('package_code')
                cc = row.get('cust_code')
                pn = row.get('product_number')
                if pd.isna(pc) or pd.isna(cc) or pd.isna(pn) or pc == '' or cc == '' or pn == '':
                    return None
                return f"{str(pc).strip().upper()}_{str(cc).strip().upper()}_{str(pn).strip().upper()}"

            if all(c in self.nobump_df.columns for c in ['package_code','cust_code','product_number']):
                self.nobump_df['code'] = self.nobump_df.apply(_compose_row_code, axis=1)

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
                elif norm in ['device']:
                    col_map[col] = 'device'
                elif norm in ['packagecode', 'package_code']:   # FIX: รองรับทั้งสองแบบ
                    col_map[col] = 'package_code'
                elif norm in ['bomrev', 'bom_rev']:              # FIX: รองรับทั้งสองแบบ
                    col_map[col] = 'bom_rev'
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
    
    # ตัวช่วยกรองแถวในไฟล์ Map ด้วยหลายคีย์
    def _filter_map_rows(self, bom_no, bom_rev=None, package_code=None, product_number=None):
        df = self.nobump_df.copy()
        if df is None or df.empty:
            return df.iloc[0:0]
        def norm(v): 
            return str(v).strip().upper()
        mask = (df['bom_no'].astype(str).str.strip().str.upper() == norm(bom_no))
        if bom_rev is not None and 'bom_rev' in df.columns:
            mask &= (df['bom_rev'].astype(str).str.strip().str.upper() == norm(bom_rev))
        if package_code is not None and 'package_code' in df.columns:
            mask &= (df['package_code'].astype(str).str.strip().str.upper() == norm(package_code))
        # Product Number ใน Map
        if product_number is not None:
            if 'product_number' in df.columns:
                mask &= (df['product_number'].astype(str).str.strip().str.upper() == norm(product_number))
        return df[mask]

    # NEW: ใช้เมื่อตรงเป๊ะไม่เจอเท่านั้น – ลดเงื่อนไขลงทีละระดับ
    def _find_map_rows_fallback(self, bom_no, bom_rev=None, package_code=None, product_number=None):
        if self.nobump_df is None or self.nobump_df.empty or not bom_no:
            return self.nobump_df.iloc[0:0], []
        df = self.nobump_df
        def N(v): return str(v).strip().upper() if v is not None else None
        vals = {
            'bom_no': N(bom_no),
            'bom_rev': N(bom_rev),
            'package_code': N(package_code),
            'product_number': N(product_number),
        }
        plans = [
            ['bom_no','bom_rev','package_code','product_number'],
            ['bom_no','bom_rev','package_code'],
            ['bom_no','bom_rev','product_number'],
            ['bom_no','package_code','product_number'],
            ['bom_no','bom_rev'],
            ['bom_no','package_code'],
            ['bom_no','product_number'],
            ['bom_no'],
        ]
        plans = [p for p in plans if all(vals.get(k) is not None for k in p)]
        if not plans:
            plans = [['bom_no']]

        for keys in plans:
            m = pd.Series(True, index=df.index)
            ok = True
            for k in keys:
                if k not in df.columns or vals[k] is None:
                    ok = False; break
                m &= (df[k].astype(str).str.strip().str.upper() == vals[k])
            if not ok:
                continue
            rows = df[m]
            if not rows.empty:
                # ให้ความสำคัญกับแถวที่มี no_bump และ number_required ครบก่อน
                pref = rows
                if 'no_bump' in rows.columns and 'number_required' in rows.columns:
                    pref2 = rows[rows['no_bump'].notna() & rows['number_required'].notna()]
                    if not pref2.empty:
                        pref = pref2
                return pref, keys

        # NEW: สุดท้าย ลองแม็พด้วย Code = PackageCode_CustCode_ProductNumber
        if 'code' in df.columns:
            target_code = self._compose_code_value(
                package_code=package_code,
                product_number=product_number,
                bom_no=bom_no  # ใช้ 3 ตัวแรกเป็น cust ถ้าไม่ได้ส่ง cust_code มา
            )
            if target_code:
                rows = df[df['code'].astype(str).str.strip().str.upper() == target_code]
                if not rows.empty:
                    pref = rows
                    if 'no_bump' in rows.columns and 'number_required' in rows.columns:
                        pref2 = rows[rows['no_bump'].notna() & rows['number_required'].notna()]
                        if not pref2.empty:
                            pref = pref2
                    return pref, ['code']

        return df.iloc[0:0], []
    
    # ===== Helpers เฉพาะกรณี Wire2 =====

    def _wire2_has_value(self, rows):
        try:
            if rows is None or rows.empty or 'number_required_2' not in rows.columns:
                return False
            v = pd.to_numeric(rows['number_required_2'].iloc[0], errors='coerce')
            return pd.notna(v) and float(v) > 0
        except:
            return False

    def _norm(self, s):
        if s is None: return ''
        return re.sub(r'[\s/_\-]+','', str(s)).upper()

    def _extract_size_token(self, text):
        s = self._norm(text)
        m = re.search(r'(\d+(?:\.\d+)?)MIL', s)
        if not m:
            return None
        try:
            val = float(m.group(1))
            return f"{val:.1f}MIL"  # 2MIL -> 2.0MIL
        except:
            return m.group(0)

    def _material_from_optn(self, optn_code):
        s = self._norm(optn_code)
        if 'CU' in s: return 'CU'
        if 'AU' in s: return 'AU'
        if 'WBROV' in s: return 'AU'  # ดีฟอลต์ ROV
        return None

    def _material_from_item_no(self, item_no):
        if not item_no: return None
        s = str(item_no).strip().upper()
        if s.startswith('WZ'): return 'CU'
        if s.startswith('GZ'): return 'AU'
        return None

    def _ensure_wire_size_map(self):
        if hasattr(self, '_wire_size_map') and self._wire_size_map is not None:
            return
        self._wire_size_map = {}
        try:
            base = os.path.dirname(os.path.abspath(__file__))
            data_map_dir = os.path.join(os.path.dirname(base), "data_MAP")
            path = os.path.join(data_map_dir, "Wire size'25.xlsx")
            if not os.path.exists(path):
                return
            with pd.ExcelFile(path) as xf:
                for sheet in xf.sheet_names:
                    key = sheet.strip().lower()
                    if key == "copper wire":
                        df = xf.parse(sheet)
                        cols = {c.strip().lower(): c for c in df.columns}
                        c_item = cols.get("item_no")
                        c_size = cols.get("size_cu")        # โครงสร้างใหม่
                        if not c_item or not c_size: 
                            continue
                        for _, r in df.iterrows():
                            item = str(r[c_item]).strip().upper()
                            size = str(r[c_size]).strip()
                            if item and size:
                                self._wire_size_map[item] = size
                    elif key == "au wire":
                        df = xf.parse(sheet)
                        cols = {c.strip().lower(): c for c in df.columns}
                        c_item = cols.get("item_no")
                        c_size = cols.get("size_au")        # โครงสร้างใหม่
                        if not c_item or not c_size: 
                            continue
                        for _, r in df.iterrows():
                            item = str(r[c_item]).strip().upper()
                            size = str(r[c_size]).strip()
                            if item and size:
                                self._wire_size_map[item] = size
        except Exception as e:
            print(f"⚠️ Wire size'25 load error: {e}")
            self._wire_size_map = {}

    def _decide_wire_index(self, row, optn_code):
        """
        คืน 1 หรือ 2 เลือกเส้นที่ match กับ Optn_Code มากกว่า
        เกณฑ์: วัสดุ (จาก prefix item_no WZ/GZ) และขนาด (จาก Wire size'25.xlsx), เทียบเฉพาะ token ที่ Optn_Code ระบุ
        """
        # เตรียมค่าของสองเส้น
        item1 = row.get('item_no');   item2 = row.get('item_no_2')
        mat1  = self._material_from_item_no(item1);  mat2 = self._material_from_item_no(item2)
        self._ensure_wire_size_map()
        size1 = self._wire_size_map.get(str(item1).upper()) if item1 else None
        size2 = self._wire_size_map.get(str(item2).upper()) if item2 else None

        optn_mat = self._material_from_optn(optn_code)
        optn_size = self._extract_size_token(optn_code)

        def score(mat_item, size_item):
            sc = 0
            if optn_mat and mat_item and optn_mat == mat_item:
                sc += 1
            if optn_size and size_item:
                tok = self._extract_size_token(size_item)
                if tok and tok == optn_size:
                    sc += 1
                elif not tok:
                    # fallback contains
                    if self._norm(optn_size) in self._norm(size_item) or self._norm(size_item) in self._norm(optn_size):
                        sc += 1
            return sc

        s1 = score(mat1, size1)
        s2 = score(mat2, size2)
        return 2 if s2 > s1 else 1  # เสมอให้ 1

    def _select_wire_fields(self, rows, optn_code):
        """
        คืน (item_no_selected, number_required_selected, no_bump_selected, selected_index)
        - ถ้าไม่มี Wire2 → คืนของเส้น 1
        - ถ้ามี Wire2 → ใช้ _decide_wire_index เลือกเส้น และคืนของเส้นนั้น
        """
        if rows is None or rows.empty:
            return None, None, None, 1
        row = rows.iloc[0]
        # defaults = wire1
        item = row.get('item_no')
        nr   = row.get('number_required')
        bump = row.get('no_bump')
        idx  = 1

        if self._wire2_has_value(rows):
            idx = self._decide_wire_index(row, optn_code)
            if idx == 2:
                item = row.get('item_no_2', item)
                nr   = row.get('number_required_2', nr)
                bump = row.get('no_bump_2', bump)
        return item, nr, bump, idx

    # ---------- ใช้ตัวเลือกนี้ทั้งในการคำนวณและแสดงผล ----------
    def calculate_wire_per_unit(self, bom_no, optn_code=None, bom_rev=None, package_code=None, product_number=None):
        """คำนวณจำนวนสายต่อหน่วยจากไฟล์ Map (คง Wire2 เดิม; ใช้ fallback เฉพาะไม่เจอแมตช์ตรง)"""
        try:
            rows = self._filter_map_rows(bom_no, bom_rev=bom_rev, package_code=package_code, product_number=product_number)
            if rows.empty:
                rows, _ = self._find_map_rows_fallback(bom_no, bom_rev=bom_rev, package_code=package_code, product_number=product_number)
            if rows.empty:
                return None

            item, num_required, no_bump, _ = self._select_wire_fields(rows, optn_code)
            if pd.isna(no_bump) or pd.isna(num_required):
                return None
            wire_per_unit = float(no_bump) / 2.0 + float(num_required)
            return wire_per_unit if wire_per_unit > 0 else None
        except Exception as e:
            print(f"❌ Error calculating wire per unit for BOM {bom_no} : {e}")
            return None

    def get_wire_info_for_bom_optn(self, bom_no, optn_code, bom_rev=None, package_code=None, product_number=None):
        """ดึง ITEM_NO, NO_BUMP, NUMBER_REQUIRED (คง Wire2 เดิม; ใช้ fallback เมื่อไม่เจอแมตช์ตรง)"""
        try:
            rows = self._filter_map_rows(bom_no, bom_rev=bom_rev, package_code=package_code, product_number=product_number)
            if rows.empty:
                rows, _ = self._find_map_rows_fallback(bom_no, bom_rev=bom_rev, package_code=package_code, product_number=product_number)
            if rows.empty:
                return None, None, None
            item, num_required, no_bump, _ = self._select_wire_fields(rows, optn_code)
            return item, no_bump, num_required
        except Exception as e:
            print(f"❌ Error getting wire info for BOM {bom_no}: {e}")
            return None, None, None

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

            # ใช้คีย์กลุ่มเดียวกับ calculate_efficiency
            group_keys = ['bom_no', 'machine_model', 'optn_code', 'bom_rev', 'device', 'package_code']
            group_keys = [k for k in group_keys if k in df.columns]  # เผื่อบางคอลัมน์ไม่มี
            grouped = df.groupby(group_keys, dropna=False)

            cleaned_data = []
            outlier_info = {}

            for group_key, group_data in grouped:
                # group_key อาจเป็น str หรือ tuple ขึ้นกับจำนวน key
                if not isinstance(group_key, tuple):
                    group_key = (group_key,)
                group_data = group_data.copy()
                original_count = len(group_data)

                # ข้ามถ้าข้อมูลน้อยกว่า 15 จุด
                if len(group_data) < 15:
                    cleaned_data.append(group_data)
                    outlier_info[group_key] = {
                        'original_count': original_count,
                        'removed_count': 0,
                        'final_count': original_count
                    }
                    continue

                # ใช้ IQR iteratively
                current_data = group_data
                max_iterations = 10
                for _ in range(max_iterations):
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
                outlier_info[group_key] = {
                    'original_count': original_count,
                    'removed_count': original_count - final_count,
                    'final_count': final_count
                }

            result_df = pd.concat(cleaned_data) if cleaned_data else df
            return result_df, outlier_info
        except Exception as e:
            print(f"❌ Error in remove_outliers: {e}")
            return df, {}
    
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
        """คำนวณประสิทธิภาพการทำงาน (พิจารณา Map ตาม BOM + REV + PKG + PRODUCT NUMBER)"""
        try:
            print(f"🔄 Starting efficiency calculation...")
            if not self.preprocess_data(start_date=start_date, end_date=end_date):
                print(f"❌ Preprocess failed")
                return None
            print(f"📊 Data shape: {self.wb_data.shape}")

            cleaned_data, outlier_info = self.remove_outliers(self.wb_data)
            if cleaned_data.empty:
                print(f"❌ No data after outlier removal")
                return None
            print(f"📊 After outlier removal: {cleaned_data.shape}")

            group_keys = ['bom_no', 'machine_model', 'optn_code', 'bom_rev', 'device', 'package_code']
            group_keys = [k for k in group_keys if k in cleaned_data.columns]
            grouped = cleaned_data.groupby(group_keys, dropna=False)

            results = []
            print(f"📊 Processing {len(grouped)} groups...")
            for i, (key_tuple, group) in enumerate(grouped):
                try:
                    if i > 0 and i % 50 == 0:
                        print(f"⏳ Progress: {i}/{len(grouped)} groups processed...")

                    if not isinstance(key_tuple, tuple):
                        key_tuple = (key_tuple,)
                    key_dict = dict(zip(group_keys, key_tuple))
                    bom_no       = key_dict.get('bom_no')
                    model        = key_dict.get('machine_model')
                    optn_code    = key_dict.get('optn_code')
                    bom_rev      = key_dict.get('bom_rev')
                    device       = key_dict.get('device')          # = Product Number
                    package_code = key_dict.get('package_code')

                    mean_uph = group['uph'].mean()
                    count_after = len(group)  # กลุ่มหลังตัด outlier

                    operation = group['operation'].iloc[0] if 'operation' in group.columns else 'WB'
                    optn_code_val = optn_code if optn_code is not None else (group['optn_code'].iloc[0] if 'optn_code' in group.columns else 'N/A')

                    # ดึงข้อมูลจากไฟล์ Map: ITEM_NO, NO_BUMP, NUMBER_REQUIRED
                    item_no, no_bump, number_required = self.get_wire_info_for_bom_optn(
                        bom_no,
                        optn_code_val,
                        bom_rev=bom_rev,
                        package_code=package_code,
                        product_number=device  # device = Product Number
                    )

                    # คำนวณ wire_per_unit และ UPH (จะเป็น None อัตโนมัติถ้า wire2 มีค่า เพราะ no_bump/number_required = None)
                    wire_per_unit = None
                    efficiency = None
                    if (no_bump is not None) and (number_required is not None) and (not pd.isna(no_bump)) and (not pd.isna(number_required)):
                        wire_per_unit = self.calculate_wire_per_unit(
                            bom_no,
                            optn_code_val,
                            bom_rev=bom_rev,
                            package_code=package_code,
                            product_number=device
                        )
                        if wire_per_unit is not None and wire_per_unit > 0:
                            efficiency = mean_uph / wire_per_unit

                    outlier_data = outlier_info.get(tuple(key_dict.get(k) for k in group_keys), {
                        'original_count': count_after,
                        'removed_count': 0,
                        'final_count': count_after
                    })
                    original_count = outlier_data.get('original_count', count_after)
                    final_count = outlier_data.get('final_count', count_after)
                    removed_count = outlier_data.get('removed_count', original_count - final_count)

                    result_entry = {
                        'Cust': str(bom_no)[:3],
                        'Package Code': package_code,
                        'Product Number': device,
                        'Bom No': bom_no,
                        'Bom Rev': bom_rev,
                        'Machine Model': model,
                        'Operation': operation,
                        'Optn_Code': optn_code_val,
                        'Item_No': item_no,
                        '#OF BUMP': no_bump,
                        '#OF WIRE': number_required,
                        'Total WireCount': round(wire_per_unit, 2) if wire_per_unit is not None else None,
                        'WPH': round(mean_uph, 2),
                        'UPH': round(efficiency, 3) if efficiency is not None else None,
                        'DataPoints_Before': original_count,
                        'DataPoints_After': final_count,
                        'Outliers_Removed': removed_count
                    }
                    results.append(result_entry)
                except Exception as group_error:
                    print(f"❌ Error processing group {key_tuple}: {group_error}")
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

    # เพิ่ม helper สร้าง target code สำหรับ fallback จากค่าในกรุป (cust ใช้ 3 ตัวแรกของ BOM ถ้าไม่ได้ส่งมา)
    def _compose_code_value(self, package_code=None, product_number=None, cust_code=None, bom_no=None):
        try:
            cc = (cust_code or (str(bom_no)[:3] if bom_no else None))
            if not package_code or not product_number or not cc:
                return None
            return f"{str(package_code).strip().upper()}_{str(cc).strip().upper()}_{str(product_number).strip().upper()}"
        except Exception:
            return None

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
    """ดึง path ของไฟล์ Wire Data (Part bom pkg.xlsx)"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(current_dir)
        wire_data_path = os.path.join(src_dir, "data_MAP", "Part bom pkg.xlsx")
        if os.path.exists(wire_data_path):
            return {'filename': os.path.basename(wire_data_path), 'filepath': wire_data_path}
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
        # ใช้ Part bom pkg.xlsx ภายในโปรเจกต์เป็นค่าเริ่มต้น
        default_wire = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data_MAP", "Part bom pkg.xlsx")
        wire_file = os.path.join(input_dir, wire_filename) if wire_filename else default_wire
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
            #mapped_path = map_data(result_path)

            print(f"WB_AUTO_UPH completed. Output: {result_path}")
            return result_path
        else:
            raise Exception("input_path ไม่ถูกต้อง")

    except Exception as e:
        print(f"❌ WB_AUTO_UPH workflow failed: {e}")
        raise e