import os
from typing import List, Optional

import pandas as pd
from datetime import datetime


# ================================================================
# PNP_PACK_TYPE
# อ่านไฟล์ทั้งหมดใน data_PNP_TYPE -> รวมข้อมูล (start_date, product_no, bom_no, assy_pack_type)
# -> เลือกแถวที่ start_date ล่าสุดต่อ (product_no, bom_no)
# -> นำไปจับกับไฟล์ input ที่มีแค่ product_no, bom_no เพื่อเติม assy_pack_type ล่าสุด
# คืนค่าเป็น DataFrame (ให้ฝั่งเว็บ export เอง)
# ================================================================


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
	df = df.copy()
	df.columns = (
		df.columns
		.str.strip()
		.str.replace("\n", " ")
		.str.replace("-", "_")
		.str.replace("/", "_")
		.str.replace(" ", "_")
		.str.lower()
	)
	return df


def _resolve_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
	cols = {c.lower(): c for c in df.columns}
	# exact name first
	for cand in candidates:
		cand_l = cand.lower()
		if cand_l in cols:
			return cols[cand_l]
	# substring fallback
	for col in df.columns:
		name = col.lower()
		if any(cand.lower() in name for cand in candidates):
			return col
	return None


def _read_any(path: str) -> pd.DataFrame:
	"""อ่านไฟล์ได้ทั้ง .xlsx/.xls/.csv (หากอ่านไม่ได้จะคืน DataFrame ว่าง)"""
	ext = os.path.splitext(path)[1].lower()
	try:
		if ext in [".xlsx", ".xlsm", ".xltx", ".xltm", ".xlsb"]:
			return pd.read_excel(path, engine="openpyxl")
		if ext == ".xls":
			# หมายเหตุ: xlrd>=2.0 ไม่รองรับ .xls แล้ว ถ้าลง xlrd รุ่นใหม่จะอ่านไม่ได้ -> ข้ามไฟล์
			try:
				return pd.read_excel(path, engine="xlrd")
			except Exception:
				return pd.DataFrame()
		if ext == ".csv":
			try:
				return pd.read_csv(path)
			except UnicodeDecodeError:
				# fallback encoding
				return pd.read_csv(path, encoding="latin-1")
		# default: ลองเป็น excel
		return pd.read_excel(path, engine="openpyxl")
	except Exception:
		return pd.DataFrame()


def _extract_core_columns(df: pd.DataFrame) -> pd.DataFrame:
	"""
	ดึงเฉพาะ 4 คอลัมน์ที่สนใจและแปลงชื่อเป็น
	start_date, product_no, bom_no, assy_pack_type
	ถ้าไม่ครบ 4 คอลัมน์ -> คืน DataFrame ว่าง
	"""
	if df is None or df.empty:
		return pd.DataFrame(columns=["start_date", "product_no", "bom_no", "assy_pack_type"]) 

	df = _normalize_columns(df)

	start_col = _resolve_column(df, [
		"start_date", "startdate", "date", "start", "start_time", "start_datetime",
	])
	prod_col = _resolve_column(df, ["product_no", "product", "product_number"])
	bom_col = _resolve_column(df, ["bom_no", "bom"])
	assy_col = _resolve_column(df, ["assy_pack_type", "assy", "pack_type", "assy_pack", "assytype"])

	if not all([start_col, prod_col, bom_col, assy_col]):
		return pd.DataFrame(columns=["start_date", "product_no", "bom_no", "assy_pack_type"]) 

	out = df[[start_col, prod_col, bom_col, assy_col]].copy()
	out.columns = ["start_date", "product_no", "bom_no", "assy_pack_type"]

	# ทำความสะอาดค่า
	out["product_no"] = out["product_no"].astype(str).str.strip()
	out["bom_no"] = out["bom_no"].astype(str).str.strip().str.upper()
	# ทำความสะอาดและ normalize assy_pack_type เพื่อป้องกัน false change
	out["assy_pack_type"] = (
		out["assy_pack_type"].astype("string").str.strip().str.upper()
	)
	# ค่าที่เทียบเท่ากับว่าง/ไม่มี ให้เป็น NA
	out["assy_pack_type"] = out["assy_pack_type"].replace({
		"": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "NULL": pd.NA
	})
	# แปลง start_date เป็น datetime
	out["start_date"] = pd.to_datetime(out["start_date"], errors="coerce")
	# ตัดแถวที่ key ไม่ครบ/วันที่แปลงไม่ได้
	out = out.dropna(subset=["start_date", "product_no", "bom_no"]) 
	return out


def _load_all_pnp_latest(pnp_dir: str) -> pd.DataFrame:
	"""อ่านทุกไฟล์ใน data_PNP_TYPE แล้วหาแถวล่าสุดต่อคู่ (product_no, bom_no)"""
	rows = []
	if not os.path.isdir(pnp_dir):
		return pd.DataFrame(columns=["start_date", "product_no", "bom_no", "assy_pack_type"]) 

	for fname in os.listdir(pnp_dir):
		if not fname.lower().endswith((".xls", ".xlsx", ".csv")):
			continue
		fpath = os.path.join(pnp_dir, fname)
		df = _read_any(fpath)
		core = _extract_core_columns(df)
		if not core.empty:
			rows.append(core)

	if not rows:
		return pd.DataFrame(columns=["start_date", "product_no", "bom_no", "assy_pack_type"]) 

	all_df = pd.concat(rows, ignore_index=True)

	# เรียงเพื่อใช้ idxmax ได้แน่นอน
	all_df.sort_values(["product_no", "bom_no", "start_date"], inplace=True)
	# หา index ของวันที่มากที่สุดในแต่ละคู่
	latest_idx = all_df.groupby(["product_no", "bom_no"])['start_date'].idxmax()
	latest_df = all_df.loc[latest_idx].copy()
	latest_df = latest_df.sort_values(["product_no", "bom_no"]).reset_index(drop=True)
	return latest_df


def _load_all_pnp_all(pnp_dir: str) -> pd.DataFrame:
	"""อ่านทุกไฟล์ใน data_PNP_TYPE แล้วรวมทุกแถว (เฉพาะ 4 คอลัมน์หลัก)"""
	rows = []
	if not os.path.isdir(pnp_dir):
		return pd.DataFrame(columns=["start_date", "product_no", "bom_no", "assy_pack_type"]) 

	for fname in os.listdir(pnp_dir):
		if not fname.lower().endswith((".xls", ".xlsx", ".csv")):
			continue
		fpath = os.path.join(pnp_dir, fname)
		df = _read_any(fpath)
		core = _extract_core_columns(df)
		if not core.empty:
			rows.append(core)

	if not rows:
		return pd.DataFrame(columns=["start_date", "product_no", "bom_no", "assy_pack_type"]) 

	all_df = pd.concat(rows, ignore_index=True)
	# ให้แน่ใจว่าการเรียงเวลาทำงานได้
	all_df.sort_values(["product_no", "bom_no", "start_date"], inplace=True)
	return all_df


def _load_pairs(input_file: str) -> pd.DataFrame:
	"""อ่านไฟล์คู่ product_no, bom_no (รองรับ xlsx/xls/csv)"""
	df = _read_any(input_file)
	if df is None or df.empty:
		return pd.DataFrame(columns=["product_no", "bom_no"]) 

	df = _normalize_columns(df)
	prod_col = _resolve_column(df, ["product_no", "product", "product_number"])
	bom_col = _resolve_column(df, ["bom_no", "bom"])
	if not prod_col or not bom_col:
		return pd.DataFrame(columns=["product_no", "bom_no"]) 

	# เก็บลำดับแถวเดิมไว้เพื่อคงจำนวนและลำดับตาม input
	out = df[[prod_col, bom_col]].copy()
	out["___row_order"] = range(len(out))
	out = out.rename(columns={prod_col: "product_no", bom_col: "bom_no"})
	out["product_no"] = out["product_no"].astype(str).str.strip()
	out["bom_no"] = out["bom_no"].astype(str).str.strip().str.upper()
	# ไม่ dropna และไม่ drop_duplicates เพื่อรักษาจำนวนแถวให้เท่ากับ input
	return out


def PNP_PACK_TYPE(input_pairs_file, output_dir):
	"""
	รวมข้อมูล data_PNP_TYPE เพื่อหา assy_pack_type ล่าสุด ต่อ (product_no, bom_no)
	แล้ว merge กับไฟล์ input (ที่มี product_no, bom_no) เพื่อเติมค่า assy_pack_type ล่าสุด

	Parameters
	- input_pairs_file: str | list[str]
	- output_dir: str (ไม่ได้ใช้โดยตรง เพราะคืนค่าเป็น DataFrame ให้ระบบ export เอง)

	Returns
	- pandas.DataFrame คอลัมน์: product_no, bom_no, assy_pack_type, start_date
	"""
	# ถ้าอัปโหลดมาหลายไฟล์ ให้ใช้ไฟล์แรก
	if isinstance(input_pairs_file, list):
		if not input_pairs_file:
			return pd.DataFrame()
		input_pairs_file = input_pairs_file[0]

	# หา path โฟลเดอร์ data_PNP_TYPE (จากตำแหน่งไฟล์นี้)
	current_dir = os.path.dirname(os.path.abspath(__file__))  # .../src/functions
	src_dir = os.path.dirname(current_dir)                    # .../src
	pnp_dir = os.path.join(src_dir, "data_PNP_TYPE")
	# หากไม่มีโฟลเดอร์ใหม่ ให้ fallback ไปที่เดิมเพื่อความเข้ากันได้ย้อนหลัง
	if not os.path.isdir(pnp_dir):
		pnp_dir = os.path.join(src_dir, "data_PNP")

	# โหลดข้อมูลทั้งหมด
	all_rows = _load_all_pnp_all(pnp_dir)
	if all_rows.empty:
		return pd.DataFrame()

	# เตรียม lookup ล่าสุดแบบสองระดับ
	# 1) ล่าสุดต่อ product_no
	latest_by_prod = (
		all_rows.sort_values(["product_no", "start_date"]).groupby(["product_no"]).tail(1)
		[["product_no", "assy_pack_type", "start_date"]]
		.rename(columns={
			"assy_pack_type": "assy_prod",
			"start_date": "start_prod",
		})
	)
	# 2) ล่าสุดต่อ bom_no
	latest_by_bom = (
		all_rows.sort_values(["bom_no", "start_date"]).groupby(["bom_no"]).tail(1)
		[["bom_no", "assy_pack_type", "start_date"]]
		.rename(columns={
			"assy_pack_type": "assy_bom",
			"start_date": "start_bom",
		})
	)

	# โหลดคู่ product_no, bom_no จากไฟล์ input (ถ้าไม่ได้ส่งมาก็ถือว่าไม่มี)
	if not input_pairs_file or (isinstance(input_pairs_file, str) and not os.path.exists(input_pairs_file)):
		pairs = pd.DataFrame(columns=["product_no", "bom_no"])  # ไม่มี input
	else:
		pairs = _load_pairs(input_pairs_file)

	if pairs.empty:
		# ถ้า input ว่าง ให้ใช้ key ทั้งหมดที่พบในข้อมูล
		pairs = all_rows[["product_no", "bom_no"]].drop_duplicates().copy()

	# เติมค่าล่าสุดโดยให้ความสำคัญ product_no ก่อน ถ้าไม่ได้ค่อย fallback ด้วย bom_no
	merged = pairs.merge(latest_by_prod, on=["product_no"], how="left")
	merged = merged.merge(latest_by_bom, on=["bom_no"], how="left")
	# สร้างคอลัมน์ผลลัพธ์จากแหล่งที่ใช้จริง: product ก่อน ถ้าไม่มีหรือว่าง ค่อยใช้ bom
	merged["assy_pack_type"] = merged["assy_prod"].combine_first(merged["assy_bom"])  # ค่าที่เลือกใช้จริง
	merged["start_date"] = merged["start_prod"]
	mask_no_prod = merged["assy_prod"].isna()
	merged.loc[mask_no_prod, "start_date"] = merged.loc[mask_no_prod, "start_bom"]

	# คำนวณการเปลี่ยนแปลง assy_pack_type ตามเวลา
	def _first_last_change(g: pd.DataFrame):
		g = g.sort_values("start_date")
		# ใช้ค่าแบบ string dtype เพื่อรักษา NA/ค่าว่าง แล้ว normalize อีกครั้งเพื่อกัน noise
		s = g["assy_pack_type"].astype("string").str.strip().str.upper()
		s = s.replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "NULL": pd.NA})
		s_no_na = s.dropna()
		if s_no_na.empty:
			return pd.Series({"Change pack": "No", "Detail": ""})
		# มีการเปลี่ยนหรือไม่: ดูจำนวนค่าที่แตกต่างแบบ non-null หลัง normalize
		changed = "Yes" if s_no_na.nunique() > 1 else "No"
		# สร้าง Detail จาก "การเปลี่ยนล่าสุด" ไม่ใช่ first-to-last เพื่อเลี่ยง 'TRAY to TRAY'
		if changed == "Yes":
			last_val = s_no_na.iloc[-1]
			# หา previous ที่มีค่าต่างจาก last
			prev_diff = None
			for val in reversed(s_no_na.iloc[:-1].tolist()):
				if val != last_val:
					prev_diff = val
					break
			detail = f"{prev_diff} to {last_val}" if prev_diff is not None else ""
		else:
			detail = ""
		return pd.Series({"Change pack": changed, "Detail": detail})

	# คำนวณการเปลี่ยนแปลงแบบสองระดับ แล้วเลือกตามวิธี lookup ที่ใช้จริง
	change_by_prod = (
		all_rows.groupby(["product_no"], as_index=False)
		.apply(_first_last_change)
		.rename(columns={"Change pack": "Change pack by prod", "Detail": "Detail by prod"})
	)
	change_by_bom = (
		all_rows.groupby(["bom_no"], as_index=False)
		.apply(_first_last_change)
		.rename(columns={"Change pack": "Change pack by bom", "Detail": "Detail by bom"})
	)
	merged = merged.merge(change_by_prod, on=["product_no"], how="left")
	merged = merged.merge(change_by_bom, on=["bom_no"], how="left")
	# เลือก Change/Detail จากแหล่งเดียวกับที่ใช้หา assy_pack_type
	use_prod = merged["assy_prod"].notna()
	merged["Change pack"] = merged["Change pack by bom"]
	merged.loc[use_prod, "Change pack"] = merged.loc[use_prod, "Change pack by prod"]
	merged["Detail"] = merged["Detail by bom"]
	merged.loc[use_prod, "Detail"] = merged.loc[use_prod, "Detail by prod"]

	# เติมค่า default กรณีหาไม่เจอ
	merged["Change pack"] = merged["Change pack"].fillna("No")
	merged["Detail"] = merged["Detail"].fillna("")

	# ระบุวิธี mapping และเตรียมคอลัมน์ Last change date จาก start_date ที่เลือกใช้จริง
	merged["Mapping by"] = ""
	use_bom = (~use_prod) & merged["assy_bom"].notna()
	merged.loc[use_prod, "Mapping by"] = "product_no"
	merged.loc[use_bom, "Mapping by"] = "Bom"
	merged["Last change date"] = merged["start_date"]

	# ลบคอลัมน์ช่วยภายในหลังจากใช้งานครบแล้ว
	merged.drop(columns=[
		"assy_prod", "start_prod", "assy_bom", "start_bom",
		"Change pack by prod", "Detail by prod", "Change pack by bom", "Detail by bom"
	], inplace=True)

	# ปรับชื่อ/ค่า assy_pack_type เฉพาะเคส FILM-FRAME -> FILM FRAME
	merged["assy_pack_type"] = merged["assy_pack_type"].replace({
		"FILM-FRAME": "FILM FRAME"
	})

	# จัดคอลัมน์ตามที่ต้องการในไฟล์ผลลัพธ์ (ตามที่ผู้ใช้ระบุ)
	out_cols = [
		"product_no",
		"bom_no",
		"assy_pack_type",
		"Mapping by",
		"Change pack",
		"Detail",
		"Last change date",
	]
	# แปลงวันที่เป็นข้อความเพื่ออ้างอิง (ถ้าอยากเก็บไว้แสดงสามารถเพิ่มคอลัมน์ได้)
	# คงลำดับตาม input หากมี ___row_order มิฉะนั้นค่อยเรียงตาม key
	if "___row_order" in merged.columns:
		merged_sorted = merged.sort_values(["___row_order"]).drop(columns=["___row_order"]).reset_index(drop=True)
	else:
		merged_sorted = merged.sort_values(["product_no", "bom_no"]).reset_index(drop=True)

	# บันทึกไฟล์ผลลัพธ์เป็น Excel
	try:
		os.makedirs(output_dir, exist_ok=True)
		ts = datetime.now().strftime("%Y%m%d_%H%M%S")
		output_path = os.path.join(output_dir, f"PNP_PACK_TYPE_{ts}.xlsx")
		merged_sorted[out_cols].to_excel(output_path, index=False)
		return output_path
	except Exception:
		# ถ้าบันทึกไฟล์ไม่สำเร็จ ให้ส่งคืน DataFrame แทน
		return merged_sorted[out_cols]


# สำหรับทดสอบแบบรันไฟล์เดี่ยว (ไม่บังคับใช้งานในเว็บ)
if __name__ == "__main__":
	# ตัวอย่างการใช้งาน (แก้ path ให้ถูกกับเครื่อง)
	src_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
	sample_pairs = os.path.join(src_root, "data_PNP_TYPE", "sample_pairs.csv")  # ไฟล์ตัวอย่างที่มี product_no,bom_no
	if not os.path.exists(sample_pairs):
		sample_pairs = os.path.join(src_root, "data_PNP", "sample_pairs.csv")
	if os.path.exists(sample_pairs):
		df_result = PNP_PACK_TYPE(sample_pairs, os.path.join(src_root, "temp"))
		print(df_result.head())
	else:
		print("(optional) วางไฟล์คู่ product_no,bom_no ที่:", sample_pairs)
