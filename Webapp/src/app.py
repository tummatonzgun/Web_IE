from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_file
import os
import tempfile
import shutil
import socket
import json     
import pandas as pd
import requests
import sys

# เพิ่ม path สำหรับ src และ src/functions เพื่อให้ importlib หา module เจอ
SRC_PATH = os.path.join(os.getcwd(), "src")
FUNCTIONS_PATH = os.path.join(SRC_PATH, "functions")
if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)
if FUNCTIONS_PATH not in sys.path:
    sys.path.append(FUNCTIONS_PATH)

app = Flask(__name__)
app.secret_key = "your_secret_key"
app.api_base_url = "http://th3sroeeeng4/RTMSAPI/ApiAutoUph/api"

# Mapping operation -> function list
OPERATION_FUNCTIONS = {
    "Singulation": ["LOGVIEW"],
    "Pick & Place": ["PNP_CHANGE_TYPE","PNP_BOM_TYPE"],
    "Die Attach": ["DA_AUTO_UPH"],
    "Wire Bond": ["WB_AUTO_UPH"],
}

@app.route("/", methods=["GET"])
def operation():
    return render_template("operation.html")

# --- เพิ่มฟีเจอร์เลือกไฟล์ในโฟลเดอร์และรองรับหลายไฟล์ ---
@app.route("/method", methods=["GET", "POST"])
def method():
    if request.method == "POST":
        operation = request.form.get("operation") or request.args.get("operation")
        session["operation"] = operation
        input_method = request.form.get("inputMethod")
        session["input_method"] = input_method

        # เพิ่ม mapping ตรงนี้
        operation_folder_map = {
            "Die Attach": "data_Da",
            "Pick & Place": "data_PNP",
            "Wire Bond": "data_WB",
            "Singulation": "data_logview",
        }
        folder_name = operation_folder_map.get(operation, "")

        temp_root = os.path.join(os.getcwd(), "temp")
        os.makedirs(temp_root, exist_ok=True)

        # รับไฟล์ (อัปโหลดหลายไฟล์)
        if input_method == "upload":
            files = request.files.getlist("file")
            uploaded_files = []
            if files and any(f.filename for f in files):
                for file in files:
                    if file and file.filename:
                        file_path = os.path.join(temp_root, file.filename)
                        file.save(file_path)
                        uploaded_files.append(file_path)
                session["uploaded_file_path"] = uploaded_files  # เก็บเป็น list
            else:
                flash("กรุณาเลือกไฟล์ก่อน", "error")
                return redirect(url_for("method", operation=operation))

        # รับโฟลเดอร์ (เลือกไฟล์ในโฟลเดอร์ได้)
        elif input_method == "folder":
            selected_files = request.form.getlist("selected_folder[]") or request.form.getlist("selected_folder")
            if not selected_files:
                flash("กรุณาเลือกไฟล์จากโฟลเดอร์ก่อน", "error")
                return redirect(url_for("method", operation=operation))
            src_folder = os.path.join(os.getcwd(), "Webapp", "src", folder_name)
            file_paths = [os.path.join(src_folder, f) for f in selected_files]
            session["selected_folder"] = file_paths  # เก็บเป็น list

        # รับ API params (เหมือนเดิม)
        elif input_method == "api":
            endpoint = request.form.get("endpoint")
            plant = request.form.get("plant")
            year_quarter = request.form.get("year_quarter")  # เช่น "2024Q1,2024Q2"
            api_operation = request.form.get("api_operation")
            bom_no = request.form.get("bom_no")
            session["endpoint"] = endpoint
            session["plant"] = plant
            session["year_quarter"] = year_quarter
            session["api_operation"] = api_operation
            session["bom_no"] = bom_no

            api_url = f"{app.api_base_url}/{endpoint}"
            all_data = []
            error_msgs = []
            # แยก year_quarter เป็น list
            yq_list = [y.strip() for y in year_quarter.split(",") if y.strip()]
            for yq in yq_list:
                params = {}
                if plant: params["plant"] = plant
                params["year_quarter"] = yq
                if api_operation: params["operation"] = api_operation
                if bom_no: params["bom_no"] = bom_no
                try:
                    response = requests.get(api_url, params=params)
                    if response.status_code == 200:
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' in content_type and response.text.strip():
                            try:
                                json_data = response.json()
                                all_data.extend(json_data if isinstance(json_data, list) else [json_data])
                            except Exception as e:
                                error_msgs.append(f"API {yq} ได้รับข้อมูลที่ไม่ใช่ JSON: {e}")
                        else:
                            error_msgs.append(f"API {yq} ไม่ได้ส่งข้อมูล JSON หรือข้อมูลว่างเปล่า")
                    else:
                        error_msgs.append(f"API {yq} ดึงข้อมูลไม่สำเร็จ: {response.status_code}")
                except Exception as e:
                    error_msgs.append(f"API {yq} error: {e}")

            if all_data:
                json_filename = f"api_{plant}_{year_quarter}_{api_operation}_{bom_no or 'none'}.json"
                json_path = os.path.join(temp_root, json_filename)
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(all_data, f, ensure_ascii=False)
                session["api_json_path"] = json_path
            if error_msgs:
                flash(" | ".join(error_msgs), "error")
                return redirect(url_for("method", operation=operation))

        return redirect(url_for("function"))
    # GET: render หน้าเลือก method
    operation = request.args.get("operation", "")
    # --- เพิ่มสำหรับเลือกไฟล์ในโฟลเดอร์ ---
    operation_folder_map = {
        "Die Attach": "data_Da",
        "Pick & Place": "data_PNP",
        "Wire Bond": "data_WB",
        "Singulation": "data_logview",  # แก้ไขชื่อโฟลเดอร์ให้ตรงกับที่มีอยู่
    }
    folder_name = operation_folder_map.get(operation, "")
    folder_list = []
    show_process_all_in_folder = operation in ["Singulation", "Pick & Place"]  # แสดงปุ่มประมวลผลทั้งโฟลเดอร์เฉพาะบาง operation
    show_api = operation in ["Die Attach", "Wire Bond"]  # แสดง API เฉพาะบาง operation
    if folder_name:
        # เดิม: folder_root = os.path.join(os.getcwd(), folder_name)
        # แก้เป็น:
        folder_root = os.path.join(os.getcwd(), "Webapp", "src", folder_name)
        if os.path.exists(folder_root):
            folder_list = [f for f in os.listdir(folder_root) if os.path.isfile(os.path.join(folder_root, f))]
    print("DEBUG folder_root:", folder_root)
    print("DEBUG folder_list:", folder_list)
    return render_template("method.html", operation=operation, folder_root=folder_name, folder_list=folder_list, show_api=show_api, show_process_all_in_folder=show_process_all_in_folder)

@app.route("/function", methods=["GET", "POST"])
def function():
    if request.method == "POST":
        func_name = request.form.get("func_name")
        # รองรับทั้งแบบเดิมและ date_range picker
        start_date = request.form.get("start_date")
        end_date = request.form.get("end_date")
        date_range = request.form.get("date_range")
        if date_range and (not start_date or not end_date):
            # แยกวันที่จาก date_range (รองรับหลายรูปแบบ)
            for sep in [' to ', ',', ' ']:
                if sep in date_range:
                    parts = [d.strip() for d in date_range.split(sep)]
                    if len(parts) == 2:
                        start_date, end_date = parts
                        break
        if not start_date or not end_date:
            start_date = None
            end_date = None
        input_method = session.get("input_method")
        operation = session.get("operation")
        result = None
        current_file = None

        # เตรียม path ไฟล์ที่ต้องประมวลผล
        file_path = None
        if input_method == "upload":
            file_path = session.get("uploaded_file_path")
        elif input_method == "folder":
            file_path = session.get("selected_folder")
        elif input_method == "api":
            file_path = session.get("api_json_path")
        else:
            file_path = None

        # ประมวลผลฟังก์ชัน
        try:
            import importlib
            import pandas as pd
            func_module = importlib.import_module(f"functions.{func_name.lower()}")
            func = getattr(func_module, func_name)
            temp_root = os.path.join(os.getcwd(), "temp")
            if func_name in ["DA_AUTO_UPH", "PNP_AUTO_UPH", "WB_AUTO_UPH"]:
                result = func(file_path, temp_root, start_date, end_date)
            else:
                result = func(file_path, temp_root)
        except Exception as e:
            result = f"เกิดข้อผิดพลาดในการเรียกใช้ฟังก์ชัน {func_name}: {e}"

        # --- สร้างไฟล์ผลลัพธ์สำหรับดาวน์โหลด ---
        export_file_path = None
        temp_root = os.path.join(os.getcwd(), "temp")
        print("DEBUG result:", result)
        if isinstance(result, pd.DataFrame):
            export_file_path = os.path.join(temp_root, f"result_{operation}_{func_name}.xlsx")
            result.to_excel(export_file_path, index=False)
        elif isinstance(result, list):
            # ถ้าเป็น list ของ path ให้ใช้ตัวแรกที่เป็นไฟล์จริง
            for r in result:
                if isinstance(r, str) and os.path.exists(r):
                    export_file_path = r
                    break
        elif isinstance(result, str) and os.path.exists(result):
            export_file_path = result
        session["export_file_path"] = export_file_path

        # ไม่ต้องเก็บ result_data ใน session อีกต่อไป
        session["current_file"] = file_path
        session["operation"] = operation
        session["func_name"] = func_name
        session["start_date"] = start_date      
        session["end_date"] = end_date          
        return redirect(url_for("result"))

    # GET: render หน้าเลือกฟังก์ชัน (เพิ่ม preview date range)
    input_method = session.get("input_method")
    current_file = None
    file_path = None
    if input_method == "upload":
        file_path = session.get("uploaded_file_path")
        if file_path:
            if isinstance(file_path, list):
                current_file = [os.path.basename(f) for f in file_path]
                file_path_preview = file_path[0]
            else:
                current_file = os.path.basename(file_path)
                file_path_preview = file_path
    elif input_method == "folder":
        folder = session.get("selected_folder")
        if folder:
            if isinstance(folder, list):
                current_file = [os.path.basename(f) for f in folder]
                file_path_preview = folder[0]
            else:
                current_file = folder
                file_path_preview = folder
    elif input_method == "api":
        json_path = session.get("api_json_path")
        if json_path:
            current_file = os.path.basename(json_path)
            file_path_preview = json_path
    else:
        file_path_preview = None

    # Preview date range
    date_info = None
    if file_path_preview and os.path.exists(file_path_preview):
        try:
            from functions.da_auto_uph import preview_date_range
            date_info = preview_date_range(file_path_preview)
        except Exception as e:
            date_info = None

    operation = session.get("operation")
    functions = OPERATION_FUNCTIONS.get(operation, [])
    return render_template("function.html", functions=functions, current_file=current_file, operation=operation, date_info=date_info)

@app.route("/result", methods=["GET"])
def result():
    import pandas as pd
    export_file_path = session.get("export_file_path")
    current_file = session.get("current_file")
    operation = session.get("operation")
    func_name = session.get("func_name")
    table_html = None
    result_data = None
    error_message = None
    if not export_file_path:
        error_message = "export_file_path ไม่ถูกสร้าง กรุณาตรวจสอบการประมวลผลหรือฟังก์ชันที่เลือก"
    elif not os.path.exists(export_file_path):
        error_message = f"ไม่พบไฟล์ผลลัพธ์: {export_file_path} กรุณาตรวจสอบว่าไฟล์ถูกสร้างจริงหลังประมวลผล"
    if error_message:
        table_html = f"<pre>{error_message}</pre>"
    else:
        try:
            if export_file_path.endswith(".xlsx"):
                df = pd.read_excel(export_file_path)
            elif export_file_path.endswith(".csv"):
                df = pd.read_csv(export_file_path)
            else:
                df = None
            if df is not None:
                table_html = df.to_html(classes="table", border=0, index=False)
                result_data = df.to_dict(orient="records")
        except Exception as e:
            table_html = f"<pre>เกิดข้อผิดพลาดในการอ่านไฟล์ผลลัพธ์: {e}</pre>"
    return render_template("result.html", result=result_data, current_file=current_file, operation=operation, func_name=func_name, table_html=table_html, start_date=session.get("start_date"), end_date=session.get("end_date"))

@app.route("/api/", methods=["GET"])
def get_api_data():
    endpoint = request.args.get("endpoint")
    plant = request.args.get("plant")
    year_quarter = request.args.get("year_quarter")
    operation = request.args.get("operation")
    bom_no = request.args.get("bom_no")

    if not endpoint:
        return jsonify({"error": "No endpoint selected"}), 400

    url = f"{app.api_base_url}/{endpoint}"
    params = {}
    if plant: params["plant"] = plant
    if year_quarter: params["year_quarter"] = year_quarter
    if operation: params["operation"] = operation
    if bom_no: params["bom_no"] = bom_no

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type and response.text.strip():
            try:
                data = response.json()
            except Exception as e:
                return jsonify({
                    "error": f"API ได้รับข้อมูลที่ไม่ใช่ JSON: {e}",
                    "example": response.text[:300]
                }), 500
            return jsonify({
                "request_url": response.url,
                "data": data
            })
        else:
            if 'text/html' in content_type:
                return jsonify({
                    "error": "API ไม่ได้ส่งข้อมูล JSON แต่ส่ง HTML (Content-Type: text/html). กรุณาตรวจสอบ URL endpoint ว่าเป็น API จริง ไม่ใช่ Swagger UI หรือหน้าเว็บ และตรวจสอบสิทธิ์การเข้าถึง API ปลายทาง",
                    "example": response.text[:300]
                }), 500
            else:
                return jsonify({
                    "error": f"API ไม่ได้ส่งข้อมูล JSON หรือข้อมูลว่างเปล่า | Content-Type: {content_type}",
                    "example": response.text[:300]
                }), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/download_result")
def download_result():
    # สมมติไฟล์ผลลัพธ์ถูกสร้างไว้ใน session["export_file_path"]
    export_file_path = session.get("export_file_path")
    if not export_file_path or not os.path.exists(export_file_path):
        flash("ไม่พบไฟล์สำหรับดาวน์โหลด", "error")
        return redirect(url_for("result"))
    return send_file(export_file_path, as_attachment=True)

if __name__ == "__main__":
    ip = socket.gethostbyname(socket.gethostname())
    print(f"\n✅ Flask app is running on: http://{ip}:80\n(เปิดจากเครื่องอื่นในเครือข่ายได้ด้วย IP นี้)\n")
    app.run(debug=True, host='0.0.0.0', port=80)

# ===== Version Information =====
# version 3.0 - Fully Refactored with Service Classes and Modern Architecture

