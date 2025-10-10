import os
import sys
import importlib.util


def _load_pnp_pack_type_module():
    """Load the real implementation from 'PNP_Pack type.py' and return the module."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(base_dir, "PNP_Pack type.py")
    if not os.path.exists(target_path):
        raise FileNotFoundError(f"ไม่พบไฟล์: {target_path}")

    spec = importlib.util.spec_from_file_location("functions.PNP_Pack type", target_path)
    if spec is None or spec.loader is None:
        raise ImportError("ไม่สามารถโหลดสเปคของโมดูล 'PNP_Pack type.py'")
    module = importlib.util.module_from_spec(spec)
    # ใส่ลง sys.modules เพื่อให้ import ภายในไฟล์ (ถ้ามี) ทำงานถูก
    sys.modules["functions.PNP_Pack type"] = module
    spec.loader.exec_module(module)
    return module


# Expose the function symbol expected by app.py
_mod = _load_pnp_pack_type_module()
PNP_PACK_TYPE = getattr(_mod, "PNP_PACK_TYPE")