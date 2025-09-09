import io
import re
import ipaddress
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import xml.etree.ElementTree as ET

# ===================== Configuración =====================
SHEET_NAME = "Interface"
IPROUTING_SHEET = "IpRouting"
IPRT_INDEX_DEFAULT = 2

RAML_NS = "raml21.xsd"
ET.register_namespace('', RAML_NS)
NS = {"r": RAML_NS}

TEMPLATE_PATH = Path(__file__).parent / "doc" / "Configuration_scf_template.xml"

DEFAULT_LOCATIONS = [
    Path(__file__).parent / "doc" / "data.xlsx",
    Path(__file__).parent / "data.xlsx",
]

LAST_EXCEL_PATH: Optional[Path] = None
IPRT_RAW_DF: Optional[pd.DataFrame] = None
IPRT_HEADER_ROW: Optional[int] = None

COLUMN_ALIASES = {
    "macro enb id": "lnBtsId", "lnbtsid": "lnBtsId", "ln bts id": "lnBtsId",
    "enbname": "eNBName", "enb name": "eNBName", "enb": "eNBName",
    "cellname": "cellName",
    "ip address of the network interface": "localIpAddr",
    "ieif or ivif/localipaddr": "localIpAddr",
    "network mask of the ip address": "netmask",
    "ieif or ivif/netmask": "netmask",
    "vlan identifier": "vlanId", "ivif/vlanid": "vlanId",
    "ntp server ip address primary": "ntpPrimary",
    "ntp server ip address secondary": "ntpSecondary",
    "ip address of the top master": "topMasterIp",
    "timing over packet message rate": "topRate",
    "modulelocation": "moduleLocation", "module location": "moduleLocation", "location": "moduleLocation",
}

COLUMN_ALIASES_IPRT = {
    "destination ip address of static route": "iprtDest",
    "destination ip address of static route (rc)": "dest_rc",
    "destination ip address of static route (trafica)": "dest_trafica",
    "destination ip address of static route (arieso)": "dest_arieso",
    "destination ip address": "iprtDest", "dest ip": "iprtDest", "destip": "iprtDest",
    "gateway": "iprtGateway", "gw": "iprtGateway",
    "macro enb id": "lnBtsId", "lnbtsid": "lnBtsId", "ln bts id": "lnBtsId",
    "enbname": "eNBName", "enb name": "eNBName", "enb": "eNBName",
}

REQUIRED_COLUMNS = ["lnBtsId", "eNBName"]

# ===================== Utils Excel =====================
def _canon_base(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

def canonize_with_aliases(col: str, aliases: Dict[str, str]) -> str:
    base = _canon_base(col)
    return aliases.get(base, None) or str(col).strip()

def _dedupe_columns(cols: List[str]) -> List[str]:
    seen, out = {}, []
    for c in cols:
        seen[c] = seen.get(c, 0) + 1
        out.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return out

def _row_has_keys(cells: List[str], keys: List[str], min_hits: int = 2) -> bool:
    norm_cells = [_canon_base(x) for x in cells if isinstance(x, str)]
    hits = sum(1 for k in keys if _canon_base(k) in norm_cells)
    return hits >= min_hits

def _autodetect_header_row_generic(df_raw: pd.DataFrame, candidate_keys: List[str]) -> int:
    for i in range(min(30, len(df_raw))):
        row_vals = df_raw.iloc[i].tolist()
        cells = [str(x) for x in row_vals if (isinstance(x, str) or pd.notna(x))]
        if _row_has_keys(cells, candidate_keys, min_hits=2):
            return i
    return 0

def _finalize_after_header_generic(df_raw: pd.DataFrame, header_row: int, aliases: Dict[str, str]) -> pd.DataFrame:
    header_vals = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = header_vals
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    df.columns = _dedupe_columns([canonize_with_aliases(c, aliases) for c in df.columns])
    return df

def read_interface_sheet(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=SHEET_NAME, header=None, dtype=object)
    cand = ["macro eNB id", "lnBtsId", "eNBName", "enbName", "IP address of the network interface",
            "Network mask of the IP address", "VLAN identifier"]
    hrow = _autodetect_header_row_generic(raw, cand)
    return _finalize_after_header_generic(raw, hrow, COLUMN_ALIASES)

def read_iprouting_both(path: Path) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[int]]:
    try:
        raw = pd.read_excel(path, sheet_name=IPROUTING_SHEET, header=None, dtype=object)
    except Exception:
        return (None, None, None)
    cand = list(COLUMN_ALIASES_IPRT.keys()) + ["iprtDest", "iprtGateway", "lnBtsId", "eNBName"]
    hrow = _autodetect_header_row_generic(raw, cand)
    df = _finalize_after_header_generic(raw, hrow, COLUMN_ALIASES_IPRT)
    return (df, raw, hrow)

def load_dataframe(initial_path: Optional[Path] = None) -> pd.DataFrame:
    global LAST_EXCEL_PATH
    if initial_path:
        p = Path(initial_path).expanduser()
        if p.exists():
            LAST_EXCEL_PATH = p
            return read_interface_sheet(p)
    for p in DEFAULT_LOCATIONS:
        if p.exists():
            LAST_EXCEL_PATH = p
            return read_interface_sheet(p)
    sel = filedialog.askopenfilename(
        title=f"Selecciona el Excel (hoja '{SHEET_NAME}')",
        filetypes=[("Excel", "*.xlsx *.xlsm *.xls"), ("Todos", "*.*")]
    )
    if not sel:
        raise FileNotFoundError("No se seleccionó archivo de Excel.")
    LAST_EXCEL_PATH = Path(sel)
    return read_interface_sheet(LAST_EXCEL_PATH)

def load_iprouting_from_last() -> Optional[pd.DataFrame]:
    global IPRT_RAW_DF, IPRT_HEADER_ROW
    if LAST_EXCEL_PATH and LAST_EXCEL_PATH.exists():
        df, raw, hrow = read_iprouting_both(LAST_EXCEL_PATH)
        IPRT_RAW_DF, IPRT_HEADER_ROW = raw, hrow
        return df
    return None

def validate_required(row: pd.Series) -> List[str]:
    missing: List[str] = []
    for col in REQUIRED_COLUMNS:
        val = row.get(col, None)
        if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
            missing.append(col)
    return missing

def sval(row: pd.Series, col: str, default: str = "") -> str:
    v = row.get(col, default)
    if pd.isna(v):
        return ""
    return str(v).strip()

# ===================== IP Utils =====================
def to_prefix_len(mask_or_prefix: str) -> int:
    s = str(mask_or_prefix).strip()
    if not s:
        return 0
    if re.fullmatch(r"\d{1,2}", s):
        return int(s)
    try:
        net = ipaddress.IPv4Network(f"0.0.0.0/{s}", strict=False)
        return net.prefixlen
    except Exception:
        return 0

def pick_host_ip(ip_str: str, prefix_len: int) -> str:
    try:
        ip = ipaddress.IPv4Address(ip_str)
        net = ipaddress.IPv4Network(f"{ip_str}/{prefix_len}", strict=False)
        if ip == net.network_address:
            return str(next(net.hosts()))
        return str(ip)
    except Exception:
        return ip_str

def normalize_ip(val: str) -> str:
    s = str(val or "").strip().replace(",", ".")
    m = re.search(r"\b\d{1,3}(?:\.\d{1,3}){3}\b", s)
    return m.group(0) if m else s

def get_block(df_row: pd.Series, idx: int) -> Dict[str, str]:
    suf = "" if idx == 1 else f"_{idx}"
    ip = sval(df_row, f"localIpAddr{suf}")
    nm = sval(df_row, f"netmask{suf}") or sval(df_row, f"netmask_{idx}")
    vlan = sval(df_row, f"vlanId{suf}")
    plen = to_prefix_len(nm) if nm else 0
    host_ip = pick_host_ip(ip, plen) if (ip and plen) else ip
    return {"ip": host_ip, "prefix": str(plen) if plen else "", "vlan": vlan}

# ===================== XML helpers =====================
def xp(elem, query):
    return elem.findall(query, NS)

def first(elem, query):
    lst = xp(elem, query)
    return lst[0] if lst else None

def replace_all_mrbts_ids_anywhere(cmData, new_id: str):
    if not new_id or not re.fullmatch(r"\d+", str(new_id).strip()):
        return
    pat = re.compile(r"MRBTS-\d+")
    repl = f"MRBTS-{str(new_id).strip()}"
    for elem in cmData.iter():
        for k, v in list(elem.attrib.items()):
            if isinstance(v, str) and pat.search(v):
                elem.set(k, pat.sub(repl, v))
        if isinstance(elem.text, str) and pat.search(elem.text):
            elem.text = pat.sub(repl, elem.text)

def set_bts_name(cmData, new_name: str):
    mr = first(cmData, ".//r:managedObject[@class='com.nokia.srbts:MRBTS']")
    if mr is None:
        return
    p = first(mr, "./r:p[@name='btsName']")
    if p is None:
        p = ET.SubElement(mr, f"{{{NS['r']}}}p", {"name":"btsName"})
    p.text = new_name

def _iter_managed_objects(cmData):
    return xp(cmData, ".//r:managedObject")

def _find_mo_by_class_and_dist_contains(cmData, class_name: str, contains_str: str):
    for mo in _iter_managed_objects(cmData):
        if mo.get("class") == class_name and contains_str in (mo.get("distName") or ""):
            return mo
    return None

def set_vlan(cmData, idx: int, vlan_id: str):
    mo = _find_mo_by_class_and_dist_contains(cmData, "com.nokia.srbts.tnl:VLANIF", f"/VLANIF-{idx}")
    if mo is None:
        return
    p = first(mo, "./r:p[@name='vlanId']")
    if p is None:
        p = ET.SubElement(mo, f"{{{NS['r']}}}p", {"name":"vlanId"})
    if vlan_id:
        try:
            p.text = str(int(float(vlan_id)))
        except Exception:
            p.text = vlan_id

def set_ip_block(cmData, idx: int, ip: str, prefix: str):
    ipmo = _find_mo_by_class_and_dist_contains(cmData, "com.nokia.srbts.tnl:IPADDRESSV4", f"/IPIF-{idx}/IPADDRESSV4-1")
    if ipmo is None:
        return
    if first(ipmo, "./r:p[@name='ipAddressAllocationMethod']") is None:
        ET.SubElement(ipmo, f"{{{NS['r']}}}p", {"name":"ipAddressAllocationMethod"}).text = "MANUAL"
    lp = first(ipmo, "./r:p[@name='localIpAddr']") or ET.SubElement(ipmo, f"{{{NS['r']}}}p", {"name":"localIpAddr"})
    if ip:
        lp.text = ip
    lpl = first(ipmo, "./r:p[@name='localIpPrefixLength']") or ET.SubElement(ipmo, f"{{{NS['r']}}}p", {"name":"localIpPrefixLength"})
    if prefix:
        lpl.text = prefix

def set_ntp_servers(cmData, primary: str, secondary: str):
    ntp = first(cmData, ".//r:managedObject[@class='com.nokia.srbts.mnl:NTP']")
    if ntp is None:
        return
    lst = first(ntp, "./r:list[@name='ntpServerIpAddrOrFqdnList']") or ET.SubElement(ntp, f"{{{NS['r']}}}list", {"name":"ntpServerIpAddrOrFqdnList"})
    for child in list(lst): lst.remove(child)
    if primary:  ET.SubElement(lst, f"{{{NS['r']}}}p").text = primary
    if secondary: ET.SubElement(lst, f"{{{NS['r']}}}p").text = secondary

def set_top_master_and_rate(cmData, master_ip: str, rate_val: str):
    topf = first(cmData, ".//r:managedObject[@class='com.nokia.srbts.mnl:TOPF']")
    if topf is None: return
    lst = first(topf, "./r:list[@name='topMasterList']")
    if lst is not None:
        it = first(lst, "./r:item") or ET.SubElement(lst, f"{{{NS['r']}}}item")
        mp = first(it, "./r:p[@name='masterIpAddr']") or ET.SubElement(it, f"{{{NS['r']}}}p", {"name":"masterIpAddr"})
        if master_ip: mp.text = master_ip
    rp = first(topf, "./r:p[@name='syncMessageRate']") or ET.SubElement(topf, f"{{{NS['r']}}}p", {"name":"syncMessageRate"})
    try:
        rx = int(str(rate_val).strip()); rp.text = f"RATE_{rx}"
    except Exception:
        rp.text = str(rate_val or "RATE_32")

def ensure_top_splane_points_to_ipif3(cmData):
    top = first(cmData, ".//r:managedObject[@class='com.nokia.srbts.mnl:TOP']")
    if top is None: return
    p = first(top, "./r:p[@name='sPlaneIpAddressDN']")
    if p is None: return
    p.text = re.sub(r"/IPIF-\d+/IPADDRESSV4-1$", "/IPIF-3/IPADDRESSV4-1", p.text or "", flags=re.I)

def set_param_global(cmData, p_name: str, value: str, create_if_missing: bool = True):
    if value is None or value == "": return
    changed = False
    for mo in xp(cmData, ".//r:managedObject"):
        p = first(mo, f"./r:p[@name='{p_name}']")
        if p is not None:
            p.text = value; changed = True
    if changed or not create_if_missing: return
    target_mo = first(cmData, ".//r:managedObject[@class='com.nokia.srbts:MRBTS']") or first(cmData, ".//r:managedObject")
    if target_mo is not None:
        ET.SubElement(target_mo, f"{{{NS['r']}}}p", {"name": p_name}).text = value

# ===================== IpRouting helpers =====================
def excel_col_to_idx(col_letter: str) -> int:
    col_letter = str(col_letter).strip().upper()
    if not re.fullmatch(r"[A-Z]+", col_letter):
        raise ValueError(f"Letra de columna inválida: {col_letter}")
    idx = 0
    for ch in col_letter:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1

def iprt_get_by_letter(raw_df: pd.DataFrame, abs_row_idx: int, col_letter: str) -> Optional[str]:
    try:
        j = excel_col_to_idx(col_letter)
        v = raw_df.iat[abs_row_idx, j]
        if pd.isna(v): return None
        return str(v).strip()
    except Exception:
        return None

def iprt_match_row(df_iprt: Optional[pd.DataFrame], lnBtsId: str, eNBName: str) -> Tuple[Optional[pd.Series], Optional[int]]:
    if df_iprt is None or df_iprt.empty or IPRT_RAW_DF is None or IPRT_HEADER_ROW is None:
        return (None, None)
    row_rel = None
    if "lnBtsId" in df_iprt.columns and lnBtsId:
        m = df_iprt["lnBtsId"].astype(str).str.strip() == str(lnBtsId).strip()
        if m.any(): row_rel = df_iprt[m].iloc[0]
    if row_rel is None and "eNBName" in df_iprt.columns and eNBName:
        m = df_iprt["eNBName"].astype(str).str.strip() == str(eNBName).strip()
        if m.any(): row_rel = df_iprt[m].iloc[0]
    if row_rel is None: row_rel = df_iprt.iloc[0]
    rel_pos = row_rel.name
    rel_pos_int = int(rel_pos) if isinstance(rel_pos, (int, float)) else df_iprt.index.get_loc(rel_pos)
    abs_idx = IPRT_HEADER_ROW + 1 + rel_pos_int
    return (row_rel, abs_idx)

def get_col_flexible_from_row(row: Optional[pd.Series], keywords: List[str]) -> Optional[str]:
    if row is None: return None
    for c in row.index:
        c_norm = _canon_base(str(c))
        if any(kw in c_norm for kw in keywords):
            v = row.get(c)
            if v is not None and not pd.isna(v):
                return str(v).strip()
    return None

def build_static_items_from_sheets(df_iprt: Optional[pd.DataFrame], iprt_abs_row_idx: Optional[int], interface_row: pd.Series) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    gw_DA = gw_DM = gw_DQ = gw_H = None
    if IPRT_RAW_DF is not None and iprt_abs_row_idx is not None:
        gw_DA = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "DA")
        gw_DM = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "DM")
        gw_DQ = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "DQ")
        gw_H  = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "H")
    iprt_row_rel, _ = iprt_match_row(df_iprt, sval(interface_row, "lnBtsId"), sval(interface_row, "eNBName"))
    dest_rc      = get_col_flexible_from_row(iprt_row_rel, ["destination ip address of static route (rc)", "(rc)"])
    dest_trafica = get_col_flexible_from_row(iprt_row_rel, ["destination ip address of static route (trafica)", "trafica"])
    dest_arieso  = get_col_flexible_from_row(iprt_row_rel, ["destination ip address of static route (arieso)", "arieso"])
    top_master_dest = sval(interface_row, "topMasterIp")
    def push(prefix, dest, gw):
        dest = normalize_ip(dest or "0.0.0.0"); gw = normalize_ip(gw or "0.0.0.0")
        items.append({"prefix": str(prefix), "dest": dest, "gw": gw, "pref": "1", "preSrc": "0.0.0.0"})
    push(13, dest_rc,        gw_DA)
    push(32, dest_trafica,   gw_DM)
    push(28, dest_arieso,    gw_DQ)
    push(32, top_master_dest, gw_H)
    push(0, "0.0.0.0",       gw_H)
    return items

def write_static_routes_to_mo(mo_iprt: ET.Element, items: List[Dict[str, str]]):
    lst = first(mo_iprt, "./r:list[@name='staticRoutes']") or ET.SubElement(mo_iprt, f"{{{NS['r']}}}list", {"name":"staticRoutes"})
    for child in list(lst): lst.remove(child)
    for it in items:
        item = ET.SubElement(lst, f"{{{NS['r']}}}item")
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "destinationIpPrefixLength"}).text = it["prefix"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "destIpAddr"}).text = it["dest"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "gateway"}).text = it["gw"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "preference"}).text = it["pref"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "preSrcIpv4Addr"}).text = it["preSrc"]

def rebuild_static_routes_from_sheets_for_all_iprt(cmData: ET.Element, df_iprt: Optional[pd.DataFrame], iprt_abs_row_idx: Optional[int], interface_row: pd.Series):
    items = build_static_items_from_sheets(df_iprt, iprt_abs_row_idx, interface_row)
    for mo in [m for m in _iter_managed_objects(cmData) if m.get("class") == "com.nokia.srbts.tnl:IPRT"]:
        write_static_routes_to_mo(mo, items)

def find_iprouting_values(df_iprt: Optional[pd.DataFrame], lnBtsId: str, eNBName: str) -> Tuple[Optional[str], Optional[str]]:
    if df_iprt is None or df_iprt.empty: return (None, None)
    row = None
    if "lnBtsId" in df_iprt.columns and lnBtsId:
        m = df_iprt["lnBtsId"].astype(str).str.strip() == str(lnBtsId).strip()
        if m.any(): row = df_iprt[m].iloc[0]
    if row is None and "eNBName" in df_iprt.columns and eNBName:
        m = df_iprt["eNBName"].astype(str).str.strip() == str(eNBName).strip()
        if m.any(): row = df_iprt[m].iloc[0]
    if row is None: row = df_iprt.iloc[0]
    return (normalize_ip(sval(row, "iprtDest")) or None, normalize_ip(sval(row, "iprtGateway")) or None)

# ===================== LNCEL helpers =====================
SECTOR_TO_LNCEL_INDEX = {"L1":1,"L2":2,"L3":3,"T1":1,"T2":2,"T3":3}

def _ensure_namespace_recursive(elem: ET.Element):
    if not elem.tag.startswith("{"):
        elem.tag = f"{{{NS['r']}}}{elem.tag}"
    for child in list(elem):
        _ensure_namespace_recursive(child)

def _set_or_create_p(elem: ET.Element, name: str, value: str):
    p = first(elem, f"./r:p[@name='{name}']")
    if p is None:
        p = ET.SubElement(elem, f"{{{NS['r']}}}p", {"name": name})
    p.text = value

def load_sector_fragment(sector: str) -> ET.Element:
    """
    Lee doc/<sector>.txt y extrae el PRIMER bloque <managedObject ...>...</managedObject>,
    ignorando BOM, encabezados XML y texto extra.
    """
    path = Path(__file__).parent / "doc" / f"{sector}.txt"
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo del sector: {path}")
    text = path.read_text(encoding="utf-8", errors="ignore")
    text = text.lstrip("\ufeff").strip()
    text = re.sub(r"^\s*<\?xml[^>]*\?>", "", text, flags=re.I)
    matches = list(re.finditer(r"<managedObject\b.*?</managedObject>", text, flags=re.S | re.I))
    if not matches:
        pos = text.lower().find("<managedobject")
        if pos >= 0:
            text = text[pos:]
            matches = list(re.finditer(r"<managedObject\b.*?</managedObject>", text, flags=re.S | re.I))
    if not matches:
        raise RuntimeError(f"El archivo {path.name} no contiene un bloque <managedObject> válido.")
    frag = matches[0].group(0).strip()
    mo = ET.fromstring(frag)
    _ensure_namespace_recursive(mo)
    return mo

def _sector_from_cellname(cellname: str) -> Optional[str]:
    m = re.search(r"_(L[123]|T[123])$", cellname.strip(), flags=re.I)
    return m.group(1).upper() if m else None

def _enb_from_cellname(cellname: str) -> str:
    sec = _sector_from_cellname(cellname) or ""
    return cellname[:-(len(sec)+1)].strip() if sec and cellname.endswith(f"_{sec}") else cellname.strip()

PARAM_COLS_OPT = ["phyCellId","tac","prachFreqOff","rootSeqIndex","prachCS","prachConfIndex","earfcnDL","pMax"]

def _norm_decimal(val: Optional[str]) -> Optional[str]:
    if val is None: return None
    s = str(val).strip().replace(",", ".")
    return s if s else None

def _set_if_has(elem: ET.Element, name: str, value: Optional[str]):
    if value is None: return
    _set_or_create_p(elem, name, value)

# ===================== Excel "solo cellName" =====================
def _read_cellname_excel(path: Path) -> List[Dict[str, str]]:
    df_raw = pd.read_excel(path, header=None, dtype=object)
    header_row = 0
    for i in range(min(10, len(df_raw))):
        row = [str(x).strip() for x in df_raw.iloc[i].tolist() if pd.notna(x)]
        if any(_canon_base(x) == "cellname" for x in row):
            header_row = i; break
    df = pd.read_excel(path, header=header_row, dtype=object)

    # Mapa canónico de columnas
    cols_map = {c: _canon_base(str(c)) for c in df.columns}

    def get_val(row, wanted):
        canon = _canon_base(wanted)
        for c, cc in cols_map.items():
            if cc == canon:
                v = row.get(c); 
                if pd.isna(v): return None
                return str(v).strip()
        return None

    rows = []
    for _, r in df.iterrows():
        cn = get_val(r, "cellName")
        if not cn: continue
        item = {"cellName": cn}
        for k in PARAM_COLS_OPT:
            item[k] = get_val(r, k)
        rows.append(item)
    if not rows:
        raise RuntimeError("No hay valores en 'cellName'.")
    return rows

def insert_lncels_from_cellnames(cmData: ET.Element, lnBtsId: str, rows: List[Dict[str, str]]):
    for r in rows:
        cn = r["cellName"]
        sec = _sector_from_cellname(cn)
        if not sec: continue
        idx = SECTOR_TO_LNCEL_INDEX.get(sec)
        if not idx: continue
        mo = load_sector_fragment(sec)
        if mo.tag != f"{{{NS['r']}}}managedObject":
            raise RuntimeError(f"El archivo de {sec} no contiene un <managedObject> válido.")
        mo.set("distName", f"MRBTS-{lnBtsId}/LNBTS-{lnBtsId}/LNCEL-{idx}")
        _set_or_create_p(mo, "cellName", cn)
        _set_if_has(mo, "phyCellId",      r.get("phyCellId"))
        _set_if_has(mo, "tac",            r.get("tac"))
        _set_if_has(mo, "prachFreqOff",   r.get("prachFreqOff"))
        _set_if_has(mo, "rootSeqIndex",   r.get("rootSeqIndex"))
        _set_if_has(mo, "prachCS",        r.get("prachCS"))
        _set_if_has(mo, "prachConfIndex", r.get("prachConfIndex"))
        _set_if_has(mo, "earfcnDL",       r.get("earfcnDL"))
        _set_if_has(mo, "pMax",           _norm_decimal(r.get("pMax")))
        cmData.append(mo)

def build_xml_from_cellnames_using_template(rows: List[Dict[str, str]], lnBtsId: str) -> bytes:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"No encuentro la plantilla: {TEMPLATE_PATH}")
    if not rows:
        raise RuntimeError("No se recibieron filas 'cellName'.")
    enb = _enb_from_cellname(rows[0]["cellName"])

    tree = ET.parse(str(TEMPLATE_PATH))
    root = tree.getroot()
    cmData = first(root, "./r:cmData")
    if cmData is None:
        raise RuntimeError("No se encontró <cmData> en la plantilla.")

    replace_all_mrbts_ids_anywhere(cmData, lnBtsId)
    set_bts_name(cmData, enb)
    set_param_global(cmData, "enbName", enb, True)
    set_param_global(cmData, "moduleLocation", enb, True)
    ensure_top_splane_points_to_ipif3(cmData)

    insert_lncels_from_cellnames(cmData, lnBtsId=lnBtsId, rows=rows)

    header_log = first(cmData, "./r:header/r:log")
    if header_log is not None:
        header_log.set("dateTime", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    bio = io.BytesIO()
    try: ET.indent(tree, space="  ", level=0)
    except Exception: pass
    tree.write(bio, encoding="utf-8", xml_declaration=True)
    bio.seek(0)
    return bio.read()

# ===================== Flujo original (Interface + IpRouting) =====================
def build_xml_from_row_using_template(
    row: pd.Series,
    df_iprt: Optional[pd.DataFrame] = None,
    iprt_dest: Optional[str] = None,
    iprt_gateway: Optional[str] = None,
    top_master_override: Optional[str] = None,
    iprt_index: int = IPRT_INDEX_DEFAULT,
    cell_suffix: Optional[str] = None,
    lncels_from_txt: Optional[List[str]] = None,
) -> bytes:
    missing = validate_required(row)
    if missing:
        raise RuntimeError("Faltan campos obligatorios: " + ", ".join(missing))
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"No encuentro la plantilla: {TEMPLATE_PATH}")

    lnBtsId = sval(row, "lnBtsId")
    enbName = sval(row, "eNBName")
    cellName_excel = sval(row, "cellName")

    b1 = get_block(row, 1); b2 = get_block(row, 2); b3 = get_block(row, 3); b4 = get_block(row, 4)

    ntp_primary   = sval(row, "ntpPrimary")
    ntp_secondary = sval(row, "ntpSecondary")
    top_master    = sval(row, "topMasterIp")
    rate_raw      = sval(row, "topRate") or "32"

    tree = ET.parse(str(TEMPLATE_PATH))
    root = tree.getroot()
    cmData = first(root, "./r:cmData")
    if cmData is None:
        raise RuntimeError("No se encontró <cmData> en la plantilla.")

    replace_all_mrbts_ids_anywhere(cmData, lnBtsId)

    set_bts_name(cmData, enbName)
    set_param_global(cmData, "enbName", enbName, True)

    suffix = (cell_suffix or "T1").strip() if enbName else ""
    cellName_final = cellName_excel or (f"{enbName}_{suffix}" if enbName and suffix else "")
    if cellName_final: set_param_global(cmData, "cellName", cellName_final, True)

    module_loc = sval(row, "moduleLocation")
    set_param_global(cmData, "moduleLocation", module_loc or enbName, create_if_missing=True)

    set_vlan(cmData, 1, b1["vlan"]); set_vlan(cmData, 2, b2["vlan"])
    set_vlan(cmData, 3, b3["vlan"]); set_vlan(cmData, 4, b4["vlan"])

    set_ip_block(cmData, 1, b1["ip"], b1["prefix"])
    set_ip_block(cmData, 2, b2["ip"], b2["prefix"])
    set_ip_block(cmData, 3, b3["ip"], b3["prefix"])
    set_ip_block(cmData, 4, b4["ip"], b4["prefix"])

    ensure_top_splane_points_to_ipif3(cmData)

    effective_master = normalize_ip(top_master_override) if top_master_override else top_master
    if ntp_primary or ntp_secondary:
        set_ntp_servers(cmData, ntp_primary or "", ntp_secondary or "")
    if effective_master or rate_raw:
        set_top_master_and_rate(cmData, normalize_ip(effective_master) if effective_master else "", rate_raw)

    _, iprt_abs_idx = iprt_match_row(df_iprt=df_iprt, lnBtsId=lnBtsId, eNBName=enbName)
    rebuild_static_routes_from_sheets_for_all_iprt(cmData=cmData, df_iprt=df_iprt, iprt_abs_row_idx=iprt_abs_idx, interface_row=row)

    if iprt_dest or iprt_gateway:
        mo = _find_mo_by_class_and_dist_contains(cmData, "com.nokia.srbts.tnl:IPRT", f"/IPRT-{iprt_index}")
        if mo is not None:
            lst = first(mo, "./r:list[@name='staticRoutes']") or ET.SubElement(mo, f"{{{NS['r']}}}list", {"name":"staticRoutes"})
            it = first(lst, "./r:item") or ET.SubElement(lst, f"{{{NS['r']}}}item")
            if iprt_dest:
                (first(it, "./r:p[@name='destIpAddr']") or ET.SubElement(it, f"{{{NS['r']}}}p", {"name":"destIpAddr"})).text = normalize_ip(iprt_dest)
            if iprt_gateway:
                (first(it, "./r:p[@name='gateway']") or ET.SubElement(it, f"{{{NS['r']}}}p", {"name":"gateway"})).text = normalize_ip(iprt_gateway)

    if lncels_from_txt:
        for sec in lncels_from_txt:
            idx = SECTOR_TO_LNCEL_INDEX.get(sec.strip().upper())
            if not idx: continue
            mo = load_sector_fragment(sec)
            mo.set("distName", f"MRBTS-{lnBtsId}/LNBTS-{lnBtsId}/LNCEL-{idx}")
            _set_or_create_p(mo, "cellName", f"{enbName}_{sec}")
            cmData.append(mo)

    header_log = first(cmData, "./r:header/r:log")
    if header_log is not None:
        header_log.set("dateTime", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    bio = io.BytesIO()
    try: ET.indent(tree, space="  ", level=0)
    except Exception: pass
    tree.write(bio, encoding="utf-8", xml_declaration=True)
    bio.seek(0)
    return bio.read()

# ===================== UI =====================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Clonador XML (Plantilla fija en ./doc)")
        self.geometry("1120x780")

        self.df: Optional[pd.DataFrame] = None
        self.df_iprt: Optional[pd.DataFrame] = None
        self.filtered_names: List[str] = []
        self.selected_name: Optional[str] = None

        self.sector_var_default = tk.StringVar(value="T1")
        self.sector_vars_multi: Dict[str, tk.BooleanVar] = {k: tk.BooleanVar(value=False) for k in ["L1","L2","L3","T1","T2","T3"]}

        self._build_widgets()

        try:
            self.df = load_dataframe()
            self.df_iprt = load_iprouting_from_last()
            self._refresh_hint()
            self._suggest_initial()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _build_widgets(self):
        top = ttk.Frame(self, padding=10); top.pack(fill=tk.X)
        ttk.Label(top, text="Buscar eNBName:").pack(side=tk.LEFT)
        self.entry = ttk.Entry(top); self.entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)
        self.entry.bind("<KeyRelease>", self.on_search_change)

        self.btn_reload = ttk.Button(top, text="Cargar Excel...", command=self.on_reload_excel)
        self.btn_reload.pack(side=tk.LEFT, padx=6)

        self.btn_generate_from_cells = ttk.Button(top, text="Generar XML (solo cellName)...", command=self.on_generate_from_cellname_excel)
        self.btn_generate_from_cells.pack(side=tk.LEFT, padx=6)

        sector_frame = ttk.LabelFrame(self, text="Sufijo cellName GLOBAL (si Excel no trae 'cellName')", padding=(10, 6))
        sector_frame.pack(fill=tk.X, padx=10)
        for val in ("L1", "L2", "L3", "T1", "T2", "T3"):
            ttk.Radiobutton(sector_frame, text=val, value=val, variable=self.sector_var_default).pack(side=tk.LEFT, padx=6)

        multi_frame = ttk.LabelFrame(self, text="Sectores a CREAR desde TXT (se pueden varios)", padding=(10, 6))
        multi_frame.pack(fill=tk.X, padx=10, pady=(4, 0))
        for val in ("L1", "L2", "L3", "T1", "T2", "T3"):
            ttk.Checkbutton(multi_frame, text=val, variable=self.sector_vars_multi[val]).pack(side=tk.LEFT, padx=8)

        middle = ttk.Panedwindow(self, orient=tk.HORIZONTAL); middle.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        left = ttk.Frame(middle); middle.add(left, weight=1)
        ttk.Label(left, text="Resultados (eNBName)").pack(anchor="w")
        self.listbox = tk.Listbox(left, height=18); self.listbox.pack(fill=tk.BOTH, expand=True)
        self.listbox.bind("<<ListboxSelect>>", self.on_select_name)

        right = ttk.Frame(middle); middle.add(right, weight=2)
        ttk.Label(right, text="Detalle de la fila seleccionada").pack(anchor="w")
        self.tree = ttk.Treeview(right, columns=("col", "val"), show="headings", height=18)
        self.tree.heading("col", text="Columna"); self.tree.heading("val", text="Valor")
        self.tree.column("col", width=360, anchor="w"); self.tree.column("val", width=620, anchor="w")
        self.tree.pack(fill=tk.BOTH, expand=True)
        yscroll = ttk.Scrollbar(right, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set); yscroll.place(in_=self.tree, relx=1.0, rely=0, relheight=1.0, x=-1)

        bottom = ttk.Frame(self, padding=10); bottom.pack(fill=tk.X)
        self.hint_lbl = ttk.Label(bottom, text="Listo."); self.hint_lbl.pack(side=tk.LEFT)
        self.btn_generate = ttk.Button(bottom, text="Generar XML", command=self.on_generate_xml)
        self.btn_generate.pack(side=tk.RIGHT)

    def _refresh_hint(self):
        tpl_ok = "OK" if TEMPLATE_PATH.exists() else "NO ENCONTRADA"
        if self.df is None:
            self.hint_lbl.config(text=f"Sin Excel cargado | Plantilla: {TEMPLATE_PATH.name} [{tpl_ok}]"); return
        cols = ", ".join(self.df.columns.tolist()[:8])
        iprt_info = "IpRouting: OK" if (self.df_iprt is not None and not self.df_iprt.empty) else "IpRouting: N/D"
        self.hint_lbl.config(text=f"Filas: {len(self.df)} | Columnas: {len(self.df.columns)} (ej: {cols}...) | {iprt_info} | Plantilla: {TEMPLATE_PATH.name} [{tpl_ok}]")

    def _suggest_initial(self):
        if self.df is None or "eNBName" not in self.df.columns: return
        names = self.df["eNBName"].dropna().astype(str).drop_duplicates().sort_values().head(50).tolist()
        self._load_listbox(names)

    def _load_listbox(self, items: List[str]):
        self.listbox.delete(0, tk.END)
        for it in items: self.listbox.insert(tk.END, it)
        self.filtered_names = items

    def on_reload_excel(self):
        try:
            self.df = load_dataframe()
            self.df_iprt = load_iprouting_from_last()
            self._refresh_hint(); self.entry.delete(0, tk.END)
            self._suggest_initial(); self.tree.delete(*self.tree.get_children())
            self.selected_name = None
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def on_search_change(self, event=None):
        if self.df is None or "eNBName" not in self.df.columns: return
        q = self.entry.get().strip()
        if not q: self._suggest_initial(); return
        mask = self.df["eNBName"].astype(str).str.contains(q, case=False, na=False)
        names = self.df.loc[mask, "eNBName"].dropna().astype(str).drop_duplicates().sort_values().head(200).tolist()
        self._load_listbox(names)

    def on_select_name(self, event=None):
        sel = self.listbox.curselection()
        if not sel: return
        name = self.listbox.get(sel[0]); self.selected_name = name
        self._show_row_details(name)

    def _show_row_details(self, name: str):
        self.tree.delete(*self.tree.get_children())
        if self.df is None: return
        rows = self.df[self.df["eNBName"].astype(str) == name]
        if rows.empty: return
        row = rows.iloc[0]
        for col, val in row.items():
            disp = "" if pd.isna(val) else str(val)
            self.tree.insert("", tk.END, values=(col, disp))
        miss = validate_required(row)
        if miss:
            messagebox.showwarning("Validación", "Faltan campos obligatorios en la fila: " + ", ".join(miss))

    def _get_selected_sectors_multi(self) -> List[str]:
        return [k for k, var in self.sector_vars_multi.items() if var.get()]

    def on_generate_xml(self):
        if self.df is None or not self.selected_name:
            messagebox.showinfo("Info", "Selecciona primero un eNBName en la lista."); return
        if not TEMPLATE_PATH.exists():
            messagebox.showerror("Plantilla faltante", f"No encuentro la plantilla:\n{TEMPLATE_PATH}\nVerifica la ruta/nombre."); return

        rows = self.df[self.df["eNBName"].astype(str) == self.selected_name]
        if rows.empty:
            messagebox.showerror("Error", "No se encontró la fila seleccionada."); return
        row = rows.iloc[0]

        lnBtsId = sval(row, "lnBtsId")
        eNBName = sval(row, "eNBName")
        dest_ip, gw_ip = find_iprouting_values(self.df_iprt, lnBtsId, eNBName)
        top_master_override = dest_ip

        sectors_multi = self._get_selected_sectors_multi()
        try:
            xml_bytes = build_xml_from_row_using_template(
                row,
                df_iprt=self.df_iprt,
                iprt_dest=dest_ip,
                iprt_gateway=gw_ip,
                top_master_override=top_master_override,
                iprt_index=IPRT_INDEX_DEFAULT,
                cell_suffix=self.sector_var_default.get(),
                lncels_from_txt=sectors_multi,
            )
        except FileNotFoundError as e:
            messagebox.showerror("Archivo faltante", str(e)); return
        except Exception as e:
            messagebox.showerror("Error al generar XML", str(e)); return

        default_name = f"{self.selected_name}.xml".replace("/", "_").replace("\\", "_")
        out_path = filedialog.asksaveasfilename(
            title="Guardar XML",
            defaultextension=".xml",
            initialfile=default_name,
            filetypes=[("XML", "*.xml"), ("Todos", "*.*")]
        )
        if not out_path: return
        with open(out_path, "wb") as f: f.write(xml_bytes)
        messagebox.showinfo("Listo", f"XML generado:\n{out_path}")

    def on_generate_from_cellname_excel(self):
        if not TEMPLATE_PATH.exists():
            messagebox.showerror("Plantilla faltante", f"No encuentro la plantilla:\n{TEMPLATE_PATH}")
            return
        sel = filedialog.askopenfilename(
            title="Selecciona el Excel con columna 'cellName'",
            filetypes=[("Excel", "*.xlsx *.xlsm *.xls"), ("Todos", "*.*")]
        )
        if not sel: return
        try:
            rows = _read_cellname_excel(Path(sel))
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo leer el Excel: {e}")
            return

        lnBtsId = simpledialog.askstring("MRBTS", "Ingresa lnBtsId (solo dígitos):", parent=self)
        if not lnBtsId or not re.fullmatch(r"\d+", lnBtsId.strip()):
            messagebox.showerror("Dato inválido", "lnBtsId es obligatorio y debe ser numérico.")
            return

        try:
            xml_bytes = build_xml_from_cellnames_using_template(rows=rows, lnBtsId=lnBtsId.strip())
        except FileNotFoundError as e:
            messagebox.showerror("Archivo faltante", str(e)); return
        except Exception as e:
            messagebox.showerror("Error al generar XML", str(e)); return

        enb = _enb_from_cellname(rows[0]["cellName"])
        default_name = f"{enb}.xml".replace("/", "_").replace("\\", "_")
        out_path = filedialog.asksaveasfilename(
            title="Guardar XML",
            defaultextension=".xml",
            initialfile=default_name,
            filetypes=[("XML", "*.xml"), ("Todos", "*.*")]
        )
        if not out_path: return
        with open(out_path, "wb") as f: f.write(xml_bytes)
        messagebox.showinfo("Listo", f"XML generado (solo cellName):\n{out_path}")

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
