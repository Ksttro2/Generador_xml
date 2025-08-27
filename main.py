import io
import re
import ipaddress
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict, Tuple

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import xml.etree.ElementTree as ET

# ===================== Configuración =====================
SHEET_NAME = "Interface"
IPROUTING_SHEET = "IpRouting"
IPRT_INDEX_DEFAULT = 2  # Cambia aquí si tu ruta está en otro IPRT-<n>

# --- NAMESPACE RAML ---
RAML_NS = "raml21.xsd"
ET.register_namespace('', RAML_NS)   # fuerza xmlns="raml21.xsd" (sin prefijo)
NS = {"r": RAML_NS}                  # para XPath: .//r:managedObject, etc.

# Ruta FIJA de la plantilla (carpeta 'doc')
TEMPLATE_PATH = Path(__file__).parent / "doc" / "Configuration_scf_template.xml"

# Excel por defecto (carpeta 'doc' y raíz)
DEFAULT_LOCATIONS = [
    Path(__file__).parent / "doc" / "data.xlsx",
    Path(__file__).parent / "data.xlsx",
]

# Último Excel cargado (para leer IpRouting del mismo archivo)
LAST_EXCEL_PATH: Optional[Path] = None

# Buffers crudos para IpRouting (para leer por letras de columna)
IPRT_RAW_DF: Optional[pd.DataFrame] = None
IPRT_HEADER_ROW: Optional[int] = None

# Alias de columnas -> nombre canónico (Interface)
COLUMN_ALIASES = {
    # Ids y nombre
    "macro enb id": "lnBtsId",
    "lnbtsid": "lnBtsId",
    "ln bts id": "lnBtsId",
    "enbname": "eNBName",
    "enb name": "eNBName",
    "enb": "eNBName",
    "cellname": "cellName",

    # IP/VLAN
    "ip address of the network interface": "localIpAddr",
    "ieif or ivif/localipaddr": "localIpAddr",
    "network mask of the ip address": "netmask",
    "ieif or ivif/netmask": "netmask",
    "vlan identifier": "vlanId",
    "ivif/vlanid": "vlanId",

    # NTP / ToP Master / Rate
    "ntp server ip address primary": "ntpPrimary",
    "ntp server ip address secondary": "ntpSecondary",
    "ip address of the top master": "topMasterIp",
    "timing over packet message rate": "topRate",

    # Ubicación
    "modulelocation": "moduleLocation",
    "module location": "moduleLocation",
    "location": "moduleLocation",
}

# Alias (IpRouting) – genéricos para compatibilidad
COLUMN_ALIASES_IPRT = {
    "destination ip address of static route": "iprtDest",
    "destination ip address of static route (rc)": "dest_rc",
    "destination ip address of static route (trafica)": "dest_trafica",
    "destination ip address of static route (arieso)": "dest_arieso",
    "destination ip address": "iprtDest",
    "dest ip": "iprtDest",
    "destip": "iprtDest",
    "gateway": "iprtGateway",
    "gw": "iprtGateway",

    # Para cruzar por si viene
    "macro enb id": "lnBtsId",
    "lnbtsid": "lnBtsId",
    "ln bts id": "lnBtsId",
    "enbname": "eNBName",
    "enb name": "eNBName",
    "enb": "eNBName",
}

REQUIRED_COLUMNS = ["lnBtsId", "eNBName"]

# ===================== Utilidades Excel =====================
def _canon_base(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

def canonize_with_aliases(col: str, aliases: Dict[str, str]) -> str:
    base = _canon_base(col)
    return aliases.get(base, None) or str(col).strip()

def _dedupe_columns(cols: List[str]) -> List[str]:
    seen = {}
    out = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 1
            out.append(c)
    return out

def _row_has_keys(cells: List[str], keys: List[str], min_hits: int = 2) -> bool:
    norm_cells = [_canon_base(x) for x in cells if isinstance(x, str)]
    hits = sum(1 for k in keys if _canon_base(k) in norm_cells)
    return hits >= min_hits

def _autodetect_header_row_generic(df_raw: pd.DataFrame, candidate_keys: List[str]) -> int:
    max_scan = min(30, len(df_raw))
    for i in range(max_scan):
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
    mapped = [canonize_with_aliases(c, aliases) for c in df.columns]
    df.columns = _dedupe_columns(mapped)
    return df

def read_interface_sheet(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=SHEET_NAME, header=None, dtype=object)
    cand = [
        "macro eNB id", "lnBtsId", "eNBName", "enbName",
        "IP address of the network interface",
        "Network mask of the IP address", "VLAN identifier"
    ]
    hrow = _autodetect_header_row_generic(raw, cand)
    df = _finalize_after_header_generic(raw, hrow, COLUMN_ALIASES)
    return df

# Lee IpRouting crudo + procesado, y guarda header_row en globales
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
        IPRT_RAW_DF = raw
        IPRT_HEADER_ROW = hrow
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

# ===================== Utilidades IP =====================
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

# ===================== XML helpers (RAML Nokia) =====================
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
    mo = _find_mo_by_class_and_dist_contains(
        cmData, "com.nokia.srbts.tnl:VLANIF", f"/VLANIF-{idx}"
    )
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
    ipmo = _find_mo_by_class_and_dist_contains(
        cmData, "com.nokia.srbts.tnl:IPADDRESSV4", f"/IPIF-{idx}/IPADDRESSV4-1"
    )
    if ipmo is None:
        return
    palloc = first(ipmo, "./r:p[@name='ipAddressAllocationMethod']")
    if palloc is None:
        ET.SubElement(ipmo, f"{{{NS['r']}}}p", {"name":"ipAddressAllocationMethod"}).text = "MANUAL"
    lp = first(ipmo, "./r:p[@name='localIpAddr']")
    if lp is None:
        lp = ET.SubElement(ipmo, f"{{{NS['r']}}}p", {"name":"localIpAddr"})
    if ip:
        lp.text = ip
    lpl = first(ipmo, "./r:p[@name='localIpPrefixLength']")
    if lpl is None:
        lpl = ET.SubElement(ipmo, f"{{{NS['r']}}}p", {"name":"localIpPrefixLength"})
    if prefix:
        lpl.text = prefix

def set_ntp_servers(cmData, primary: str, secondary: str):
    ntp = first(cmData, ".//r:managedObject[@class='com.nokia.srbts.mnl:NTP']")
    if ntp is None:
        return
    lst = first(ntp, "./r:list[@name='ntpServerIpAddrOrFqdnList']")
    if lst is None:
        lst = ET.SubElement(ntp, f"{{{NS['r']}}}list", {"name":"ntpServerIpAddrOrFqdnList"})
    for child in list(lst):
        lst.remove(child)
    if primary:
        p1 = ET.SubElement(lst, f"{{{NS['r']}}}p"); p1.text = primary
    if secondary:
        p2 = ET.SubElement(lst, f"{{{NS['r']}}}p"); p2.text = secondary

def set_top_master_and_rate(cmData, master_ip: str, rate_val: str):
    topf = first(cmData, ".//r:managedObject[@class='com.nokia.srbts.mnl:TOPF']")
    if topf is None:
        return
    lst = first(topf, "./r:list[@name='topMasterList']")
    if lst is not None:
        it = first(lst, "./r:item")
        if it is None:
            it = ET.SubElement(lst, f"{{{NS['r']}}}item")
        mp = first(it, "./r:p[@name='masterIpAddr']")
        if mp is None:
            mp = ET.SubElement(it, f"{{{NS['r']}}}p", {"name":"masterIpAddr"})
        if master_ip:
            mp.text = master_ip
    rp = first(topf, "./r:p[@name='syncMessageRate']")
    if rp is None:
        rp = ET.SubElement(topf, f"{{{NS['r']}}}p", {"name":"syncMessageRate"})
    try:
        rx = int(str(rate_val).strip())
        rp.text = f"RATE_{rx}"
    except Exception:
        rp.text = str(rate_val or "RATE_32")

def ensure_top_splane_points_to_ipif3(cmData):
    top = first(cmData, ".//r:managedObject[@class='com.nokia.srbts.mnl:TOP']")
    if top is None:
        return
    p = first(top, "./r:p[@name='sPlaneIpAddressDN']")
    if p is None:
        return
    p.text = re.sub(r"/IPIF-\d+/IPADDRESSV4-1$", "/IPIF-3/IPADDRESSV4-1", p.text or "", flags=re.I)

def set_param_global(cmData, p_name: str, value: str, create_if_missing: bool = True):
    if value is None or value == "":
        return
    changed = False
    for mo in xp(cmData, ".//r:managedObject"):
        p = first(mo, f"./r:p[@name='{p_name}']")
        if p is not None:
            p.text = value
            changed = True
    if changed or not create_if_missing:
        return
    target_mo = first(cmData, ".//r:managedObject[@class='com.nokia.srbts:MRBTS']") or first(cmData, ".//r:managedObject")
    if target_mo is not None:
        ET.SubElement(target_mo, f"{{{NS['r']}}}p", {"name": p_name}).text = value

# ===================== IpRouting helpers (por letra de columna) =====================
def excel_col_to_idx(col_letter: str) -> int:
    col_letter = str(col_letter).strip().upper()
    if not re.fullmatch(r"[A-Z]+", col_letter):
        raise ValueError(f"Letra de columna inválida: {col_letter}")
    idx = 0
    for ch in col_letter:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1  # 0-based

def iprt_get_by_letter(raw_df: pd.DataFrame, abs_row_idx: int, col_letter: str) -> Optional[str]:
    try:
        j = excel_col_to_idx(col_letter)
        v = raw_df.iat[abs_row_idx, j]
        if pd.isna(v):
            return None
        return str(v).strip()
    except Exception:
        return None

def iprt_match_row(df_iprt: Optional[pd.DataFrame], lnBtsId: str, eNBName: str) -> Tuple[Optional[pd.Series], Optional[int]]:
    """Devuelve (fila_iprt_procesada, abs_row_idx_en_raw)"""
    if df_iprt is None or df_iprt.empty or IPRT_RAW_DF is None or IPRT_HEADER_ROW is None:
        return (None, None)
    row_rel = None
    if "lnBtsId" in df_iprt.columns and lnBtsId:
        m = df_iprt["lnBtsId"].astype(str).str.strip() == str(lnBtsId).strip()
        if m.any():
            row_rel = df_iprt[m].iloc[0]
    if row_rel is None and "eNBName" in df_iprt.columns and eNBName:
        m = df_iprt["eNBName"].astype(str).str.strip() == str(eNBName).strip()
        if m.any():
            row_rel = df_iprt[m].iloc[0]
    if row_rel is None:
        row_rel = df_iprt.iloc[0]

    rel_pos = row_rel.name
    if isinstance(rel_pos, (int, float)):
        rel_pos_int = int(rel_pos)
    else:
        rel_pos_int = df_iprt.index.get_loc(rel_pos)
    abs_idx = IPRT_HEADER_ROW + 1 + rel_pos_int
    return (row_rel, abs_idx)

# ===================== Static Routes: construir y escribir en TODOS los IPRT =====================
def get_col_flexible_from_row(row: Optional[pd.Series], keywords: List[str]) -> Optional[str]:
    """Busca en los encabezados de la fila una columna que contenga alguno de los keywords."""
    if row is None:
        return None
    for c in row.index:
        c_norm = _canon_base(str(c))
        if any(kw in c_norm for kw in keywords):
            v = row.get(c)
            if v is not None and not pd.isna(v):
                return str(v).strip()
    return None

def build_static_items_from_sheets(
    df_iprt: Optional[pd.DataFrame],
    iprt_abs_row_idx: Optional[int],
    interface_row: pd.Series
) -> List[Dict[str, str]]:
    """
    Devuelve SIEMPRE los 5 ítems en el orden correcto:
    RC (13), Tráfica (32), Arieso (28), ToP Master (32), Default (0).
    Si falta el dato en Excel, se crea con dest vacío.
    """
    items: List[Dict[str, str]] = []

    # Gateways por letra
    gw_DA = gw_DM = gw_DQ = gw_H = None
    if IPRT_RAW_DF is not None and iprt_abs_row_idx is not None:
        gw_DA = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "DA")
        gw_DM = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "DM")
        gw_DQ = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "DQ")
        gw_H  = iprt_get_by_letter(IPRT_RAW_DF, iprt_abs_row_idx, "H")

    # Fila procesada (por encabezados)
    iprt_row_rel, _ = iprt_match_row(df_iprt, sval(interface_row, "lnBtsId"), sval(interface_row, "eNBName"))

    # Destinos
    dest_rc      = get_col_flexible_from_row(iprt_row_rel, ["destination ip address of static route (rc)", "(rc)"])
    dest_trafica = get_col_flexible_from_row(iprt_row_rel, ["destination ip address of static route (trafica)", "trafica"])
    dest_arieso  = get_col_flexible_from_row(iprt_row_rel, ["destination ip address of static route (arieso)", "arieso"])
    top_master_dest = sval(interface_row, "topMasterIp")

    # Helper: agrega siempre el ítem (aunque no haya dest)
    def push(prefix, dest, gw):
        dest = normalize_ip(dest or "")
        gw   = normalize_ip(gw or "")
        if not dest:
            dest = "0.0.0.0"
        if not gw:
            gw = "0.0.0.0"
        items.append({"prefix": str(prefix), "dest": dest, "gw": gw, "pref": "1", "preSrc": "0.0.0.0"})

    # Orden fijo
    push(13, dest_rc,        gw_DA)  # RC
    push(32, dest_trafica,   gw_DM)  # Tráfica
    push(28, dest_arieso,    gw_DQ)  # Arieso
    push(32, top_master_dest, gw_H)  # ToP Master
    push(0, "0.0.0.0",       gw_H)   # Default

    # Debug
    print(f"[StaticRoutes] RC={dest_rc} gw_DA={gw_DA} | TRAF={dest_trafica} gw_DM={gw_DM} | ARI={dest_arieso} gw_DQ={gw_DQ} | TOP={top_master_dest} gw_H={gw_H}")

    return items


def write_static_routes_to_mo(mo_iprt: ET.Element, items: List[Dict[str, str]]):
    """
    Limpia y escribe <list name="staticRoutes"> en el MO IPRT dado.
    """
    lst = first(mo_iprt, "./r:list[@name='staticRoutes']")
    if lst is None:
        lst = ET.SubElement(mo_iprt, f"{{{NS['r']}}}list", {"name":"staticRoutes"})
    else:
        for child in list(lst):
            lst.remove(child)

    for it in items:
        item = ET.SubElement(lst, f"{{{NS['r']}}}item")
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "destinationIpPrefixLength"}).text = it["prefix"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "destIpAddr"}).text = it["dest"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "gateway"}).text = it["gw"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "preference"}).text = it["pref"]
        ET.SubElement(item, f"{{{NS['r']}}}p", {"name": "preSrcIpv4Addr"}).text = it["preSrc"]

def rebuild_static_routes_from_sheets_for_all_iprt(
    cmData: ET.Element,
    df_iprt: Optional[pd.DataFrame],
    iprt_abs_row_idx: Optional[int],
    interface_row: pd.Series
):
    """
    Genera los 5 ítems desde las hojas y los escribe en TODOS los managedObject IPRT del XML.
    Así te aseguras de que no queden valores de la plantilla en ningún IPRT.
    """
    items = build_static_items_from_sheets(df_iprt, iprt_abs_row_idx, interface_row)
    all_iprt = [m for m in _iter_managed_objects(cmData) if m.get("class") == "com.nokia.srbts.tnl:IPRT"]
    for mo in all_iprt:
        write_static_routes_to_mo(mo, items)

# ===================== Búsqueda en IpRouting (compat UI) =====================
def find_iprouting_values(df_iprt: Optional[pd.DataFrame], lnBtsId: str, eNBName: str) -> Tuple[Optional[str], Optional[str]]:
    if df_iprt is None or df_iprt.empty:
        return (None, None)
    row = None
    if "lnBtsId" in df_iprt.columns and lnBtsId:
        m = df_iprt["lnBtsId"].astype(str).str.strip() == str(lnBtsId).strip()
        if m.any():
            row = df_iprt[m].iloc[0]
    if row is None and "eNBName" in df_iprt.columns and eNBName:
        m = df_iprt["eNBName"].astype(str).str.strip() == str(eNBName).strip()
        if m.any():
            row = df_iprt[m].iloc[0]
    if row is None:
        row = df_iprt.iloc[0]
    dest = normalize_ip(sval(row, "iprtDest"))
    gw = normalize_ip(sval(row, "iprtGateway"))
    return (dest or None, gw or None)

# ===================== Construcción desde plantilla fija =====================
def build_xml_from_row_using_template(
    row: pd.Series,
    df_iprt: Optional[pd.DataFrame] = None,
    iprt_dest: Optional[str] = None,
    iprt_gateway: Optional[str] = None,
    top_master_override: Optional[str] = None,
    iprt_index: int = IPRT_INDEX_DEFAULT
) -> bytes:
    missing = validate_required(row)
    if missing:
        raise RuntimeError("Faltan campos obligatorios: " + ", ".join(missing))
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"No encuentro la plantilla: {TEMPLATE_PATH}")

    lnBtsId = sval(row, "lnBtsId")
    enbName = sval(row, "eNBName")
    cellName_excel = sval(row, "cellName")

    b1 = get_block(row, 1)
    b2 = get_block(row, 2)
    b3 = get_block(row, 3)
    b4 = get_block(row, 4)

    ntp_primary   = sval(row, "ntpPrimary")
    ntp_secondary = sval(row, "ntpSecondary")
    top_master    = sval(row, "topMasterIp")
    rate_raw      = sval(row, "topRate") or "32"

    tree = ET.parse(str(TEMPLATE_PATH))
    root = tree.getroot()
    cmData = first(root, "./r:cmData")
    if cmData is None:
        raise RuntimeError("No se encontró <cmData> en la plantilla.")

    # Reemplazo global de MRBTS-<n>
    replace_all_mrbts_ids_anywhere(cmData, lnBtsId)

    # Nombres y ubicación
    set_bts_name(cmData, enbName)
    set_param_global(cmData, "enbName", enbName, True)
    cellName_final = cellName_excel or (f"{enbName}_T1" if enbName else "")
    if cellName_final:
        set_param_global(cmData, "cellName", cellName_final, True)

    module_loc = sval(row, "moduleLocation")
    if module_loc:
        set_param_global(cmData, "moduleLocation", module_loc, create_if_missing=True)
    else:
        set_param_global(cmData, "moduleLocation", enbName, create_if_missing=True)

    # VLANs e IPs
    set_vlan(cmData, 1, b1["vlan"])
    set_vlan(cmData, 2, b2["vlan"])
    set_vlan(cmData, 3, b3["vlan"])
    set_vlan(cmData, 4, b4["vlan"])

    set_ip_block(cmData, 1, b1["ip"], b1["prefix"])
    set_ip_block(cmData, 2, b2["ip"], b2["prefix"])
    set_ip_block(cmData, 3, b3["ip"], b3["prefix"])
    set_ip_block(cmData, 4, b4["ip"], b4["prefix"])

    # TOP a IPIF-3
    ensure_top_splane_points_to_ipif3(cmData)

    # NTP y TOPF
    effective_master = normalize_ip(top_master_override) if top_master_override else top_master
    if ntp_primary or ntp_secondary:
        set_ntp_servers(cmData, ntp_primary or "", ntp_secondary or "")
    if effective_master or rate_raw:
        set_top_master_and_rate(cmData, normalize_ip(effective_master) if effective_master else "", rate_raw)

    # ===== Reconstrucción de staticRoutes usando hojas (TODOS los IPRT) =====
    _, iprt_abs_idx = iprt_match_row(
        df_iprt=df_iprt,
        lnBtsId=lnBtsId,
        eNBName=enbName
    )
    rebuild_static_routes_from_sheets_for_all_iprt(
        cmData=cmData,
        df_iprt=df_iprt,
        iprt_abs_row_idx=iprt_abs_idx,
        interface_row=row
    )

    # ===== Compatibilidad: si pasan overrides, fuerza primer item del primer IPRT =====
    if iprt_dest or iprt_gateway:
        mo = _find_mo_by_class_and_dist_contains(cmData, "com.nokia.srbts.tnl:IPRT", f"/IPRT-{iprt_index}")
        if mo is not None:
            lst = first(mo, "./r:list[@name='staticRoutes']")
            if lst is None:
                lst = ET.SubElement(mo, f"{{{NS['r']}}}list", {"name":"staticRoutes"})
            it = first(lst, "./r:item")
            if it is None:
                it = ET.SubElement(lst, f"{{{NS['r']}}}item")
            if iprt_dest:
                dp = first(it, "./r:p[@name='destIpAddr']") or ET.SubElement(it, f"{{{NS['r']}}}p", {"name":"destIpAddr"})
                dp.text = normalize_ip(iprt_dest)
            if iprt_gateway:
                gp = first(it, "./r:p[@name='gateway']") or ET.SubElement(it, f"{{{NS['r']}}}p", {"name":"gateway"})
                gp.text = normalize_ip(iprt_gateway)

    # Fecha en header
    header_log = first(cmData, "./r:header/r:log")
    if header_log is not None:
        header_log.set("dateTime", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

    # Serializar
    bio = io.BytesIO()
    try:
        ET.indent(tree, space="  ", level=0)
    except Exception:
        pass
    tree.write(bio, encoding="utf-8", xml_declaration=True)
    bio.seek(0)
    return bio.read()

# ===================== UI =====================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Clonador XML (Plantilla fija en ./doc)")
        self.geometry("1020x680")

        self.df: Optional[pd.DataFrame] = None
        self.df_iprt: Optional[pd.DataFrame] = None
        self.filtered_names: List[str] = []
        self.selected_name: Optional[str] = None

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

        middle = ttk.Panedwindow(self, orient=tk.HORIZONTAL); middle.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        left = ttk.Frame(middle); middle.add(left, weight=1)
        ttk.Label(left, text="Resultados (eNBName)").pack(anchor="w")
        self.listbox = tk.Listbox(left, height=18); self.listbox.pack(fill=tk.BOTH, expand=True)
        self.listbox.bind("<<ListboxSelect>>", self.on_select_name)

        right = ttk.Frame(middle); middle.add(right, weight=2)
        ttk.Label(right, text="Detalle de la fila seleccionada").pack(anchor="w")
        self.tree = ttk.Treeview(right, columns=("col", "val"), show="headings", height=18)
        self.tree.heading("col", text="Columna"); self.tree.heading("val", text="Valor")
        self.tree.column("col", width=340, anchor="w"); self.tree.column("val", width=560, anchor="w")
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
        top_master_override = dest_ip  # opcional: setear TOPF.masterIpAddr igual al "dest" genérico

        try:
            xml_bytes = build_xml_from_row_using_template(
                row,
                df_iprt=self.df_iprt,
                iprt_dest=dest_ip,
                iprt_gateway=gw_ip,
                top_master_override=top_master_override,
                iprt_index=IPRT_INDEX_DEFAULT
            )
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

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
