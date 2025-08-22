import io
import re
import ipaddress
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Dict

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import xml.etree.ElementTree as ET

# ===================== Configuración =====================
SHEET_NAME = "Interface"

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

# Alias de columnas -> nombre canónico
COLUMN_ALIASES = {
    # Ids y nombre
    "macro enb id": "lnBtsId",
    "lnbtsid": "lnBtsId",
    "ln bts id": "lnBtsId",
    "enbname": "eNBName",
    "enb name": "eNBName",
    "enb": "eNBName",

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

    # Ubicación (para que la lea del Excel con distintos encabezados)
    "modulelocation": "moduleLocation",
    "module location": "moduleLocation",
    "location": "moduleLocation",
}

REQUIRED_COLUMNS = ["lnBtsId", "eNBName"]

# ===================== Utilidades Excel =====================
def _canon_base(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

def canonize(col: str) -> str:
    base = _canon_base(col)
    return COLUMN_ALIASES.get(base, None) or str(col).strip()

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

def _autodetect_header_row(df_raw: pd.DataFrame) -> int:
    candidate_keys = [
        "macro eNB id", "lnBtsId", "eNBName", "enbName",
        "IP address of the network interface",
        "Network mask of the IP address", "VLAN identifier"
    ]
    max_scan = min(30, len(df_raw))
    for i in range(max_scan):
        row_vals = df_raw.iloc[i].tolist()
        cells = [str(x) for x in row_vals if (isinstance(x, str) or pd.notna(x))]
        if _row_has_keys(cells, candidate_keys, min_hits=2):
            return i
    return 0

def _finalize_after_header(df_raw: pd.DataFrame, header_row: int) -> pd.DataFrame:
    header_vals = df_raw.iloc[header_row].tolist()
    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = header_vals
    df = df.dropna(axis=1, how="all").dropna(axis=0, how="all")
    mapped = [canonize(c) for c in df.columns]
    df.columns = _dedupe_columns(mapped)
    return df

def read_interface_sheet(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=SHEET_NAME, header=None, dtype=object)
    hrow = _autodetect_header_row(raw)
    df = _finalize_after_header(raw, hrow)
    return df

def load_dataframe(initial_path: Optional[Path] = None) -> pd.DataFrame:
    # Ruta explícita (opcional)
    if initial_path:
        p = Path(initial_path).expanduser()
        if p.exists():
            return read_interface_sheet(p)
    # Ubicaciones por defecto
    for p in DEFAULT_LOCATIONS:
        if p.exists():
            return read_interface_sheet(p)
    # Diálogo si no se encontró
    sel = filedialog.askopenfilename(
        title=f"Selecciona el Excel (hoja '{SHEET_NAME}')",
        filetypes=[("Excel", "*.xlsx *.xlsm *.xls"), ("Todos", "*.*")]
    )
    if not sel:
        raise FileNotFoundError("No se seleccionó archivo de Excel.")
    return read_interface_sheet(Path(sel))

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

def get_block(df_row: pd.Series, idx: int) -> Dict[str, str]:
    suf = "" if idx == 1 else f"_{idx}"
    ip = sval(df_row, f"localIpAddr{suf}")
    nm = sval(df_row, f"netmask{suf}") or sval(df_row, f"netmask_{idx}")
    vlan = sval(df_row, f"vlanId{suf}")
    plen = to_prefix_len(nm) if nm else 0
    host_ip = pick_host_ip(ip, plen) if (ip and plen) else ip
    return {"ip": host_ip, "prefix": str(plen) if plen else "", "vlan": vlan}

# ===================== XML helpers (RAML Nokia) =====================
def xp(elem, query):  # xpath con namespace por defecto
    return elem.findall(query, NS)

def first(elem, query):
    lst = xp(elem, query)
    return lst[0] if lst else None

def get_template_mrbts_id(root) -> str:
    mr = first(root, ".//r:managedObject[@class='com.nokia.srbts:MRBTS']")
    if mr is None:
        return ""
    dist = mr.get("distName", "")
    m = re.search(r"MRBTS-(\d+)", dist)
    return m.group(1) if m else ""

def replace_all_distNames(cmData, old_id: str, new_id: str):
    if not old_id or not new_id or old_id == new_id:
        return
    for mo in xp(cmData, ".//r:managedObject"):
        dn = mo.get("distName", "")
        if f"MRBTS-{old_id}" in dn:
            mo.set("distName", dn.replace(f"MRBTS-{old_id}", f"MRBTS-{new_id}"))
        # Reemplaza también DNs dentro de <p> que contienen MRBTS-<id>
        for p in xp(mo, "./r:p"):
            if p.text and f"MRBTS-{old_id}" in p.text:
                p.text = p.text.replace(f"MRBTS-{old_id}", f"MRBTS-{new_id}")

def set_bts_name(cmData, new_name: str):
    mr = first(cmData, ".//r:managedObject[@class='com.nokia.srbts:MRBTS']")
    if mr is None:
        return
    p = first(mr, "./r:p[@name='btsName']")
    if p is None:
        p = ET.SubElement(mr, f"{{{NS['r']}}}p", {"name":"btsName"})
    p.text = new_name
    

# ---- Evitan 'invalid predicate' (no usan contains() en XPath) ----
def _iter_managed_objects(cmData):
    return xp(cmData, ".//r:managedObject")

def _find_mo_by_class_and_dist_contains(cmData, class_name: str, contains_str: str):
    for mo in _iter_managed_objects(cmData):
        if mo.get("class") == class_name and contains_str in (mo.get("distName") or ""):
            return mo
    return None

def set_vlan(cmData, idx: int, vlan_id: str):
    mo = _find_mo_by_class_and_dist_contains(
        cmData,
        "com.nokia.srbts.tnl:VLANIF",
        f"/VLANIF-{idx}"
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
        cmData,
        "com.nokia.srbts.tnl:IPADDRESSV4",
        f"/IPIF-{idx}/IPADDRESSV4-1"
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
    # limpiar hijos <p> actuales y reponer
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
    # master list
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
    # rate
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

# ======= Setter global para parámetros <p name="..."> (OPCIÓN 2) =======
def set_param_global(cmData, p_name: str, value: str, create_if_missing: bool = True):
    """
    Actualiza TODAS las ocurrencias de <p name="p_name"> en cualquier managedObject.
    Si no existe ninguna y create_if_missing=True, crea una bajo MRBTS (si existe), si no, en el primer managedObject.
    """
    if value is None:
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

# ===================== Construcción desde plantilla fija =====================
def build_xml_from_row_using_template(row: pd.Series) -> bytes:
    # Validación mínima
    missing = validate_required(row)
    if missing:
        raise RuntimeError("Faltan campos obligatorios: " + ", ".join(missing))
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"No encuentro la plantilla: {TEMPLATE_PATH}")

    lnBtsId = sval(row, "lnBtsId")
    enbName = sval(row, "eNBName")

    # Bloques de transporte (1..4)
    b1 = get_block(row, 1)
    b2 = get_block(row, 2)
    b3 = get_block(row, 3)
    b4 = get_block(row, 4)

    # NTP / TOP
    ntp_primary   = sval(row, "ntpPrimary")
    ntp_secondary = sval(row, "ntpSecondary")
    top_master    = sval(row, "topMasterIp")
    rate_raw      = sval(row, "topRate") or "32"

    # Cargar plantilla
    tree = ET.parse(str(TEMPLATE_PATH))
    root = tree.getroot()
    cmData = first(root, "./r:cmData")
    if cmData is None:
        raise RuntimeError("No se encontró <cmData> en la plantilla.")

    # MRBTS old -> new
    old_id = get_template_mrbts_id(cmData)
    replace_all_distNames(cmData, old_id, lnBtsId)

    # Nombre del sitio
    set_bts_name(cmData, enbName)


    module_loc = sval(row, "moduleLocation")
    if module_loc:
        set_param_global(cmData, "moduleLocation", module_loc, create_if_missing=True)
    else:
        # ⚠️ Forzar que moduleLocation = eNBName si no viene en Excel
        set_param_global(cmData, "moduleLocation", enbName, create_if_missing=True)

    # VLANs (1..4)
    set_vlan(cmData, 1, b1["vlan"])
    set_vlan(cmData, 2, b2["vlan"])
    set_vlan(cmData, 3, b3["vlan"])
    set_vlan(cmData, 4, b4["vlan"])

    # IP/prefix (1..4)
    set_ip_block(cmData, 1, b1["ip"], b1["prefix"])
    set_ip_block(cmData, 2, b2["ip"], b2["prefix"])
    set_ip_block(cmData, 3, b3["ip"], b3["prefix"])
    set_ip_block(cmData, 4, b4["ip"], b4["prefix"])

    # TOP apunta a IPIF-3 (como en tu plantilla)
    ensure_top_splane_points_to_ipif3(cmData)

    # NTP y TOP master/rate (si hay datos en Excel; si vienen vacíos, se mantienen los de la plantilla)
    if ntp_primary or ntp_secondary:
        set_ntp_servers(cmData, ntp_primary or "", ntp_secondary or "")
    if top_master or rate_raw:
        set_top_master_and_rate(cmData, top_master or "", rate_raw)

    # Actualiza fecha en header (sin tocar nada más)
    header_log = first(cmData, "./r:header/r:log")
    if header_log is not None:
        header_log.set("dateTime", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

    # Serializar sin cambiar nada más
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
        self.geometry("980x640")

        self.df: Optional[pd.DataFrame] = None
        self.filtered_names: List[str] = []
        self.selected_name: Optional[str] = None

        self.selected_name: Optional[str] = None

        self._build_widgets()

        try:
            self.df = load_dataframe()
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
        self.tree.column("col", width=320, anchor="w"); self.tree.column("val", width=520, anchor="w")
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
        self.hint_lbl.config(text=f"Filas: {len(self.df)} | Columnas: {len(self.df.columns)} (ej: {cols}...) | Plantilla: {TEMPLATE_PATH.name} [{tpl_ok}]")

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
        try:
            xml_bytes = build_xml_from_row_using_template(row)
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
