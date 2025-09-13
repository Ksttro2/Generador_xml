# -*- coding: utf-8 -*-
"""
GUI para unir 3 XML (correcto.xml + SCRIP_PUERTOS_L1.xml + T1.xml) y actualizar
NOKLTE:LNCEL según un Excel. Soporta XML completos o fragmentos de managedObject.

Requisitos:
    pip install pandas openpyxl
"""

import re
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

import pandas as pd
import xml.etree.ElementTree as ET

# ================== Config / Namespace ==================
RAML_NS = "raml21.xsd"
NS = {"r": RAML_NS}
ET.register_namespace('', RAML_NS)

# Campos LNCEL a actualizar
FIELDS = [
    "phyCellId", "tac", "prachFreqOff", "rootSeqIndex",
    "prachCS", "prachConfIndex", "earfcnDL", "pMax"
]

# ================== Utilidades de texto/XML ==================
def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return path.read_text(encoding="latin-1")

def _strip_extra_xml_decls(s: str) -> str:
    # Deja sólo la primera declaración XML si hay varias
    decls = list(re.finditer(r'<\?xml[^>]*\?>', s, flags=re.IGNORECASE))
    if len(decls) > 1:
        keep_end = decls[0].end()
        s = s[:keep_end] + re.sub(r'<\?xml[^>]*\?>', '', s[keep_end:], flags=re.IGNORECASE)
    return s

def load_xml_or_fragments(path: Path):
    """
    Si es XML bien formado -> ('tree', ElementTree)
    Si es fragmento con 1..N <managedObject> -> ('fragments', List[Element]) (MISMO ORDEN)
    Si no se puede leer -> ('empty', [])
    """
    if not path or not path.exists():
        return 'empty', []

    # 1) intento: XML completo
    try:
        tree = ET.parse(path)
        return 'tree', tree
    except ET.ParseError:
        pass

    # 2) fragmentos
    text = _read_text(path)
    text = _strip_extra_xml_decls(text).strip()
    if not text:
        return 'empty', []

    # método principal: wrapper
    try:
        wrapper = f'<wrap xmlns="{RAML_NS}">\n{text}\n</wrap>'
        root = ET.fromstring(wrapper)
        mos = [n for n in root if n.tag.endswith('managedObject')]
        if mos:
            return 'fragments', mos  # orden original
    except ET.ParseError:
        pass

    # fallback: extraer bloques por regex (mantiene orden de aparición)
    blocks = re.findall(r'(<managedObject\b.*?</managedObject>)', text, flags=re.DOTALL | re.IGNORECASE)
    mos = []
    for b in blocks:
        try:
            tmp = ET.fromstring(f'<wrap xmlns="{RAML_NS}">{b}</wrap>')
            for child in tmp:
                mos.append(child)
        except ET.ParseError:
            pass
    return ('fragments', mos) if mos else ('empty', [])

def find_cmdata(root: ET.Element) -> ET.Element:
    cm = root.find(".//r:cmData", NS)
    return cm if cm is not None else root

def extract_mrbts_id(root: ET.Element) -> str:
    """Busca class='com.nokia.srbts:MRBTS' y devuelve 'MRBTS-XXXX' del distName."""
    mo = root.find(".//r:managedObject[@class='com.nokia.srbts:MRBTS']", NS)
    if mo is not None:
        dn = mo.get("distName", "")
        m = re.match(r"(MRBTS-\d+)", dn)
        if m:
            return m.group(1)
    return ""

def fix_mrbts_in_distname(distname: str, mrbts_id: str) -> str:
    """Alinea el prefijo MRBTS-#### del distName con el del XML base."""
    if not distname and mrbts_id:
        return mrbts_id
    if not distname:
        return distname
    parts = distname.split('/')
    if parts[0].startswith('MRBTS-'):
        if mrbts_id and parts[0] != mrbts_id:
            parts[0] = mrbts_id
            return '/'.join(parts)
        return distname
    else:
        return f"{mrbts_id}/{distname}" if mrbts_id else distname

def append_managed_objects(dst_cmdata: ET.Element, src_spec, mrbts_id_dst: str):
    """
    Anexa TODOS los <managedObject> de src en el MISMO ORDEN en que vienen.
    NO ordena, NO filtra, NO deduplica. Sólo corrige el prefijo MRBTS del distName.
    """
    kind, payload = src_spec
    if kind == 'empty':
        return

    if kind == 'tree':
        src_root = payload.getroot()
        mos = src_root.findall(".//r:managedObject", NS)
    elif kind == 'fragments':
        mos = payload
    else:
        mos = []

    for mo in mos:
        # clonar para no mover referencias
        mo_copy = ET.fromstring(ET.tostring(mo, encoding="utf-8"))
        dn = mo_copy.get("distName", "")
        mo_copy.set("distName", fix_mrbts_in_distname(dn, mrbts_id_dst))
        dst_cmdata.append(mo_copy)

# ================== Excel / Update LNCEL ==================
def normalize_decimal(val: str) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    return s

def read_excel(excel_path: Path) -> pd.DataFrame:
    df = pd.read_excel(excel_path, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    lower = {c.lower(): c for c in df.columns}

    # ENBNAME
    enb_col = next((lower[c] for c in ["enbname","btsname","name","base","sitio","nodo","neiname"] if c in lower), None)
    if enb_col is None:
        enb_col = df.columns[0]
    df = df.rename(columns={enb_col: "ENBNAME"})

    # T1 (opcional)
    t1_col = next((lower[c] for c in ["t1","cellname_t1","nombre_t1"] if c in lower), None)
    if t1_col is None and len(df.columns) > 1:
        possible = df.columns[1]
        if df[possible].astype(str).str.contains(r"_T1\b", na=False).any():
            t1_col = possible
    if t1_col:
        df = df.rename(columns={t1_col: "T1"})
    else:
        df["T1"] = ""

    # lnBtsId (opcional)
    for cand in ["lnbtsid","ln_bts_id","macro enb id","macro_enb_id","ln bts id","id","mrbts","lnbts"]:
        if cand in lower:
            df = df.rename(columns={lower[cand]: "lnBtsId"})
            break
    if "lnBtsId" not in df.columns:
        df["lnBtsId"] = ""

    # LNCEL fields + alias
    for col in FIELDS:
        if col not in df.columns:
            alias = {
                "earfcnDL": ["earfcndl","earfcn_dl","earfcn"],
                "pMax": ["pmax","p max","potencia","power"],
            }.get(col, [])
            found = next((lower[a] for a in alias if a in lower), None)
            if found:
                df = df.rename(columns={found: col})
            else:
                df[col] = ""

    # ECI/RZ si vienen en cadena (p. ej. "ECI=1148416::RZ=22")
    col_ecgi = None
    for c in df.columns:
        if df[c].astype(str).str.contains(r"ECI\s*=\s*\d+", na=False).any():
            col_ecgi = c; break

    def parse_eci_rz(cell: str):
        eci, rz = "", ""
        s = str(cell) if pd.notna(cell) else ""
        m_eci = re.search(r"ECI\s*=\s*(\d+)", s, re.IGNORECASE)
        if m_eci: eci = m_eci.group(1)
        m_rz = re.search(r"RZ\s*=\s*(\d+)", s, re.IGNORECASE)
        if m_rz: rz = m_rz.group(1)
        return eci, rz

    df["ECI"] = ""
    df["RZ"]  = ""
    if col_ecgi:
        parsed = df[col_ecgi].apply(parse_eci_rz)
        df["ECI"] = parsed.apply(lambda t: t[0])
        df["RZ"]  = parsed.apply(lambda t: t[1])
    else:
        if "ECI" not in df.columns: df["ECI"] = ""
        if "RZ"  not in df.columns: df["RZ"]  = ""

    # Limpieza
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    df["pMax"] = df["pMax"].apply(normalize_decimal)

    # cellName para casar con LNCEL
    if "cellName" not in df.columns:
        df["cellName"] = df.apply(lambda r: r["T1"] if r.get("T1","").strip() else r["ENBNAME"], axis=1)

    keep_cols = list(dict.fromkeys(["ENBNAME","T1","lnBtsId","ECI","RZ","cellName"] + FIELDS))
    return df[keep_cols]

def find_lncel_nodes(root):
    for mo in root.findall(".//r:managedObject", NS):
        if mo.get("class","") == "NOKLTE:LNCEL":
            yield mo

def get_param(mo, name: str):
    return mo.find(f"./r:p[@name='{name}']", NS)

def set_param(mo, name: str, value: str):
    node = get_param(mo, name)
    if node is None:
        node = ET.SubElement(mo, f"{{{RAML_NS}}}p", {"name": name})
    node.text = value

def get_cell_name(mo) -> str:
    node = get_param(mo, "cellName")
    return node.text.strip() if node is not None and node.text else ""

def update_lncel_from_df(root: ET.Element, df: pd.DataFrame):
    df_indexed = df.set_index("cellName")
    updated = 0
    missing = []
    for mo in find_lncel_nodes(root):
        name = get_cell_name(mo)
        if not name:
            continue
        if name in df_indexed.index:
            row = df_indexed.loc[name]
            for field in FIELDS:
                val = row.get(field, "")
                if pd.notna(val) and str(val).strip() != "":
                    set_param(mo, field, str(val).strip())
            updated += 1
        else:
            missing.append(name)
    # celdas del Excel que no existen en el XML
    lncel_names = [get_cell_name(m) for m in find_lncel_nodes(root)]
    not_in_xml = sorted(set(df_indexed.index) - set(lncel_names))
    return updated, missing, not_in_xml

# ================== Merge + Update ==================
def merge_and_update(base_xml: Path, puertos_xml: Path, t1_xml: Path, excel_path: Path, out_xml: Path):
    # 1) Cargar base y obtener cmData + MRBTS
    base_tree = ET.parse(base_xml)
    base_root = base_tree.getroot()
    dst_cmdata = find_cmdata(base_root)
    mrbts_id = extract_mrbts_id(base_root)

    # 2) Anexar los otros dos XML (como árbol completo o fragmentos) conservando ORDEN
    if puertos_xml and puertos_xml.exists():
        puertos_spec = load_xml_or_fragments(puertos_xml)
        append_managed_objects(dst_cmdata, puertos_spec, mrbts_id)
    if t1_xml and t1_xml.exists():
        t1_spec = load_xml_or_fragments(t1_xml)
        append_managed_objects(dst_cmdata, t1_spec, mrbts_id)

    # 3) Leer Excel y actualizar LNCEL
    df = read_excel(excel_path)
    updated, missing, not_in_xml = update_lncel_from_df(base_root, df)

    # 4) Guardar
    out_xml.parent.mkdir(parents=True, exist_ok=True)
    base_tree.write(out_xml, encoding="UTF-8", xml_declaration=True)
    return updated, missing, not_in_xml

# ================== GUI ==================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Unir XML (3) + Actualizar LNCEL desde Excel")
        self.geometry("820x520")

        # Variables
        self.var_base = tk.StringVar()
        self.var_puertos = tk.StringVar()
        self.var_t1 = tk.StringVar()
        self.var_excel = tk.StringVar()
        self.var_out = tk.StringVar(value="doc/correcto_actualizado.xml")

        pad = {"padx": 10, "pady": 6}

        # Base XML
        frm_base = ttk.LabelFrame(self, text="XML base (correcto.xml)")
        frm_base.pack(fill="x", **pad)
        ttk.Entry(frm_base, textvariable=self.var_base).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_base, text="Buscar…", command=self.browse_base).pack(side="left", padx=(0,10), pady=8)

        # Puertos XML
        frm_puertos = ttk.LabelFrame(self, text="XML Puertos (SCRIP_PUERTOS_T1.xml)")
        frm_puertos.pack(fill="x", **pad)
        ttk.Entry(frm_puertos, textvariable=self.var_puertos).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_puertos, text="Buscar…", command=self.browse_puertos).pack(side="left", padx=(0,10), pady=8)

        # T1 XML
        frm_t1 = ttk.LabelFrame(self, text="XML T1 (T1.xml)")
        frm_t1.pack(fill="x", **pad)
        ttk.Entry(frm_t1, textvariable=self.var_t1).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_t1, text="Buscar…", command=self.browse_t1).pack(side="left", padx=(0,10), pady=8)

        # Excel
        frm_xls = ttk.LabelFrame(self, text="Excel parámetros")
        frm_xls.pack(fill="x", **pad)
        ttk.Entry(frm_xls, textvariable=self.var_excel).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_xls, text="Seleccionar Excel…", command=self.browse_excel).pack(side="left", padx=(0,10), pady=8)

        # Salida
        frm_out = ttk.LabelFrame(self, text="Salida XML")
        frm_out.pack(fill="x", **pad)
        ttk.Entry(frm_out, textvariable=self.var_out).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_out, text="Cambiar…", command=self.browse_out).pack(side="left", padx=(0,10), pady=8)

        # Botón ejecutar
        ttk.Button(self, text="Unir + Actualizar", command=self.run).pack(pady=10)

        # Log
        self.txt = tk.Text(self, height=12)
        self.txt.pack(fill="both", expand=True, padx=10, pady=(0,10))

    # --- Browsers ---
    def browse_base(self):
        p = filedialog.askopenfilename(title="Selecciona XML base", filetypes=[("XML","*.xml"),("Todos","*.*")])
        if p: self.var_base.set(p)

    def browse_puertos(self):
        p = filedialog.askopenfilename(title="Selecciona XML Puertos", filetypes=[("XML","*.xml"),("Todos","*.*")])
        if p: self.var_puertos.set(p)

    def browse_t1(self):
        p = filedialog.askopenfilename(title="Selecciona XML T1", filetypes=[("XML","*.xml"),("Todos","*.*")])
        if p: self.var_t1.set(p)

    def browse_excel(self):
        p = filedialog.askopenfilename(title="Selecciona Excel", filetypes=[("Excel","*.xlsx"),("Todos","*.*")])
        if p: self.var_excel.set(p)

    def browse_out(self):
        p = filedialog.asksaveasfilename(title="Guardar XML unido/actualizado como…",
                                         defaultextension=".xml", initialfile="correcto_actualizado.xml",
                                         filetypes=[("XML","*.xml")])
        if p: self.var_out.set(p)

    # --- Helpers ---
    def log(self, msg: str):
        self.txt.insert("end", msg + "\n")
        self.txt.see("end")
        self.update_idletasks()

    # --- Run ---
    def run(self):
        base = Path(self.var_base.get()).expanduser()
        puertos = Path(self.var_puertos.get()).expanduser()
        t1 = Path(self.var_t1.get()).expanduser()
        excel = Path(self.var_excel.get()).expanduser()
        outp = Path(self.var_out.get()).expanduser()

        # Validaciones mínimas
        if not base.exists():
            messagebox.showerror("Error", f"No existe el XML base:\n{base}")
            return
        if not puertos.exists():
            messagebox.showerror("Error", f"No existe el XML Puertos:\n{puertos}")
            return
        if not t1.exists():
            messagebox.showerror("Error", f"No existe el XML T1:\n{t1}")
            return
        if excel.suffix.lower() != ".xlsx" or not excel.exists():
            messagebox.showerror("Error", "Selecciona un archivo Excel válido (.xlsx).")
            return

        try:
            self.log("Uniendo XML…")
            updated, missing, not_in_xml = merge_and_update(base, puertos, t1, excel, outp)
            self.log(f"✅ LNCEL actualizados: {updated}")
            if missing:
                self.log("⚠️ Celdas presentes en XML pero sin fila en Excel:")
                for m in missing:
                    self.log(f"  - {m}")
            if not_in_xml:
                self.log("ℹ️ Celdas en Excel que NO existen en el XML unido (se ignoran):")
                for n in not_in_xml:
                    self.log(f"  - {n}")
            self.log(f"💾 Archivo final: {outp}")
            messagebox.showinfo("Listo", f"XML generado:\n{outp}")
        except Exception as e:
            messagebox.showerror("Error", f"Ocurrió un error:\n{e}")
            self.log(f"❌ Error: {e}")

# ================== main ==================
if __name__ == "__main__":
    app = App()
    app.mainloop()
