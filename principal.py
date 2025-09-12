# -*- coding: utf-8 -*-
"""
GUI para actualizar NOKLTE:LNCEL en un XML (plantilla RAML) a partir de un Excel.

- El XML base se asume en: doc/correcto.xml (si no existe, podrás seleccionarlo).
- La interfaz te deja elegir el Excel (.xlsx) y generar el XML actualizado.
- Parámetros que se reemplazan por cada LNCEL (buscando por cellName):
    phyCellId, tac, prachFreqOff, rootSeqIndex, prachCS, prachConfIndex, earfcnDL, pMax

Requisitos:
    pip install pandas openpyxl
"""

import sys
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import xml.etree.ElementTree as ET

# ===== Config =====
RAML_NS = "raml21.xsd"
NS = {"r": RAML_NS}
ET.register_namespace('', RAML_NS)

# Rutas por defecto relativas al script
SCRIPT_DIR = Path(__file__).resolve().parent
DOC_DIR = SCRIPT_DIR / "doc"
DEFAULT_XML = DOC_DIR / "correcto.xml"
DEFAULT_OUT = DOC_DIR / "correcto_actualizado.xml"

FIELDS = ["phyCellId", "tac", "prachFreqOff", "rootSeqIndex", "prachCS", "prachConfIndex", "earfcnDL", "pMax"]

# ===== Lógica de negocio =====
def read_excel(excel_path: Path) -> pd.DataFrame:
    df = pd.read_excel(excel_path, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    # Detectar columna cellName si no está explícita
    if "cellName" not in df.columns:
        first_col = df.columns[0]
        # si la primera columna parece contener nombres NEI.../ _L#, la usamos
        if df[first_col].astype(str).str.contains("_L", na=False).any() or df[first_col].astype(str).str.contains("NEI.", na=False).any():
            df = df.rename(columns={first_col: "cellName"})
        else:
            # usar índice como cellName
            df = df.rename_axis("cellName").reset_index()

    keep = ["cellName"] + [c for c in FIELDS if c in df.columns]
    df = df[keep]
    df["cellName"] = df["cellName"].astype(str).str.strip()
    for c in FIELDS:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def find_lncel_nodes(root):
    for mo in root.findall(".//r:managedObject", NS):
        if mo.get("class", "") == "NOKLTE:LNCEL":
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

def apply_row_to_lncel(mo, row: pd.Series):
    for field in FIELDS:
        if field in row and pd.notna(row[field]) and str(row[field]).strip() != "":
            set_param(mo, field, str(row[field]).strip())

def process(xml_in: Path, excel_path: Path, xml_out: Path):
    tree = ET.parse(xml_in)
    root = tree.getroot()
    df = read_excel(excel_path)
    df_indexed = df.set_index("cellName")
    updated = 0
    missing = []

    for mo in find_lncel_nodes(root):
        name = get_cell_name(mo)
        if not name:
            continue
        if name in df_indexed.index:
            row = df_indexed.loc[name]
            apply_row_to_lncel(mo, row)
            updated += 1
        else:
            missing.append(name)

    xml_out.parent.mkdir(parents=True, exist_ok=True)
    tree.write(xml_out, encoding="UTF-8", xml_declaration=True)

    not_in_xml = sorted(set(df_indexed.index) - set([get_cell_name(m) for m in find_lncel_nodes(root)]))

    return updated, missing, not_in_xml

# ===== GUI =====
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Generar XML desde Excel (NOKLTE:LNCEL)")
        self.geometry("680x360")

        # Variables
        self.var_xml = tk.StringVar(value=str(DEFAULT_XML))
        self.var_excel = tk.StringVar(value="")  # el usuario elige
        self.var_out = tk.StringVar(value=str(DEFAULT_OUT))

        # Layout
        pad = {"padx": 10, "pady": 6}

        # XML
        frm_xml = ttk.LabelFrame(self, text="Plantilla XML (RAML)")
        frm_xml.pack(fill="x", **pad)
        ttk.Entry(frm_xml, textvariable=self.var_xml).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_xml, text="Buscar…", command=self.browse_xml).pack(side="left", padx=(0,10), pady=8)

        # Excel
        frm_xls = ttk.LabelFrame(self, text="Excel con parámetros")
        frm_xls.pack(fill="x", **pad)
        ttk.Entry(frm_xls, textvariable=self.var_excel).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_xls, text="Seleccionar Excel…", command=self.browse_excel).pack(side="left", padx=(0,10), pady=8)

        # Salida
        frm_out = ttk.LabelFrame(self, text="Salida XML")
        frm_out.pack(fill="x", **pad)
        ttk.Entry(frm_out, textvariable=self.var_out).pack(side="left", fill="x", expand=True, padx=(10,6), pady=8)
        ttk.Button(frm_out, text="Cambiar…", command=self.browse_out).pack(side="left", padx=(0,10), pady=8)

        # Botón generar
        ttk.Button(self, text="Generar XML", command=self.run).pack(pady=10)

        # Log
        self.txt = tk.Text(self, height=10)
        self.txt.pack(fill="both", expand=True, padx=10, pady=(0,10))

    def log(self, msg: str):
        self.txt.insert("end", msg + "\n")
        self.txt.see("end")
        self.update_idletasks()

    def browse_xml(self):
        path = filedialog.askopenfilename(
            title="Selecciona XML base",
            filetypes=[("XML files","*.xml"), ("Todos","*.*")],
            initialdir=str(DOC_DIR if DOC_DIR.exists() else SCRIPT_DIR)
        )
        if path:
            self.var_xml.set(path)

    def browse_excel(self):
        path = filedialog.askopenfilename(
            title="Selecciona Excel",
            filetypes=[("Excel (*.xlsx)", "*.xlsx"), ("Todos", "*.*")]
        )
        if path:
            self.var_excel.set(path)

    def browse_out(self):
        path = filedialog.asksaveasfilename(
            title="Guardar XML actualizado como…",
            defaultextension=".xml",
            initialfile="correcto_actualizado.xml",
            filetypes=[("XML files", "*.xml")]
        )
        if path:
            self.var_out.set(path)

    def run(self):
        xml_path = Path(self.var_xml.get()).expanduser()
        excel_path = Path(self.var_excel.get()).expanduser()
        out_path = Path(self.var_out.get()).expanduser()

        # Validaciones
        if not xml_path.exists():
            messagebox.showerror("Error", f"No existe el XML base:\n{xml_path}")
            return
        if excel_path.suffix.lower() != ".xlsx" or not excel_path.exists():
            messagebox.showerror("Error", "Selecciona un archivo Excel válido (.xlsx).")
            return

        try:
            self.log(f"Procesando…\n  XML: {xml_path}\n  Excel: {excel_path}")
            updated, missing, not_in_xml = process(xml_path, excel_path, out_path)
            self.log(f"✅ Actualizado(s): {updated}.")
            if missing:
                self.log("⚠️ Celdas en XML sin fila en Excel:")
                for m in missing:
                    self.log(f"  - {m}")
            if not_in_xml:
                self.log("ℹ️ Celdas en Excel que NO existen en el XML (se ignoran):")
                for n in not_in_xml:
                    self.log(f"  - {n}")
            self.log(f"💾 Guardado en: {out_path}")
            messagebox.showinfo("Listo", f"XML generado:\n{out_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Ocurrió un error:\n{e}")
            self.log(f"❌ Error: {e}")

if __name__ == "__main__":
    app = App()
    app.mainloop()
