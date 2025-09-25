import re
import sys
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import pandas as pd
import xml.etree.ElementTree as ET

############################################################
# Configuración
############################################################
APP_TITLE = "XML Merger OSC — Base + Sectores (L1/L2/L3)"
DEFAULT_DOC_DIR = Path(__file__).parent / "Doc"

# Namespace RAML
RAML_NS = "raml21.xsd"
ET.register_namespace('', RAML_NS)
NS = {"r": RAML_NS}

# Archivos de sectores esperados dentro de Doc/
SECTOR_FILES = {
    "L1": "L1.xml",
    "L2": "L2.xml",
    "L3": "L3.xml",
}

# Mapeos de parámetros a actualizar en sectores (earfcnDL excluido)
SECTOR_PARAM_COLUMNS = [
    "phyCellId", "tac", "prachFreqOff", "rootSeqIndex", "prachCS",
    "prachConfIndex", "pMax",
]

############################################################
# Utilidades XML
############################################################
def _findall(root: ET.Element, xp: str) -> List[ET.Element]:
    return root.findall(xp, NS)

def _find(root: ET.Element, xp: str) -> Optional[ET.Element]:
    return root.find(xp, NS)

def _iter_mos_anyns(root: ET.Element) -> List[ET.Element]:
    mos = root.findall('.//{raml21.xsd}managedObject')
    mos += [e for e in root.iter() if isinstance(e.tag, str) and e.tag.endswith('managedObject') and '}' not in e.tag]
    return mos

############################################################
# Carga tolerante de XML de sectores
############################################################
def load_xml_tolerant(path: Path) -> ET.ElementTree:
    text = path.read_text(encoding='utf-8', errors='ignore').replace('\ufeff', '')
    try:
        return ET.ElementTree(ET.fromstring(text))
    except ET.ParseError:
        pass

    mos_txt = re.findall(r"<managedObject[\s\S]*?</managedObject>", text, flags=re.IGNORECASE)
    if not mos_txt:
        end = text.lower().find('</raml>')
        if end != -1:
            frag = text[:end+7]
            return ET.ElementTree(ET.fromstring(frag))
        raise ET.ParseError(f"No se pudieron extraer managedObject del XML de sector: {path}")

    raml = ET.Element(f"{{{RAML_NS}}}raml", attrib={"version": "2.1"})
    cm = ET.SubElement(raml, f"{{{RAML_NS}}}cmData", attrib={"type": "plan"})
    for mo_txt in mos_txt:
        wrapped = f"<wrapper xmlns=\"{RAML_NS}\">{mo_txt}</wrapper>"
        w = ET.fromstring(wrapped)
        mo_elem = list(w)[0]
        cm.append(mo_elem)
    return ET.ElementTree(raml)

############################################################
# Lógica principal
############################################################
class XmlExcelMerger:
    def __init__(self, base_xml_path: Path, excel_path: Path, doc_dir: Path, output_dir: Path):
        self.base_xml_path = Path(base_xml_path)
        self.excel_path = Path(excel_path)
        self.doc_dir = Path(doc_dir)
        self.output_dir = Path(output_dir)
        self.df = self._load_excel()
        self._validate_df()

    def _load_excel(self) -> pd.DataFrame:
        try:
            df = pd.read_excel(self.excel_path)
        except Exception:
            df = pd.read_csv(self.excel_path)
        df.columns = [str(c).strip() for c in df.columns]
        return df

    def _validate_df(self) -> None:
        required_any = {"name", "cellName"}
        missing = [c for c in required_any if c not in self.df.columns]
        if missing:
            raise ValueError(
                f"Faltan columnas en el Excel: {missing}. Debe existir al menos 'name' y 'cellName'."
            )

    def _load_xml(self, path: Path) -> ET.ElementTree:
        text = path.read_text(encoding='utf-8', errors='ignore').replace('\ufeff', '')
        return ET.ElementTree(ET.fromstring(text))

    def apply_base_replacements(self, tree: ET.ElementTree) -> None:
        root = tree.getroot()
        row_name = None
        for _, r in self.df.iterrows():
            n = str(r.get('name', '')).strip()
            if n:
                row_name = r
                break
        if row_name is None:
            raise ValueError("No encontré ninguna fila con la columna 'name' poblada.")

        name_value = str(row_name.get('name', '')).strip()
        bts_value = str(row_name.get('btsName', name_value)).strip()
        mod_value = str(row_name.get('moduleLocation', name_value)).strip()
        enb_value = str(row_name.get('enbName', name_value)).strip()

        for pname, val in [("btsName", bts_value), ("moduleLocation", mod_value), ("enbName", enb_value)]:
            for p in _findall(root, ".//r:p[@name='" + pname + "']"):
                p.text = val

        for p in _findall(root, ".//r:p[@name='cellName']"):
            if p.text:
                m = re.match(r"^(.*?)(_[ML]\d)\s*$", p.text.strip(), flags=re.I)
                if m:
                    suffix = m.group(2)
                    p.text = f"{name_value}{suffix}"

        self._replace_anywhere(root, "473042A", "475964A")

        lnbts_id = str(row_name.get('LNBTSID', '')).strip()
        if lnbts_id:
            self._rewrite_mrbts_lnbts_ids(root, lnbts_id)

    def _replace_anywhere(self, root: ET.Element, old: str, new: str) -> None:
        old, new = str(old), str(new)
        for elem in root.iter():
            for a, v in list(elem.attrib.items()):
                if v and old in v:
                    elem.attrib[a] = v.replace(old, new)
            if elem.text and old in elem.text:
                elem.text = elem.text.replace(old, new)
            if elem.tail and old in elem.tail:
                elem.tail = elem.tail.replace(old, new)

    def _rewrite_mrbts_lnbts_ids(self, root: ET.Element, lnbts_id: str) -> None:
        pat = re.compile(r"\b(MRBTS|LNBTS)-(\d+)\b")
        for mo in _findall(root, ".//r:managedObject"):
            dn = mo.attrib.get('distName')
            if dn:
                new_dn = pat.sub(lambda m: f"{m.group(1)}-{lnbts_id}", dn)
                mo.attrib['distName'] = new_dn
        for e in root.iter():
            for a, v in list(e.attrib.items()):
                if v and pat.search(v):
                    e.attrib[a] = pat.sub(lambda m: f"{m.group(1)}-{lnbts_id}", v)
            if e.text and pat.search(e.text):
                e.text = pat.sub(lambda m: f"{m.group(1)}-{lnbts_id}", e.text)

    def build_sector_trees(self) -> List[ET.ElementTree]:
        sector_rows = []
        for _, r in self.df.iterrows():
            cn = str(r.get('cellName', '')).strip()
            if cn:
                sector_rows.append(r)
        if not sector_rows:
            return []

        wanted_labels: Set[str] = set()
        by_label_rows: Dict[str, List[pd.Series]] = {"L1": [], "L2": [], "L3": []}
        for r in sector_rows:
            cn = str(r.get('cellName', '')).strip()
            m = re.search(r"_([Ll][123])\b", cn)
            if not m:
                continue
            label = m.group(1).upper()
            wanted_labels.add(label)
            by_label_rows[label].append(r)

        trees: List[ET.ElementTree] = []
        for label in sorted(wanted_labels):
            file_name = SECTOR_FILES.get(label)
            if not file_name:
                raise FileNotFoundError(f"No tengo un archivo configurado para el sector {label}.")
            fpath = self.doc_dir / file_name
            if not fpath.exists():
                raise FileNotFoundError(f"No encontré el XML de sector {label} en: {fpath}")
            t = load_xml_tolerant(fpath)
            r0 = by_label_rows[label][0]
            self._fill_sector_params(t.getroot(), r0)
            lnbts_id = str(r0.get('LNBTSID', '')).strip()
            if lnbts_id:
                self._rewrite_mrbts_lnbts_ids(t.getroot(), lnbts_id)
            trees.append(t)
        return trees

    def _fill_sector_params(self, root: ET.Element, row: pd.Series) -> None:
        cn = str(row.get('cellName', '')).strip()
        for p in _findall(root, ".//r:p[@name='cellName']"):
            p.text = cn
        for col in SECTOR_PARAM_COLUMNS:
            val = row.get(col, None)
            if pd.notna(val):
                sval = str(int(val)) if isinstance(val, (int, float)) and float(val).is_integer() else str(val)
                for p in _findall(root, f".//r:p[@name='{col}']"):
                    p.text = sval

    def merge_all(self, base_tree: ET.ElementTree, sector_trees: List[ET.ElementTree]) -> ET.ElementTree:
        base_root = base_tree.getroot()
        cmdata = _find(base_root, ".//r:cmData")
        if cmdata is None:
            raise ValueError("El XML base no contiene <cmData>.")
        for st in sector_trees:
            sroot = st.getroot()
            scmdata = _find(sroot, ".//r:cmData")
            mos = _iter_mos_anyns(scmdata if scmdata is not None else sroot)
            for mo in mos:
                cmdata.append(mo)
        return base_tree

    def run(self) -> Path:
        base_tree = self._load_xml(self.base_xml_path)
        self.apply_base_replacements(base_tree)
        sector_trees = self.build_sector_trees()
        merged = self.merge_all(base_tree, sector_trees)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        out_path = self.output_dir / f"merged_{ts}.xml"
        merged.write(out_path, encoding='utf-8', xml_declaration=True)
        return out_path

############################################################
# GUI Tkinter
############################################################
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("780x520")
        self.resizable(True, True)
        self.configure(padx=16, pady=16)

        self.var_base_xml = tk.StringVar()
        self.var_excel = tk.StringVar()
        self.var_doc = tk.StringVar(value=str(DEFAULT_DOC_DIR))
        self.var_output = tk.StringVar()

        self._build_ui()

    def _build_ui(self):
        frm = ttk.Frame(self)
        frm.pack(fill='both', expand=True)

        ttk.Label(frm, text="XML base:").grid(row=0, column=0, sticky='w')
        ttk.Entry(frm, textvariable=self.var_base_xml, width=80).grid(row=1, column=0, sticky='we', padx=(0,8))
        ttk.Button(frm, text="Elegir...", command=self._choose_xml).grid(row=1, column=1)

        ttk.Label(frm, text="Excel con datos:").grid(row=2, column=0, sticky='w', pady=(12,0))
        ttk.Entry(frm, textvariable=self.var_excel, width=80).grid(row=3, column=0, sticky='we', padx=(0,8))
        ttk.Button(frm, text="Elegir...", command=self._choose_excel).grid(row=3, column=1)

        ttk.Label(frm, text="Carpeta Doc con sectores (L1/L2/L3):").grid(row=4, column=0, sticky='w', pady=(12,0))
        ttk.Entry(frm, textvariable=self.var_doc, width=80).grid(row=5, column=0, sticky='we', padx=(0,8))
        ttk.Button(frm, text="Cambiar...", command=self._choose_doc).grid(row=5, column=1)

        ttk.Label(frm, text="Carpeta de salida:").grid(row=6, column=0, sticky='w', pady=(12,0))
        ttk.Entry(frm, textvariable=self.var_output, width=80).grid(row=7, column=0, sticky='we', padx=(0,8))
        ttk.Button(frm, text="Elegir...", command=self._choose_output).grid(row=7, column=1)

        ttk.Button(frm, text="Generar XML Unificado", command=self._run).grid(row=8, column=0, pady=16, sticky='w')

        ttk.Label(frm, text="Log:").grid(row=9, column=0, sticky='w')
        self.txt = tk.Text(frm, height=12)
        self.txt.grid(row=10, column=0, columnspan=2, sticky='nsew')

        frm.columnconfigure(0, weight=1)
        frm.rowconfigure(10, weight=1)

    def _choose_xml(self):
        p = filedialog.askopenfilename(title="Seleccionar XML base", filetypes=[("XML","*.xml"), ("Todos","*.*")])
        if p:
            self.var_base_xml.set(p)

    def _choose_excel(self):
        p = filedialog.askopenfilename(title="Seleccionar Excel", filetypes=[("Excel","*.xlsx;*.xls"), ("CSV","*.csv"), ("Todos","*.*")])
        if p:
            self.var_excel.set(p)

    def _choose_doc(self):
        p = filedialog.askdirectory(title="Seleccionar carpeta Doc")
        if p:
            self.var_doc.set(p)

    def _choose_output(self):
        p = filedialog.askdirectory(title="Seleccionar carpeta de salida")
        if p:
            self.var_output.set(p)

    def _run(self):
        try:
            base = Path(self.var_base_xml.get())
            xl = Path(self.var_excel.get())
            doc = Path(self.var_doc.get())
            out_dir = Path(self.var_output.get()) if self.var_output.get() else Path(__file__).parent / "salidas"
            if not base.exists():
                messagebox.showerror(APP_TITLE, "Debes seleccionar el XML base.")
                return
            if not xl.exists():
                messagebox.showerror(APP_TITLE, "Debes seleccionar el Excel.")
                return
            if not doc.exists():
                messagebox.showerror(APP_TITLE, f"La carpeta Doc no existe: {doc}")
                return
            self._log(f"Base XML: {base}")
            self._log(f"Excel: {xl}")
            self._log(f"Doc: {doc}")
            self._log(f"Salida: {out_dir}")

            merger = XmlExcelMerger(base, xl, doc, out_dir)
            out = merger.run()
            self._log(f"✅ Generado: {out}")
            messagebox.showinfo(APP_TITLE, f"Archivo generado:\n{out}")
        except Exception as e:
            tb = traceback.format_exc()
            self._log(tb)
            messagebox.showerror(APP_TITLE, f"Error: {e}")

    def _log(self, msg: str):
        self.txt.insert('end', msg + "\n")
        self.txt.see('end')


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
