#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Dict, List, Tuple, Iterable, Optional

# ---------- Utilidades de normalización ----------

def strip_ns(tag: str) -> str:
    """Quita el namespace {uri} de un tag."""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag

def text_clean(s: Optional[str]) -> str:
    return (s or "").strip()

def key_for_element(el: ET.Element) -> str:
    """
    Construye una clave estable para identificar nodos.
    - managedObject: usa class + distName
    - p: usa atributo name
    - log: usa action (y opcionalmente dateTime, pero suele ignorarse)
    - genérico: tag + attrs ordenados
    """
    tag = strip_ns(el.tag)

    # Casos comunes en RAML Nokia
    if tag == "managedObject":
        cl = el.attrib.get("class", "")
        dn = el.attrib.get("distName", "")
        return f"{tag}[class={cl}][distName={dn}]"

    if tag == "p":
        name = el.attrib.get("name", "")
        return f"{tag}[name={name}]"

    if tag == "list":
        name = el.attrib.get("name", "")
        return f"{tag}[name={name}]"

    if tag == "item":
        # En listas sin identificador claro, usamos posición más tarde
        return tag

    # Para otros nodos, incluimos atributos ordenados
    if el.attrib:
        parts = [f'{k}={el.attrib[k]}' for k in sorted(el.attrib)]
        return f"{tag}[" + ",".join(parts) + "]"
    return tag

def build_path(parent_path: str, el: ET.Element, index: Optional[int]=None) -> str:
    """
    Construye una ruta legible. Si hay múltiples hijos con misma key,
    añade #idx para desambiguar.
    """
    k = key_for_element(el)
    if index is not None:
        k = f"{k}#{index}"
    return f"{parent_path}/{k}"

def enumerate_children_stably(el: ET.Element) -> List[Tuple[str, ET.Element]]:
    """
    Agrupa hijos por clave y devuelve una lista estable de (clave, hijo),
    agregando índice si hay repetidos.
    """
    groups: Dict[str, List[ET.Element]] = defaultdict(list)
    for child in list(el):
        groups[key_for_element(child)].append(child)

    ordered: List[Tuple[str, ET.Element]] = []
    for k in sorted(groups.keys()):
        lst = groups[k]
        if len(lst) == 1:
            ordered.append((k, lst[0]))
        else:
            for i, c in enumerate(lst, start=1):
                ordered.append((f"{k}#{i}", c))
    return ordered

def flatten_xml(el: ET.Element, parent_path: str = "") -> Dict[str, str]:
    """
    Convierte el árbol en un dict {ruta -> valor}
    - Para nodos con texto: ruta = texto
    - Para atributos: agrega /@attr = valor
    - Para nodos sin texto: se registran atributos y se sigue en profundidad
    """
    out: Dict[str, str] = {}

    # Atributos del nodo
    for k, v in sorted(el.attrib.items()):
        out[f"{parent_path}/@{k}"] = text_clean(v)

    # Texto del nodo (si es significativo)
    if text_clean(el.text):
        out[f"{parent_path}"] = text_clean(el.text)

    # Hijos (agrupados de forma estable)
    child_groups: Dict[str, List[ET.Element]] = defaultdict(list)
    for child in list(el):
        child_groups[key_for_element(child)].append(child)

    for key in sorted(child_groups.keys()):
        children = child_groups[key]
        if len(children) == 1:
            child = children[0]
            child_path = f"{parent_path}/{key}"
            out.update(flatten_xml(child, child_path))
        else:
            # desambiguar con índice
            for i, child in enumerate(children, start=1):
                child_path = f"{parent_path}/{key}#{i}"
                out.update(flatten_xml(child, child_path))

    return out

# ---------- Filtros para ignorar rutas ----------

DEFAULT_IGNORES = [
    r"/header/log/@dateTime$",     # cabecera volátil
    r"/cmData/@id$",               # id de plan puede variar
]

def make_ignore_matcher(patterns: Iterable[str]):
    regs = [re.compile(p) for p in patterns]
    def _match(path: str) -> bool:
        return any(r.search(path) for r in regs)
    return _match

# ---------- Comparación ----------

def compare_dicts(
    A: Dict[str, str], B: Dict[str, str], ignore_paths: Iterable[str]
) -> Tuple[List[str], List[str], List[Tuple[str, str, str]]]:
    """
    Devuelve:
      - solo_en_A: rutas presentes solo en A
      - solo_en_B: rutas presentes solo en B
      - dif_valor: lista de (ruta, valA, valB) donde existen en ambos pero distinto valor
    """
    ignore = make_ignore_matcher(ignore_paths)
    keysA = {k for k in A.keys() if not ignore(k)}
    keysB = {k for k in B.keys() if not ignore(k)}

    solo_en_A = sorted(keysA - keysB)
    solo_en_B = sorted(keysB - keysA)

    comunes = keysA & keysB
    dif_valor = []
    for k in sorted(comunes):
        if A[k] != B[k]:
            dif_valor.append((k, A[k], B[k]))

    return solo_en_A, solo_en_B, dif_valor

# ---------- Carga del XML (manejo de namespace por defecto) ----------

def parse_xml(path: str) -> ET.Element:
    """
    Carga el XML y devuelve la raíz real del contenido (por ej. <raml>).
    """
    parser = ET.XMLParser()
    tree = ET.parse(path, parser=parser)
    root = tree.getroot()
    return root

# ---------- Formato de salida ----------

def print_section(title: str):
    print("\n" + title)
    print("-" * len(title))

def main():
    ap = argparse.ArgumentParser(
        description="Compara dos XML RAML y muestra las partes no iguales (estructura, atributos y valores)."
    )
    ap.add_argument("xml_a", help="Archivo XML A (por ejemplo, correcto.xml)")
    ap.add_argument("xml_b", help="Archivo XML B (por ejemplo, NEI.Jose Carbonell_L1.xml)")
    ap.add_argument(
        "--ignore",
        nargs="*",
        default=DEFAULT_IGNORES,
        help="Lista de regex para ignorar rutas (por defecto ignora header/log/@dateTime y cmData/@id)",
    )
    args = ap.parse_args()

    try:
        rootA = parse_xml(args.xml_a)
        rootB = parse_xml(args.xml_b)
    except Exception as e:
        print(f"❌ Error al leer/parsing XML: {e}")
        sys.exit(1)

    # Construir rutas base con nombre de tag raíz (sin ns)
    baseA = f"/{strip_ns(rootA.tag)}"
    baseB = f"/{strip_ns(rootB.tag)}"

    flatA = flatten_xml(rootA, baseA)
    flatB = flatten_xml(rootB, baseB)

    soloA, soloB, diffvals = compare_dicts(flatA, flatB, args.ignore)

    total_diff = len(soloA) + len(soloB) + len(diffvals)

    print_section("Resumen")
    print(f"Rutas únicas en A: {len(soloA)}")
    print(f"Rutas únicas en B: {len(soloB)}")
    print(f"Rutas con valores distintos: {len(diffvals)}")
    print(f"Total diferencias: {total_diff}")

    if soloA:
        print_section("🔹 Solo en A")
        for k in soloA:
            v = flatA.get(k, "")
            print(f"{k} = {v}")

    if soloB:
        print_section("🔸 Solo en B")
        for k in soloB:
            v = flatB.get(k, "")
            print(f"{k} = {v}")

    if diffvals:
        print_section("⚖️ Valores distintos (A vs B)")
        for k, va, vb in diffvals:
            print(f"{k}")
            print(f"  A: {va}")
            print(f"  B: {vb}")

    if total_diff == 0:
        print("\n✅ Los XML son equivalentes (considerando las rutas ignoradas).")

if __name__ == "__main__":
    main()
