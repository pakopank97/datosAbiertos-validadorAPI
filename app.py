# -*- coding: utf-8 -*-
"""
Sistema: API-Validador-Formatos-Datos-Abiertos
Autor: Mtro. Francisco Daniel Martínez Martínez — Jefe de Departamento de Procesos Orientados a la Transparencia
Tecnologías: Flask, Polars, OpenPyXL, ReportLab
Versión: v5 (acuse institucional PDF no editable, numerador automático de oficio, solo descarga PDF)
"""
import io, os, re, json
from datetime import datetime
from flask import Flask, request, render_template, send_file
from werkzeug.utils import secure_filename
import polars as pl

# ReportLab
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib import colors

# ---------------- Config ----------------
ALLOWED_EXTENSIONS = {"csv", "xls", "xlsx"}
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "resultados"
LOGOS_FOLDER = "logos"
OFICIO_PREFIJO = "253"  # fijo, por instrucción
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 256MB

ART_PREP = {"de","la","del","el","en","para","por","DE","LA","DEL","EL","EN","PARA","POR"}

# --------- Utilidades ----------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def split_words_underscore(name: str) -> int:
    return len([p for p in name.split("_") if p])

def detect_iso8601_strict(s: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s))

def is_utf8(file_bytes: bytes) -> bool:
    try:
        file_bytes.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False

def generar_no_oficio():
    """Genera y persiste un No. de Oficio: 253.-0001-YYYY"""
    contador_path = os.path.join(RESULTS_FOLDER, "contador_oficios.txt")
    try:
        with open(contador_path, "r", encoding="utf-8") as f:
            consecutivo = int(f.read().strip())
    except Exception:
        consecutivo = 0
    consecutivo += 1
    with open(contador_path, "w", encoding="utf-8") as f:
        f.write(str(consecutivo))
    año = datetime.now().year
    return f"{OFICIO_PREFIJO}.-{consecutivo:04d}-{año}"

# ---------------- Validadores ----------------
def validar_formato_y_carga(file_storage, filename, ext):
    obs = []
    if ext in ("xls", "xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(file_storage, read_only=True, data_only=True)
        sheetnames = wb.sheetnames
        if len(sheetnames) != 1:
            obs.append(f"El archivo no tiene el formato correcto y tiene {len(sheetnames)} hojas.")
        ws = wb[sheetnames[0]]
        rows = list(ws.values)
        if not rows:
            df = pl.DataFrame()
        else:
            headers = [str(h) if h is not None else "" for h in rows[0]]
            data = rows[1:]
            df = pl.DataFrame(data, schema=[h if h != "None" else "" for h in headers])
        empty_headers = [h for h in df.columns if (h is None or str(h).strip() in ("", " "))]
        if empty_headers:
            obs.append(f"Se encuentran {len(empty_headers)} variables sin nombre. Revisar el contenido de estas variables.")
        return obs, df

    # CSV (acepta FileStorage o BytesIO)
    if isinstance(file_storage, io.BytesIO):
        file_bytes = file_storage.getvalue()
        file_stream = io.BytesIO(file_bytes)
    else:
        file_bytes = file_storage.read()
        file_stream = io.BytesIO(file_bytes)

    if not is_utf8(file_bytes):
        obs.append("La codificación no es la correcta, debe ser 'UTF-8'.")

    try:
        df = pl.read_csv(file_stream, infer_schema_length=2000, ignore_errors=True)
    except Exception as e:
        obs.append(f"No fue posible leer el CSV: {e}")
        df = pl.DataFrame()

    empty_headers = [h for h in df.columns if (h is None or str(h).strip() in ("", " "))]
    if empty_headers:
        obs.append(f"Se encuentran {len(empty_headers)} variables sin nombre. Revisar el contenido de estas variables.")
    return obs, df

def validar_nombre_archivo(nom_arch: str):
    obs = []
    if re.search(r"[ñáéíóúüÑÁÉÍÓÚÜ]", nom_arch):
        obs.append("El nombre del archivo contiene caracteres especiales (ñ, tildes, diéresis).")
    for p in ART_PREP:
        if f"_{p}_" in nom_arch:
            obs.append(f"El nombre del archivo incluye la preposición/artículo: {p}")
    if re.search(r"[A-Z]", nom_arch) and not re.fullmatch(r"[A-Z0-9_]+", nom_arch):
        obs.append("El nombre del archivo debe estar en minúsculas, salvo siglas.")
    if " " in nom_arch:
        obs.append("El nombre del archivo no debe tener espacios. Se recomienda usar guiones bajos para separar palabras.")
    if not obs:
        return ["No se encontraron observaciones con el nombre del archivo."]
    return obs

def validar_nombres_columnas(df: pl.DataFrame):
    obs = []
    cols = df.columns
    especiales = [c for c in cols if re.search(r"[ñáéíóúüÑÁÉÍÓÚÜ]", c or "")]
    if especiales:
        obs.append("Nombre de columnas con caracteres especiales (ñ, tildes, diéresis): " + ", ".join(especiales))
    espacios = [c for c in cols if (" " in (c or "")) or ("\n" in (c or ""))]
    if espacios:
        obs.append("Nombre de columnas con espacios o saltos de linea: " + ", ".join(espacios) + ". Se recomienda utilizar guión bajo")
    largas = [c for c in cols if split_words_underscore(c or "") > 5]
    if largas:
        obs.append("Nombre de columnas con más de 5 palabras: " + ", ".join(largas))
    if any((c or "").lower() == "id" for c in cols):
        obs.append("No se permite llamar a una columna 'id'. Usar un nombre más descriptivo.")
    sufijo_mal = [c for c in cols if re.search(r"_[0-9]$", c or "") and ("extra" not in (c or ""))]
    if sufijo_mal:
        obs.append("Nombres de columnas terminan en sufijo de un solo dígito (debe ser con 2 dígitos, ej. _01): " + ", ".join(sufijo_mal))
    preps_cols = set()
    for c in cols:
        if c is None:
            continue
        if any(re.search(rf"(_{p}_)|(\s{p}\s)", c) for p in ART_PREP):
            preps_cols.add(c)
    if preps_cols:
        if len(preps_cols) == 1:
            obs.append(f"El nombre de la columna: {list(preps_cols)[0]} tiene artículos o preposiciones")
        else:
            obs.append("El nombre de las columnas: " + ", ".join(sorted(preps_cols)) + " tienen artículos o preposiciones")
    if not obs:
        return ["No se encontraron observaciones de los nombres de las columnas"]
    return obs

def validar_datos(df: pl.DataFrame):
    obs = []
    if df.is_empty():
        return ["No se encontraron observaciones sobre los datos."]
    cols_texto = [c for c, s in zip(df.columns, df.dtypes) if s == pl.Utf8]
    cols_con_espacios = []
    for c in cols_texto:
        serie = df[c].cast(pl.Utf8, strict=False)
        if serie.drop_nans().drop_nulls().map_elements(lambda x: bool(re.match(r'^\\s|.*\\s$', x)) if isinstance(x, str) else False, return_dtype=pl.Boolean).any():
            cols_con_espacios.append(c)
    if cols_con_espacios:
        if len(cols_con_espacios) == 1:
            obs.append(f"La columna {cols_con_espacios[0]} tiene valores con espacios al inicio o final")
        else:
            obs.append("Las columnas " + ", ".join(cols_con_espacios) + " tienen valores con espacios al inicio o final")
    cols_fecha = [c for c in df.columns if "fecha" in (c or "").lower()]
    cols_fecha_invalidas = []
    for c in cols_fecha:
        serie = df[c].cast(pl.Utf8, strict=False)
        vals = [v for v in serie.to_list() if isinstance(v, str) and v.strip() != ""]
        if not vals:
            continue
        if any(not detect_iso8601_strict(v.strip()) for v in vals):
            cols_fecha_invalidas.append(c)
    if cols_fecha_invalidas:
        if len(cols_fecha_invalidas) == 1:
            obs.append(f"La columna {cols_fecha_invalidas[0]} tiene valores con formato de fecha inválido. Debe ser ISO 8601 estricto: YYYY-MM-DDTHH:MM:SS")
        else:
            obs.append("Las columnas: " + ", ".join(cols_fecha_invalidas) + " tienen valores con formato de fecha inválido. Debe ser ISO 8601 estricto: YYYY-MM-DDTHH:MM:SS")
    cols_numericas = [c for c, s in zip(df.columns, df.dtypes) if s in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.Float32, pl.Float64)]
    cols_num_malas = []
    for c in cols_numericas:
        serie_txt = df[c].cast(pl.Utf8, strict=False)
        if serie_txt.drop_nulls().drop_nans().map_elements(lambda x: bool(re.search(r"[,\\$\\s]", x)) if isinstance(x, str) else False, return_dtype=pl.Boolean).any():
            cols_num_malas.append(c)
    if cols_num_malas:
        if len(cols_num_malas) == 1:
            obs.append(f"La columna numérica {cols_num_malas[0]} contiene símbolos o separadores no permitidos.")
        else:
            obs.append("Las columnas numéricas " + ", ".join(cols_num_malas) + " contienen símbolos o separadores no permitidos.")
    cols_caracter = [c for c, s in zip(df.columns, df.dtypes) if s == pl.Utf8]
    inconsistencias = []
    for c in cols_caracter:
        vals = [v for v in df[c].drop_nulls().unique().to_list() if isinstance(v, str)]
        if not vals:
            continue
        lower_set = set(v.lower() for v in vals)
        if len(lower_set) < len(vals):
            dups = []
            seen = set()
            for v in vals:
                lv = v.lower()
                if lv in seen:
                    dups.append(v)
                else:
                    seen.add(lv)
            if dups:
                inconsistencias.append(f"{c}. En las categorias: " + ", ".join(sorted(set(dups))))
    if inconsistencias:
        if len(inconsistencias) == 1:
            obs.append("La columna: " + inconsistencias[0] + ", tiene variantes de mayúsculas/minúsculas o acentos.")
        else:
            obs.append("Las columnas: " + ", ".join(inconsistencias) + ", tienen variantes de mayúsculas/minúsculas o acentos.")
    if not obs:
        return ["No se encontraron observaciones sobre los datos."]
    return obs

# ---------------- PDF (no editable, acuse) ----------------
def construir_pdf(final_dict: dict, nombre_archivo: str, token: str, no_oficio: str) -> bytes:
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    top_margin = 3.5*cm
    bottom_margin = 3.0*cm
    left_margin = 2.2*cm
    right_margin = 2.2*cm

    # Encabezado
    sup_img = os.path.join(LOGOS_FOLDER, "superior.png")
    header_h = 3.0*cm
    if os.path.exists(sup_img):
        img_w = width - 3*cm
        c.drawImage(sup_img, (width - img_w)/2, height - header_h - 1.0*cm, width=img_w, height=header_h, preserveAspectRatio=True, mask='auto')

    c.setFont("Helvetica-Bold", 10)
    c.setFillColor(colors.HexColor("#333333"))
    c.drawRightString(width - right_margin, height - header_h - 1.2*cm, f"No. de Oficio: {no_oficio}")

    c.setFont("Helvetica", 11)
    text = [
        "Unidad de Innovación de la Gestión Pública",
        "Dirección General de Datos y Transparencia Proactiva",
        f"Ciudad de México a {datetime.now().strftime('%d de %B de %Y')}"
    ]
    y = height - header_h - 2.2*cm
    for line in text:
        c.drawCentredString(width/2, y, line); y -= 0.45*cm

    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(width/2, y-0.2*cm, "ATENTA NOTA")
    y -= 1.2*cm

    c.setFont("Helvetica-Oblique", 10.5)
    c.drawCentredString(width/2, y, "El siguiente documento se genera automáticamente con el sistema API-Validador-Formatos-Datos-Abiertos.")
    y -= 0.9*cm

    c.setFont("Helvetica", 10.5)
    c.drawString(left_margin, y, f"Nombre del Archivo: {nombre_archivo}")
    y -= 0.5*cm
    c.drawString(left_margin, y, f"Fecha Validación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    y -= 0.8*cm

    def draw_obs_block(title, obs_list, y):
        c.setFont("Helvetica-Bold", 13)
        c.drawString(left_margin, y, title); y -= 0.5*cm
        if not obs_list:
            c.setFont("Helvetica", 10.5); c.drawString(left_margin, y, "Sin observaciones."); y -= 0.4*cm
            return y
        num = 1
        for o in obs_list:
            c.setFont("Helvetica-Bold", 10.5); c.drawString(left_margin, y, f"Observación {num}"); y -= 0.4*cm
            c.setFont("Helvetica", 10.5)
            import textwrap
            wrapped = textwrap.wrap(o, width=110)
            for line in wrapped:
                c.drawString(left_margin, y, line); y -= 0.38*cm
            y -= 0.2*cm
            num += 1
        return y

    bloques = [
        ("Observaciones de Formato", final_dict.get("formato", [])),
        ("Observaciones del Nombre del Archivo", final_dict.get("archivo", [])),
        ("Observaciones de Nombres de Columnas", final_dict.get("columnas", [])),
        ("Observaciones de Filas/Datos", final_dict.get("datos", [])),
    ]

    for titulo, obs in bloques:
        y = draw_obs_block(titulo, obs, y)
        y -= 0.3*cm
        if y < bottom_margin + 5*cm:
            break

    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(width/2, bottom_margin + 3.0*cm, "Atentamente")
    c.drawCentredString(width/2, bottom_margin + 2.3*cm, "Mtro. Lamik Kasis Petraki")
    c.drawCentredString(width/2, bottom_margin + 1.7*cm, "Director de Innovación y Análisis de Datos")

    line_y = bottom_margin + 1.2*cm
    c.setStrokeColor(colors.HexColor("#7B1733"))
    c.setLineWidth(2)
    c.line(left_margin, line_y, width - right_margin, line_y)

    inf_img = os.path.join(LOGOS_FOLDER, "inferior.png")
    img_h = 2.0*cm
    if os.path.exists(inf_img):
        c.drawImage(inf_img, left_margin, 0.5*cm, width=6.5*cm, height=img_h, preserveAspectRatio=True, mask='auto')

    c.setFont("Helvetica", 8)
    c.setFillColor(colors.HexColor("#333333"))
    right_text_x = (width - right_margin)
    c.drawRightString(right_text_x, line_y + 0.3*cm, "Av. Insurgentes Sur, No. 3211, 1° piso, Col. Insurgentes Cuicuilco,")
    c.drawRightString(right_text_x, line_y - 0.05*cm, "alcaldía Coyoacán, código postal 04530, Ciudad de México.")

    c.showPage(); c.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

# ---------------- Rutas ----------------
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/validar", methods=["POST"])
def validar():
    if "archivo" not in request.files:
        return render_template("index.html", error="No se adjuntó archivo.")
    file = request.files["archivo"]
    if file.filename == "":
        return render_template("index.html", error="No se seleccionó archivo.")
    if not allowed_file(file.filename):
        return render_template("index.html", error="Formato no permitido. Use CSV/XLSX.")

    filename = secure_filename(file.filename)
    ext = filename.rsplit(".", 1)[1].lower()
    file.stream.seek(0)
    contenido = io.BytesIO(file.read()); contenido.seek(0)

    formato_obs, df = validar_formato_y_carga(io.BytesIO(contenido.getvalue()), filename, ext)
    nombre_sin_ext = os.path.splitext(filename)[0]
    archivo_obs = validar_nombre_archivo(nombre_sin_ext)
    columnas_obs = validar_nombres_columnas(df) if df.shape[1] > 0 else ["No se encontraron observaciones de los nombres de las columnas"]
    datos_obs = validar_datos(df)

    FINAL = {
        "formato": formato_obs or ["No se encontraron observaciones de formato."],
        "archivo": archivo_obs,
        "columnas": columnas_obs,
        "datos": datos_obs
    }

    def es_ok(lista, texto_ok): return len(lista) == 1 and texto_ok in lista[0]
    pasa = (
        es_ok(FINAL["formato"], "No se encontraron observaciones de formato") and
        es_ok(FINAL["archivo"], "No se encontraron observaciones con el nombre del archivo") and
        es_ok(FINAL["columnas"], "No se encontraron observaciones de los nombres de las columnas") and
        es_ok(FINAL["datos"], "No se encontraron observaciones sobre los datos")
    )

    token = datetime.now().strftime("%Y%m%d%H%M%S%f")

    # Guardar FINAL
    final_json_path = os.path.join(RESULTS_FOLDER, f"final_{token}.json")
    with open(final_json_path, "w", encoding="utf-8") as fj:
        json.dump(FINAL, fj, ensure_ascii=False)

    # Guardar número de oficio persistente para este token
    no_oficio = generar_no_oficio()
    with open(os.path.join(RESULTS_FOLDER, f"no_oficio_{token}.txt"), "w", encoding="utf-8") as fno:
        fno.write(no_oficio)

    return render_template("resultados.html",
                           token=token,
                           pasa=pasa,
                           FINAL=FINAL,
                           nombre_archivo=filename)

@app.route("/descargar/pdf/<token>")
def descargar_pdf(token):
    final_json_path = os.path.join(RESULTS_FOLDER, f"final_{token}.json")
    if not os.path.exists(final_json_path):
        return "No existe el recurso", 404
    with open(final_json_path, "r", encoding="utf-8") as fj:
        FINAL = json.load(fj)

    no_oficio_path = os.path.join(RESULTS_FOLDER, f"no_oficio_{token}.txt")
    try:
        with open(no_oficio_path, "r", encoding="utf-8") as fno:
            no_oficio = fno.read().strip()
    except Exception:
        no_oficio = generar_no_oficio()

    nombre_archivo = request.args.get("nombre", "archivo_validado")
    pdf_bytes = construir_pdf(FINAL, nombre_archivo, token, no_oficio)

    pdf_path = os.path.join(RESULTS_FOLDER, f"informe_{token}.pdf")
    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)

    return send_file(io.BytesIO(pdf_bytes), as_attachment=True, download_name=f"informe_{token}.pdf", mimetype="application/pdf")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
