# -*- coding: utf-8 -*-
"""
Sistema: API-Validador-Formatos-Datos-Abiertos
Autor: Mtro. Francisco Daniel Martínez Martínez
Versión: v8.7 (texto derecho y logo derecho ajustado)
"""
import io, os, re, json
from datetime import datetime
from flask import Flask, request, render_template, send_file
from werkzeug.utils import secure_filename
import polars as pl
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
from reportlab.lib import colors

# ---------------- CONFIG ----------------
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "resultados"
LOGOS_FOLDER = "logos"
ALLOWED_EXTENSIONS = {"csv", "xls", "xlsx"}

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 256MB

# ---------------- UTILIDADES ----------------
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def is_utf8(file_bytes: bytes) -> bool:
    try:
        file_bytes.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False

def split_words_underscore(name: str) -> int:
    return len([p for p in name.split("_") if p])

# ---------------- VALIDADORES ----------------
def validar_formato_y_carga(file_storage, filename, ext):
    obs = []
    if ext in ("xls", "xlsx"):
        import openpyxl
        wb = openpyxl.load_workbook(file_storage, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        rows = list(ws.values)
        headers = [str(h) if h else "" for h in rows[0]] if rows else []
        data = rows[1:] if len(rows) > 1 else []
        df = pl.DataFrame(data, schema=headers) if headers else pl.DataFrame()
        empty_headers = [h for h in df.columns if (h is None or str(h).strip() in ("", " "))]
        if empty_headers:
            obs.append(f"Se encuentran {len(empty_headers)} variables sin nombre. Revisar el contenido de estas variables.")
        return obs, df

    # CSV
    file_bytes = file_storage.read()
    if not is_utf8(file_bytes):
        obs.append("La codificación no es la correcta, debe ser 'UTF-8'.")
    stream = io.BytesIO(file_bytes)
    try:
        df = pl.read_csv(stream, infer_schema_length=2000, ignore_errors=True)
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
    if " " in nom_arch:
        obs.append("El nombre del archivo no debe tener espacios. Se recomienda usar guiones bajos para separar palabras.")
    return obs or ["No se encontraron observaciones con el nombre del archivo."]

def validar_nombres_columnas(df: pl.DataFrame):
    if df.is_empty():
        return ["No se encontraron observaciones de los nombres de las columnas."]
    obs = []
    cols = df.columns
    especiales = [c for c in cols if re.search(r"[ñáéíóúüÑÁÉÍÓÚÜ]", c or "")]
    if especiales:
        obs.append("Nombre de columnas con caracteres especiales: " + " | ".join(especiales))
    largas = [c for c in cols if split_words_underscore(c or "") > 5]
    if largas:
        obs.append("Nombre de columnas con más de 5 palabras: " + " | ".join(largas))
    return obs or ["No se encontraron observaciones de los nombres de las columnas."]

def validar_datos(df: pl.DataFrame):
    obs = []
    if df.is_empty():
        return ["No se encontraron observaciones sobre los datos."]
    cols_texto = [c for c, s in zip(df.columns, df.dtypes) if s == pl.Utf8]
    for c in cols_texto:
        serie = df[c].cast(pl.Utf8, strict=False)
        if serie.drop_nans().drop_nulls().map_elements(
            lambda x: bool(re.match(r'^\s|.*\s$', x)) if isinstance(x, str) else False,
            return_dtype=pl.Boolean
        ).any():
            obs.append(f"La columna {c} tiene valores con espacios al inicio o final.")
    return obs or ["No se encontraron observaciones sobre los datos."]

# ---------------- PDF CORREGIDO ----------------
def construir_pdf(final_dict: dict, nombre_archivo: str, token: str) -> bytes:
    pdf_buffer = io.BytesIO()
    width, height = letter
    c = canvas.Canvas(pdf_buffer, pagesize=letter)

    left_margin = 2.2 * cm
    right_margin = 2.2 * cm
    top_margin = 4.5 * cm
    bottom_margin = 3.5 * cm
    line_height = 0.5 * cm
    available_width = width - left_margin - right_margin

    header_left = os.path.join(LOGOS_FOLDER, "superiorizquierdo.png")
    header_right = os.path.join(LOGOS_FOLDER, "superiorderecho.png")
    footer_img = os.path.join(LOGOS_FOLDER, "inferior.png")

    def draw_header_footer():
        # Logos superiores
        if os.path.exists(header_left):
            c.drawImage(header_left, left_margin, height - 3.0 * cm,
                        width=7.0 * cm, height=2.0 * cm, preserveAspectRatio=True, mask="auto")
        if os.path.exists(header_right):
            # Logo derecho MÁS PEGADO A LA DERECHA
            c.drawImage(header_right, width - 7.5 * cm, height - 3.0 * cm,  # Reducido de 9.0cm a 7.5cm
                        width=7.0 * cm, height=2.0 * cm, preserveAspectRatio=True, mask="auto")
        # Pie de página
        if os.path.exists(footer_img):
            c.drawImage(footer_img, 0, 0.5 * cm,
                        width=width, height=2.5 * cm, preserveAspectRatio=True, mask="auto")

    def nueva_pagina():
        c.showPage()
        draw_header_footer()
        c.setFont("Helvetica", 10.5)
        return height - top_margin

    # Dibujar cabecera en la primera página
    draw_header_footer()
    c.setFont("Helvetica", 10.5)
    
    # Empezar más abajo para dejar espacio a los logos más grandes
    y = height - top_margin

    # Encabezado de texto institucional - ALINEADO A LA DERECHA
    encabezado = [
        "Unidad de Innovación de la Gestión Pública",
        "Dirección General de Datos y Transparencia Proactiva",
        f"Ciudad de México, a {datetime.now().strftime('%d de %B de %Y')}"
    ]
    
    for line in encabezado:
        # Calcular ancho del texto para alineación derecha
        text_width = c.stringWidth(line, "Helvetica", 10.5)
        x_position = width - right_margin - text_width  # Alineado a la derecha
        c.drawString(x_position, y, line)
        y -= 0.6 * cm

    # Título (centrado)
    y -= 0.4 * cm
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(width / 2, y, "ATENTA NOTA")
    y -= 0.8 * cm

    c.setFont("Helvetica-Oblique", 10)
    descripcion = "El siguiente documento se genera automáticamente con el sistema API-Validador-Formatos-Datos-Abiertos."
    c.drawCentredString(width / 2, y, descripcion)
    y -= 0.8 * cm

    # Información del archivo (alineado a la izquierda)
    c.setFont("Helvetica", 10.5)
    c.drawString(left_margin, y, f"Nombre del Archivo: {nombre_archivo}")
    y -= 0.5 * cm
    c.drawString(left_margin, y, f"Fecha de Validación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    y -= 0.8 * cm

    def draw_wrapped_text(text, x, y, max_width, font_name="Helvetica", font_size=10.5):
        """Función MEJORADA para manejo de textos largos con saltos de línea REALES"""
        if not text:
            return y
            
        words = text.split(' ')
        lines = []
        current_line = []
        
        for word in words:
            test_line = ' '.join(current_line + [word]) if current_line else word
            # Usar stringWidth para medir exactamente el ancho del texto
            test_width = c.stringWidth(test_line, font_name, font_size)
            
            if test_width <= max_width:
                current_line.append(word)
            else:
                # Si la línea actual tiene contenido, guardarla
                if current_line:
                    lines.append(' '.join(current_line))
                # Si una palabra individual es más larga que el ancho máximo, dividirla
                if c.stringWidth(word, font_name, font_size) > max_width:
                    # Dividir palabra larga
                    chars = list(word)
                    temp_word = ""
                    for char in chars:
                        temp_test = temp_word + char
                        if c.stringWidth(temp_test, font_name, font_size) <= max_width:
                            temp_word += char
                        else:
                            if temp_word:
                                lines.append(temp_word)
                            temp_word = char
                    if temp_word:
                        current_line = [temp_word]
                else:
                    current_line = [word]
        
        if current_line:
            lines.append(' '.join(current_line))
        
        # Dibujar las líneas
        for line in lines:
            if y < bottom_margin + 1.5 * cm:
                y = nueva_pagina()
            c.drawString(x, y, line)
            y -= line_height
            
        return y

    def draw_block(title, obs_list, y):
        c.setFont("Helvetica-Bold", 12)
        c.drawString(left_margin, y, title)
        y -= 0.7 * cm
        
        if not obs_list:
            c.setFont("Helvetica", 10.5)
            c.drawString(left_margin, y, "Sin observaciones.")
            return y - 0.7 * cm
            
        for i, obs in enumerate(obs_list, 1):
            # Verificar si necesitamos nueva página ANTES de dibujar
            if y < bottom_margin + 3 * cm:
                y = nueva_pagina()
                
            c.setFont("Helvetica-Bold", 10.5)
            obs_title = f"Observación {i}:"
            c.drawString(left_margin, y, obs_title)
            y -= 0.5 * cm
            
            c.setFont("Helvetica", 10.5)
            # Usar la función mejorada para wrap de texto
            y = draw_wrapped_text(obs, left_margin, y, available_width, "Helvetica", 10.5)
            y -= 0.3 * cm
            
        return y

    bloques = [
        ("Observaciones de Formato", final_dict.get("formato", [])),
        ("Observaciones del Nombre del Archivo", final_dict.get("archivo", [])),
        ("Observaciones de Nombres de Columnas", final_dict.get("columnas", [])),
        ("Observaciones de Filas/Datos", final_dict.get("datos", []))
    ]
    
    for titulo, lista in bloques:
        y = draw_block(titulo, lista, y)
        y -= 0.3 * cm

    # Firma
    if y < bottom_margin + 3 * cm:
        y = nueva_pagina()

    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(width / 2, bottom_margin + 3.0 * cm, "Atentamente")
    c.drawCentredString(width / 2, bottom_margin + 2.3 * cm, "Datos Abiertos")
    c.drawCentredString(width / 2, bottom_margin + 1.6 * cm, "Dirección de Innovación y Análisis de Datos")

    c.save()
    pdf_bytes = pdf_buffer.getvalue()
    pdf_buffer.close()
    
    # Guardar también en archivo para verificación
    pdf_path = os.path.join(RESULTS_FOLDER, f"informe_{token}.pdf")
    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)
        
    return pdf_bytes

# ---------------- FLASK ----------------
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
    contenido = io.BytesIO(file.read()); contenido.seek(0)

    formato_obs, df = validar_formato_y_carga(io.BytesIO(contenido.getvalue()), filename, ext)
    archivo_obs = validar_nombre_archivo(os.path.splitext(filename)[0])
    columnas_obs = validar_nombres_columnas(df)
    datos_obs = validar_datos(df)

    FINAL = {"formato": formato_obs, "archivo": archivo_obs, "columnas": columnas_obs, "datos": datos_obs}
    token = datetime.now().strftime("%Y%m%d%H%M%S%f")
    with open(os.path.join(RESULTS_FOLDER, f"final_{token}.json"), "w", encoding="utf-8") as f:
        json.dump(FINAL, f, ensure_ascii=False)

    return render_template("resultados.html", token=token, FINAL=FINAL, nombre_archivo=filename)

@app.route("/descargar/pdf/<token>")
def descargar_pdf(token):
    path_json = os.path.join(RESULTS_FOLDER, f"final_{token}.json")
    if not os.path.exists(path_json):
        return "No existe el recurso", 404
    with open(path_json, "r", encoding="utf-8") as f:
        FINAL = json.load(f)

    nombre_archivo = request.args.get("nombre", "archivo_validado")
    pdf_bytes = construir_pdf(FINAL, nombre_archivo, token)
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"informe_{token}.pdf",
        mimetype="application/pdf"
    )

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)