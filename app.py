# -*- coding: utf-8 -*-
"""
Sistema: API-Validador-Formatos-Datos-Abiertos
Autor: Mtro. Francisco Daniel Martínez Martínez
Versión: v8.9.1 (PDF para archivos válidos sin errores - CORREGIDO)
"""
import io, os, re, json, csv
from datetime import datetime, timedelta
from flask import Flask, request, render_template, send_file, after_this_request
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
LOGS_FOLDER = "logs"
REPORTS_FOLDER = "reportes"
ALLOWED_EXTENSIONS = {"csv", "xls", "xlsx"}

# Crear directorios necesarios
for folder in [UPLOAD_FOLDER, RESULTS_FOLDER, LOGOS_FOLDER, LOGS_FOLDER, REPORTS_FOLDER]:
    os.makedirs(folder, exist_ok=True)

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024  # 256MB

# ---------------- MANEJO DE LOGS Y REPORTES ----------------
def get_current_log_file():
    """Obtiene el archivo de log semanal actual"""
    current_date = datetime.now()
    log_filename = f"validaciones_{current_date.year}_{current_date.month:02d}.log"
    return os.path.join(LOGS_FOLDER, log_filename)

def get_current_report_file():
    """Obtiene el archivo de reporte semanal actual"""
    current_date = datetime.now()
    week_number = current_date.isocalendar()[1]
    report_filename = f"reporte_{current_date.year}_{week_number:02d}.csv"
    return os.path.join(REPORTS_FOLDER, report_filename)

def write_to_log(ip_address, filename, file_size_kb, processing_time, status):
    """Escribe una entrada en el log semanal"""
    log_file = get_current_log_file()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    log_entry = f"{timestamp} | IP={ip_address} | Archivo={filename} | Peso={file_size_kb} KB | Tiempo de Procesamiento = {processing_time}s | Estado={status}\n"
    
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)

def update_weekly_report(ip_address, filename, file_size_kb, processing_time, status, observations_count):
    """Actualiza el reporte semanal en CSV"""
    report_file = get_current_report_file()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Verificar si el archivo existe para escribir los headers
    file_exists = os.path.isfile(report_file)
    
    with open(report_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Escribir headers si el archivo no existe
        if not file_exists:
            writer.writerow([
                "Fecha", "Hora", "IP", "Archivo", "Peso_KB", 
                "Tiempo_Procesamiento_s", "Estado", "Cantidad_Observaciones"
            ])
        
        date_part = timestamp.split(" ")[0]
        time_part = timestamp.split(" ")[1]
        
        writer.writerow([
            date_part, time_part, ip_address, filename, file_size_kb,
            processing_time, status, observations_count
        ])

def count_total_observations(final_dict):
    """Cuenta el total de observaciones en todas las categorías"""
    total = 0
    for category, observations in final_dict.items():
        # No contar los mensajes "No se encontraron observaciones"
        valid_observations = [obs for obs in observations if not obs.startswith("No se encontraron")]
        total += len(valid_observations)
    return total

def cleanup_temp_files(token):
    """Elimina los archivos temporales (JSON y PDF) después de la descarga"""
    try:
        json_file = os.path.join(RESULTS_FOLDER, f"final_{token}.json")
        pdf_file = os.path.join(RESULTS_FOLDER, f"informe_{token}.pdf")
        
        if os.path.exists(json_file):
            os.remove(json_file)
        if os.path.exists(pdf_file):
            os.remove(pdf_file)
            
        print(f"Archivos temporales eliminados para token: {token}")
    except Exception as e:
        print(f"Error al eliminar archivos temporales: {e}")

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

# ---------------- VALIDADORES CORREGIDOS ----------------
def validar_formato_y_carga(file_storage, filename, ext):
    obs = []
    if ext in ("xls", "xlsx"):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(file_storage, read_only=True, data_only=True)
            ws = wb[wb.sheetnames[0]]
            rows = list(ws.values)
            
            if not rows:
                obs.append("El archivo Excel está vacío.")
                return obs, pl.DataFrame()
                
            headers = [str(h) if h is not None else "" for h in rows[0]]
            data = rows[1:] if len(rows) > 1 else []
            
            # CONVERSIÓN SEGURA A STRING - SOLUCIÓN AL ERROR
            safe_data = []
            for row in data:
                safe_row = []
                for cell in row:
                    if cell is None:
                        safe_row.append("")
                    elif isinstance(cell, datetime):
                        safe_row.append(cell.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        safe_row.append(str(cell))
                safe_data.append(safe_row)
            
            # Crear DataFrame con orientación explícita y manejo de tipos
            if headers:
                df = pl.DataFrame(safe_data, schema=headers, orient="row")
            else:
                df = pl.DataFrame()
                
            empty_headers = [h for h in df.columns if (h is None or str(h).strip() in ("", " "))]
            if empty_headers:
                obs.append(f"Se encuentran {len(empty_headers)} variables sin nombre. Revisar el contenido de estas variables.")
            
            return obs, df
            
        except Exception as e:
            obs.append(f"Error al procesar archivo Excel: {str(e)}")
            return obs, pl.DataFrame()

    # CSV - MANTENER LA LÓGICA ORIGINAL
    file_bytes = file_storage.read()
    if not is_utf8(file_bytes):
        obs.append("La codificación no es la correcta, debe ser 'UTF-8'.")
    stream = io.BytesIO(file_bytes)
    try:
        df = pl.read_csv(stream, infer_schema_length=10000, ignore_errors=True)
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
    
    cols_texto = df.columns
    for c in cols_texto:
        try:
            serie_str = df[c].cast(pl.Utf8, strict=False)
            has_spaces = serie_str.drop_nulls().map_elements(
                lambda x: bool(re.match(r'^\s|\s$', str(x))) if x is not None else False,
                return_dtype=pl.Boolean
            ).any()
            
            if has_spaces:
                obs.append(f"La columna {c} tiene valores con espacios al inicio o final.")
        except Exception:
            continue
            
    return obs or ["No se encontraron observaciones sobre los datos."]

# ---------------- PDF ACTUALIZADO PARA ARCHIVOS VÁLIDOS ----------------
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
        if os.path.exists(header_left):
            c.drawImage(header_left, left_margin, height - 3.0 * cm,
                        width=7.0 * cm, height=2.0 * cm, preserveAspectRatio=True, mask="auto")
        if os.path.exists(header_right):
            c.drawImage(header_right, width - 7.5 * cm, height - 3.0 * cm,
                        width=7.0 * cm, height=2.0 * cm, preserveAspectRatio=True, mask="auto")
        if os.path.exists(footer_img):
            c.drawImage(footer_img, 0, 0.5 * cm,
                        width=width, height=2.5 * cm, preserveAspectRatio=True, mask="auto")

    def nueva_pagina():
        c.showPage()
        draw_header_footer()
        c.setFont("Helvetica", 10.5)
        return height - top_margin

    draw_header_footer()
    c.setFont("Helvetica", 10.5)
    
    y = height - top_margin

    encabezado = [
        "Unidad de Innovación de la Gestión Pública",
        "Dirección General de Datos y Transparencia Proactiva",
        f"Ciudad de México, a {datetime.now().strftime('%d de %B de %Y')}"
    ]
    
    for line in encabezado:
        text_width = c.stringWidth(line, "Helvetica", 10.5)
        x_position = width - right_margin - text_width
        c.drawString(x_position, y, line)
        y -= 0.6 * cm

    y -= 0.4 * cm
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(width / 2, y, "ATENTA NOTA")
    y -= 0.8 * cm

    c.setFont("Helvetica", 10)
    descripcion = "El siguiente documento se genera automáticamente con el sistema API-Validador-Formatos-Datos-Abiertos."
    c.drawCentredString(width / 2, y, descripcion)
    y -= 0.8 * cm

    c.setFont("Helvetica", 10.5)
    c.drawString(left_margin, y, f"Nombre del Archivo: {nombre_archivo}")
    y -= 0.5 * cm
    c.drawString(left_margin, y, f"Fecha de Validación: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
    y -= 2.5 * cm

    def draw_wrapped_text(text, x, y, max_width, font_name="Helvetica", font_size=10.5):
        if not text:
            return y
            
        words = text.split(' ')
        lines = []
        current_line = []
        
        for word in words:
            test_line = ' '.join(current_line + [word]) if current_line else word
            test_width = c.stringWidth(test_line, font_name, font_size)
            
            if test_width <= max_width:
                current_line.append(word)
            else:
                if current_line:
                    lines.append(' '.join(current_line))
                if c.stringWidth(word, font_name, font_size) > max_width:
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
            if y < bottom_margin + 3 * cm:
                y = nueva_pagina()
                
            c.setFont("Helvetica-Bold", 10.5)
            obs_title = f"Observación {i}:"
            c.drawString(left_margin, y, obs_title)
            y -= 0.5 * cm
            
            c.setFont("Helvetica", 10.5)
            y = draw_wrapped_text(obs, left_margin, y, available_width, "Helvetica", 10.5)
            y -= 0.3 * cm
            
        return y

    # VERIFICAR SI NO HAY ERRORES - CORREGIDO
    def tiene_errores_verdadero():
        formato = final_dict.get("formato", [])
        if len(formato) > 1 or (len(formato) == 1 and not formato[0].startswith("No se encontraron")):
            return True
        
        archivo = final_dict.get("archivo", [])
        if len(archivo) > 1 or (len(archivo) == 1 and not archivo[0].startswith("No se encontraron")):
            return True
        
        columnas = final_dict.get("columnas", [])
        if len(columnas) > 1 or (len(columnas) == 1 and not columnas[0].startswith("No se encontraron")):
            return True
        
        datos = final_dict.get("datos", [])
        if len(datos) > 1 or (len(datos) == 1 and not datos[0].startswith("No se encontraron")):
            return True
        
        return False

    tiene_errores = tiene_errores_verdadero()

    if not tiene_errores:
        c.setFont("Helvetica-Bold", 16)
        mensaje_exito = "✓ VALIDACIÓN EXITOSA"
        text_width = c.stringWidth(mensaje_exito, "Helvetica-Bold", 16)
        x_position = (width - text_width) / 2
        c.drawString(x_position, y, mensaje_exito)
        y -= 1.2 * cm

        c.setFont("Helvetica", 12)
        mensaje_descripcion = "El documento no contiene errores."
        text_width_desc = c.stringWidth(mensaje_descripcion, "Helvetica", 12)
        x_position_desc = (width - text_width_desc) / 2
        c.drawString(x_position_desc, y, mensaje_descripcion)
        y -= 2.5 * cm

        c.setFont("Helvetica-Bold", 12)
        c.drawString(left_margin, y, "Resumen de Validaciones:")
        y -= 0.7 * cm

        validaciones = [
            "✓ Formato del archivo: Correcto",
            "✓ Nombre del archivo: Correcto", 
            "✓ Nombres de columnas: Correctos",
            "✓ Datos y filas: Correctos"
        ]

        c.setFont("Helvetica", 10.5)
        for validacion in validaciones:
            if y < bottom_margin + 2 * cm:
                y = nueva_pagina()
            c.drawString(left_margin + 0.5 * cm, y, validacion)
            y -= 0.5 * cm

    else:
        bloques = [
            ("Observaciones de Formato", final_dict.get("formato", [])),
            ("Observaciones del Nombre del Archivo", final_dict.get("archivo", [])),
            ("Observaciones de Nombres de Columnas", final_dict.get("columnas", [])),
            ("Observaciones de Filas/Datos", final_dict.get("datos", []))
        ]
        
        for titulo, lista in bloques:
            y = draw_block(titulo, lista, y)
            y -= 0.3 * cm

    if y < bottom_margin + 3 * cm:
        y = nueva_pagina()

    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(width / 2, bottom_margin + 3.0 * cm, "Atentamente")
    c.drawCentredString(width / 2, bottom_margin + 2.3 * cm, "Datos Abiertos")
    c.drawCentredString(width / 2, bottom_margin + 1.6 * cm, "Dirección de Innovación y Análisis de Datos")

    c.save()
    pdf_bytes = pdf_buffer.getvalue()
    pdf_buffer.close()
    
    # Guardar PDF temporalmente (se eliminará después de la descarga)
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
    start_time = datetime.now()
    
    if "archivo" not in request.files:
        return render_template("index.html", error="No se adjuntó archivo.")
    
    file = request.files["archivo"]
    if file.filename == "":
        return render_template("index.html", error="No se seleccionó archivo.")
    if not allowed_file(file.filename):
        return render_template("index.html", error="Formato no permitido. Use CSV/XLSX.")

    filename = secure_filename(file.filename)
    ext = filename.rsplit(".", 1)[1].lower()
    
    # Obtener IP del cliente
    ip_address = request.remote_addr
    
    # Leer archivo para calcular tamaño
    file_content = file.read()
    file_size_kb = round(len(file_content) / 1024, 2)
    contenido = io.BytesIO(file_content)
    contenido.seek(0)

    try:
        formato_obs, df = validar_formato_y_carga(contenido, filename, ext)
        archivo_obs = validar_nombre_archivo(os.path.splitext(filename)[0])
        columnas_obs = validar_nombres_columnas(df)
        datos_obs = validar_datos(df)

        FINAL = {"formato": formato_obs, "archivo": archivo_obs, "columnas": columnas_obs, "datos": datos_obs}
        token = datetime.now().strftime("%Y%m%d%H%M%S%f")
        
        # Guardar JSON temporal (se eliminará después de la descarga)
        with open(os.path.join(RESULTS_FOLDER, f"final_{token}.json"), "w", encoding="utf-8") as f:
            json.dump(FINAL, f, ensure_ascii=False)

        # DETERMINAR SI PASA LA VALIDACIÓN
        def pasa_validacion():
            formato = FINAL.get("formato", [])
            if len(formato) > 1 or (len(formato) == 1 and not formato[0].startswith("No se encontraron")):
                return False
            
            archivo = FINAL.get("archivo", [])
            if len(archivo) > 1 or (len(archivo) == 1 and not archivo[0].startswith("No se encontraron")):
                return False
            
            columnas = FINAL.get("columnas", [])
            if len(columnas) > 1 or (len(columnas) == 1 and not columnas[0].startswith("No se encontraron")):
                return False
            
            datos = FINAL.get("datos", [])
            if len(datos) > 1 or (len(datos) == 1 and not datos[0].startswith("No se encontraron")):
                return False
            
            return True

        pasa = pasa_validacion()
        processing_time = (datetime.now() - start_time).total_seconds()
        status = "VÁLIDO" if pasa else "NO VÁLIDO"
        observations_count = count_total_observations(FINAL)

        # REGISTRAR EN LOGS Y REPORTES (esto permanece)
        write_to_log(ip_address, filename, file_size_kb, processing_time, status)
        update_weekly_report(ip_address, filename, file_size_kb, processing_time, status, observations_count)

        return render_template("resultados.html", token=token, FINAL=FINAL, nombre_archivo=filename, pasa=pasa)
    
    except Exception as e:
        # Registrar error en logs
        processing_time = (datetime.now() - start_time).total_seconds()
        write_to_log(request.remote_addr, filename, file_size_kb, processing_time, "ERROR")
        update_weekly_report(request.remote_addr, filename, file_size_kb, processing_time, "ERROR", 0)
        
        return render_template("index.html", error=f"Error al procesar el archivo: {str(e)}")

@app.route("/descargar/pdf/<token>")
def descargar_pdf(token):
    path_json = os.path.join(RESULTS_FOLDER, f"final_{token}.json")
    if not os.path.exists(path_json):
        return "No existe el recurso", 404
    with open(path_json, "r", encoding="utf-8") as f:
        FINAL = json.load(f)

    nombre_archivo = request.args.get("nombre", "archivo_validado")
    pdf_bytes = construir_pdf(FINAL, nombre_archivo, token)
    
    # Configurar limpieza después de la respuesta
    @after_this_request
    def remove_files(response):
        try:
            cleanup_temp_files(token)
        except Exception as e:
            print(f"Error en limpieza automática: {e}")
        return response
    
    return send_file(
        io.BytesIO(pdf_bytes),
        as_attachment=True,
        download_name=f"informe_{token}.pdf",
        mimetype="application/pdf"
    )

# Limpieza programada de archivos temporales viejos (por si hay descargas fallidas)
def cleanup_old_temp_files(hours_old=24):
    """Elimina archivos temporales más viejos que el tiempo especificado"""
    try:
        current_time = datetime.now()
        for filename in os.listdir(RESULTS_FOLDER):
            if filename.startswith(("final_", "informe_")):
                file_path = os.path.join(RESULTS_FOLDER, filename)
                file_time = datetime.fromtimestamp(os.path.getctime(file_path))
                if (current_time - file_time).total_seconds() > (hours_old * 3600):
                    os.remove(file_path)
                    print(f"Archivo temporal antiguo eliminado: {filename}")
    except Exception as e:
        print(f"Error en limpieza de archivos antiguos: {e}")

if __name__ == "__main__":
    # Ejecutar limpieza al iniciar la aplicación
    cleanup_old_temp_files()
    app.run(host="127.0.0.1", port=8081, debug=True)