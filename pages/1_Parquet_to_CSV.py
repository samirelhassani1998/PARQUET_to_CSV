"""
Parquet → CSV Conversion Page

This page provides the UI for converting Parquet files to CSV format.
"""

import io
import sys
from pathlib import Path

import streamlit as st

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.parquet_to_csv import (
    convert_parquet_filelike_to_csv_bytes,
    convert_multiple_to_zip_bytes,
    get_parquet_preview,
)

# Page configuration
st.set_page_config(
    page_title="Parquet → CSV",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🔄 Parquet → CSV")
st.markdown("Convertissez vos fichiers Parquet en CSV avec des options personnalisées.")

# Sidebar - CSV Options
st.sidebar.header("⚙️ Options CSV")

separator = st.sidebar.selectbox(
    "Séparateur",
    options=[",", ";", "\t", "|"],
    format_func=lambda x: {"," : "Virgule (,)", ";" : "Point-virgule (;)", "\t" : "Tabulation", "|" : "Pipe (|)"}[x],
    index=0
)

encoding = st.sidebar.selectbox(
    "Encodage",
    options=["utf-8", "latin-1", "cp1252"],
    index=0
)

include_header = st.sidebar.checkbox("Inclure les en-têtes", value=True)

convert_complex = st.sidebar.checkbox(
    "Convertir types complexes en JSON",
    value=True,
    help="Les types comme liste, struct, map seront convertis en chaînes JSON"
)

st.sidebar.markdown("---")
st.sidebar.markdown("""
### 📖 Aide

**Séparateur** : Caractère utilisé pour séparer les colonnes

**Encodage** : 
- `utf-8` : Standard universel (recommandé)
- `latin-1` : Europe occidentale  
- `cp1252` : Windows

**Types complexes** : Les colonnes contenant des listes ou 
objets imbriqués seront sérialisées en JSON.
""")

# Main content - File Upload
st.header("📁 Upload de fichiers")

uploaded_files = st.file_uploader(
    "Glissez-déposez vos fichiers Parquet ici",
    type=["parquet"],
    accept_multiple_files=True,
    help="Vous pouvez sélectionner plusieurs fichiers à la fois"
)

if uploaded_files:
    st.success(f"✅ {len(uploaded_files)} fichier(s) uploadé(s)")
    
    # Display file info and previews
    st.header("📋 Aperçu des fichiers")
    
    file_data = []
    preview_errors = []
    
    for uploaded_file in uploaded_files:
        with st.expander(f"📄 {uploaded_file.name} ({uploaded_file.size / 1024:.1f} KB)", expanded=len(uploaded_files) == 1):
            try:
                # Reset file position for reading
                uploaded_file.seek(0)
                
                # Get preview
                preview_table, metadata = get_parquet_preview(uploaded_file, num_rows=50)
                
                # Display metadata
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Lignes totales", f"{metadata['num_rows']:,}")
                with col2:
                    st.metric("Colonnes", metadata['num_columns'])
                with col3:
                    st.metric("Row Groups", metadata['num_row_groups'])
                
                # Display schema
                with st.expander("📐 Schéma"):
                    schema_data = [{"Colonne": f["name"], "Type": f["type"]} for f in metadata['schema']]
                    st.table(schema_data)
                
                # Display preview
                st.subheader("Aperçu (50 premières lignes)")
                st.dataframe(preview_table.to_pandas(), use_container_width=True)
                
                # Store file data for conversion
                uploaded_file.seek(0)
                file_data.append((uploaded_file.name, uploaded_file))
                
            except Exception as e:
                st.error(f"❌ Erreur lors de la lecture : {e}")
                preview_errors.append(uploaded_file.name)
    
    # Conversion section
    if file_data:
        st.header("🚀 Conversion")
        
        # Filter out files with errors
        valid_files = [(name, f) for name, f in file_data if name not in preview_errors]
        
        if len(valid_files) > 0:
            if st.button("🔄 Convertir en CSV", type="primary", use_container_width=True):
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    if len(valid_files) == 1:
                        # Single file conversion
                        filename, file_obj = valid_files[0]
                        file_obj.seek(0)
                        
                        status_text.text(f"Conversion de {filename}...")
                        
                        def update_progress(current, total):
                            progress = min(current / total, 1.0)
                            progress_bar.progress(progress)
                        
                        csv_bytes = convert_parquet_filelike_to_csv_bytes(
                            file_obj,
                            separator=separator,
                            encoding=encoding,
                            include_header=include_header,
                            convert_complex_to_json=convert_complex,
                            progress_callback=update_progress
                        )
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ Conversion terminée !")
                        
                        # Generate output filename
                        csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                        
                        st.download_button(
                            label=f"⬇️ Télécharger {csv_filename}",
                            data=csv_bytes,
                            file_name=csv_filename,
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        st.info(f"📊 Taille du CSV : {len(csv_bytes) / 1024:.1f} KB")
                        
                    else:
                        # Multiple files - create ZIP
                        status_text.text(f"Préparation de {len(valid_files)} fichiers...")
                        
                        # Reset all file positions
                        files_for_conversion = []
                        for name, f in valid_files:
                            f.seek(0)
                            # Read into BytesIO to avoid issues with file pointers
                            content = f.read()
                            files_for_conversion.append((name, io.BytesIO(content)))
                        
                        def update_zip_progress(current, total, filename):
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(f"Conversion : {filename} ({current + 1}/{total})")
                        
                        zip_bytes = convert_multiple_to_zip_bytes(
                            files_for_conversion,
                            separator=separator,
                            encoding=encoding,
                            include_header=include_header,
                            convert_complex_to_json=convert_complex,
                            progress_callback=update_zip_progress
                        )
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ Archive ZIP créée !")
                        
                        st.download_button(
                            label="⬇️ Télécharger l'archive ZIP",
                            data=zip_bytes,
                            file_name="parquet_to_csv.zip",
                            mime="application/zip",
                            use_container_width=True
                        )
                        
                        st.info(f"📦 Taille de l'archive : {len(zip_bytes) / 1024:.1f} KB ({len(valid_files)} fichiers)")
                
                except Exception as e:
                    progress_bar.progress(0)
                    st.error(f"❌ Erreur lors de la conversion : {e}")
                    st.markdown("""
                    **Solutions possibles :**
                    - Vérifiez que les fichiers sont des fichiers Parquet valides
                    - Activez l'option "Convertir types complexes en JSON"
                    - Essayez avec des fichiers plus petits
                    """)
        else:
            st.warning("⚠️ Aucun fichier valide à convertir")
else:
    # Empty state
    st.info("👆 Uploadez un ou plusieurs fichiers Parquet pour commencer")
    
    # Example usage
    with st.expander("💡 Exemple d'utilisation"):
        st.markdown("""
        1. Cliquez sur "Browse files" ou glissez-déposez vos fichiers
        2. Vérifiez l'aperçu de vos données
        3. Ajustez les options CSV dans la barre latérale si nécessaire
        4. Cliquez sur "Convertir en CSV"
        5. Téléchargez le résultat (CSV ou ZIP)
        """)
