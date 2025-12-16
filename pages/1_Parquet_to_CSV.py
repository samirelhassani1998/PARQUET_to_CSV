"""
Parquet → CSV Conversion Page

This page provides the UI for converting Parquet files to CSV format.
Supports merging multiple files via UNION ALL or JOIN.
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
    get_common_columns,
    merge_parquets_union_to_csv_bytes,
    merge_parquets_join_to_csv_bytes,
)

# Page configuration
st.set_page_config(
    page_title="Parquet → CSV",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Authentication gate - blocks access until authenticated
from app.auth import require_password, show_logout_button
require_password()
show_logout_button()

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
    
    # Merge options (only show when multiple files)
    merge_enabled = False
    merge_mode = "union"
    add_source_column = False
    join_key = None
    join_type = "inner"
    
    if len(uploaded_files) > 1:
        st.sidebar.markdown("---")
        st.sidebar.header("🔗 Fusion")
        
        merge_enabled = st.sidebar.checkbox(
            "Fusionner en un seul output",
            value=False,
            help="Combiner tous les fichiers en un seul CSV"
        )
        
        if merge_enabled:
            merge_mode = st.sidebar.radio(
                "Mode de fusion",
                options=["union", "join"],
                format_func=lambda x: {
                    "union": "A) UNION ALL (empiler lignes)",
                    "join": "B) JOIN sur clé"
                }[x]
            )
            
            if merge_mode == "union":
                add_source_column = st.sidebar.checkbox(
                    "Ajouter colonne 'source_file'",
                    value=False,
                    help="Identifie l'origine de chaque ligne"
                )
            else:
                # Get common columns for JOIN key selection
                # Read files to find common columns
                temp_files = []
                for f in uploaded_files:
                    f.seek(0)
                    content = f.read()
                    temp_files.append((f.name, io.BytesIO(content)))
                    f.seek(0)
                
                common_cols = get_common_columns(temp_files)
                
                if common_cols:
                    join_key = st.sidebar.selectbox(
                        "Colonne clé pour JOIN",
                        options=common_cols,
                        help="Colonne commune pour joindre les fichiers"
                    )
                    join_type = st.sidebar.selectbox(
                        "Type de JOIN",
                        options=["inner", "left", "outer"],
                        format_func=lambda x: {
                            "inner": "INNER (intersection)",
                            "left": "LEFT (garder tout de gauche)",
                            "outer": "OUTER (garder tout)"
                        }[x]
                    )
                else:
                    st.sidebar.warning("⚠️ Aucune colonne commune trouvée pour JOIN")
                    merge_mode = "union"  # Fall back to union
    
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
            # Determine button label based on mode
            if merge_enabled and len(valid_files) > 1:
                if merge_mode == "union":
                    button_label = "🔗 Fusionner (UNION ALL) → CSV"
                else:
                    button_label = "🔗 Joindre (JOIN) → CSV"
            elif len(valid_files) == 1:
                button_label = "🔄 Convertir en CSV"
            else:
                button_label = "🔄 Convertir en ZIP"
            
            if st.button(button_label, type="primary", use_container_width=True):
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Prepare files (read into BytesIO)
                    files_for_conversion = []
                    for name, f in valid_files:
                        f.seek(0)
                        content = f.read()
                        files_for_conversion.append((name, io.BytesIO(content)))
                    
                    # MERGE MODE: UNION ALL
                    if merge_enabled and len(valid_files) > 1 and merge_mode == "union":
                        status_text.text(f"Fusion UNION ALL de {len(valid_files)} fichiers...")
                        
                        def update_progress(current, total, filename):
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(f"Traitement : {filename}")
                        
                        csv_bytes = merge_parquets_union_to_csv_bytes(
                            files_for_conversion,
                            add_source_column=add_source_column,
                            separator=separator,
                            encoding=encoding,
                            include_header=include_header,
                            convert_complex_to_json=convert_complex,
                            progress_callback=update_progress
                        )
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ Fusion terminée !")
                        
                        st.download_button(
                            label="⬇️ Télécharger merged.csv",
                            data=csv_bytes,
                            file_name="merged.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        st.info(f"📊 Taille du CSV fusionné : {len(csv_bytes) / 1024:.1f} KB")
                    
                    # MERGE MODE: JOIN
                    elif merge_enabled and len(valid_files) > 1 and merge_mode == "join" and join_key:
                        status_text.text(f"JOIN {join_type.upper()} sur '{join_key}'...")
                        
                        def update_progress(current, total, message):
                            progress = current / total
                            progress_bar.progress(progress)
                            status_text.text(message)
                        
                        csv_bytes = merge_parquets_join_to_csv_bytes(
                            files_for_conversion,
                            join_key=join_key,
                            join_type=join_type,
                            separator=separator,
                            encoding=encoding,
                            progress_callback=update_progress
                        )
                        
                        progress_bar.progress(1.0)
                        status_text.text("✅ JOIN terminé !")
                        
                        st.download_button(
                            label="⬇️ Télécharger joined.csv",
                            data=csv_bytes,
                            file_name="joined.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        st.info(f"📊 Taille du CSV joint : {len(csv_bytes) / 1024:.1f} KB")
                    
                    # SINGLE FILE
                    elif len(valid_files) == 1:
                        filename, file_obj = files_for_conversion[0]
                        
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
                        
                        csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                        
                        st.download_button(
                            label=f"⬇️ Télécharger {csv_filename}",
                            data=csv_bytes,
                            file_name=csv_filename,
                            mime="text/csv",
                            use_container_width=True
                        )
                        
                        st.info(f"📊 Taille du CSV : {len(csv_bytes) / 1024:.1f} KB")
                    
                    # MULTIPLE FILES - ZIP (no merge)
                    else:
                        status_text.text(f"Préparation de {len(valid_files)} fichiers...")
                        
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
                    - Pour UNION: vérifiez que les schémas sont compatibles
                    - Pour JOIN: vérifiez que la colonne clé existe dans tous les fichiers
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
        4. **Nouveau !** Pour plusieurs fichiers, activez "Fusionner en un seul output"
           - **UNION ALL** : Empile les lignes de tous les fichiers
           - **JOIN** : Joint les fichiers sur une colonne clé commune
        5. Cliquez sur le bouton de conversion
        6. Téléchargez le résultat
        """)
