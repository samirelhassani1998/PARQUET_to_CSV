"""
Parquet → CSV Converter - Streamlit App

A professional Streamlit application for converting Parquet files to CSV format.
Supports single and multiple file uploads with streaming conversion.
"""

import streamlit as st

# Page configuration - must be the first Streamlit command
st.set_page_config(
    page_title="Parquet → CSV Converter",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Authentication gate - blocks access until authenticated
from app.auth import require_password, show_logout_button
require_password()
show_logout_button()

# Main page content
st.title("📊 Parquet → CSV Converter")

st.markdown("""
Bienvenue dans l'outil de conversion Parquet vers CSV !

### Fonctionnalités

- 📁 **Upload multiple** : Convertissez un ou plusieurs fichiers Parquet en une seule fois
- ⚡ **Conversion streaming** : Gestion optimisée de la mémoire pour les gros fichiers
- 🔧 **Options flexibles** : Personnalisez le séparateur, l'encodage et plus encore
- 📦 **Export ZIP** : Téléchargez plusieurs fichiers CSV dans une archive ZIP

### Pour commencer

👈 Utilisez la navigation dans la barre latérale pour accéder à la page **Parquet → CSV**.

---

### Limites

| Paramètre | Limite |
|-----------|--------|
| Taille max par fichier | 200 MB |
| Nombre de fichiers | Illimité |
| Mémoire disponible | Limitée (Streamlit Cloud) |

> ⚠️ Pour les fichiers très volumineux, préférez la conversion locale ou des outils CLI comme `pyarrow`.
""")

# Sidebar info
with st.sidebar:
    st.header("ℹ️ À propos")
    st.markdown("""
    Cette application utilise **PyArrow** pour une conversion 
    efficace des fichiers Parquet vers CSV.
    
    **Technologies utilisées :**
    - Streamlit
    - PyArrow
    
    [📖 Documentation](https://arrow.apache.org/docs/python/)
    """)
