# Parquet → CSV Converter

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://anefapi-ujclrot25dw4b77r2fg7w6.streamlit.app/)

Application Streamlit professionnelle pour convertir, fusionner et transformer des fichiers Parquet en CSV. Déployée sur Streamlit Cloud avec authentification par mot de passe.

## 🚀 Fonctionnalités

### Conversion Parquet → CSV
- **Upload multiple** : Convertissez un ou plusieurs fichiers en une seule opération
- **Conversion streaming** : Gestion optimisée de la mémoire avec `iter_batches`
- **Options CSV personnalisables** :
  - Séparateur (`,`, `;`, `\t`, `|`)
  - Encodage (`utf-8`, `latin-1`, `cp1252`)
  - En-têtes inclus/exclus
  - Types complexes → JSON
- **Export ZIP** : Archive compressée pour plusieurs fichiers
- **Aperçu** : 50 premières lignes + schéma avant conversion

### 🔗 Fusion de fichiers
- **UNION ALL** : Empiler les lignes de plusieurs fichiers (streaming PyArrow)
  - Unification automatique des schémas
  - Option colonne `_source_file` pour tracer l'origine
- **JOIN sur clé** : Joindre les fichiers sur une colonne commune (DuckDB)
  - Types : INNER, LEFT, OUTER
  - Gestion des collisions de colonnes

### 🔐 Authentification
- Protection par mot de passe via Streamlit Secrets
- Session persistante sur toutes les pages
- Comparaison sécurisée (`hmac.compare_digest`)
- Bouton de déconnexion

---

## 📋 Utilisation

1. Accédez à l'app : [Parquet → CSV Converter](https://anefapi-ujclrot25dw4b77r2fg7w6.streamlit.app/)
2. Entrez le mot de passe
3. Naviguez vers "Parquet → CSV"
4. Uploadez vos fichiers `.parquet`
5. **Fichier unique** : Convertir → Télécharger CSV
6. **Fichiers multiples** :
   - Sans fusion → Télécharger ZIP
   - Avec fusion (UNION/JOIN) → Télécharger CSV unique

---

## 🔐 Configuration des Secrets

### Sur Streamlit Cloud

Dans **Settings > Secrets** :

```toml
[auth]
required = true
password = "votre_mot_de_passe"
```

### En local

Créez `.streamlit/secrets.toml` :

```toml
[auth]
required = true
password = "dev_password"
```

> ⚠️ Ce fichier est dans `.gitignore` - ne jamais le commiter !

---

## ⚠️ Limites

| Paramètre | Limite | Note |
|-----------|--------|------|
| Taille max/fichier | 200 MB | Configurable dans `config.toml` |
| Mémoire | ~1 GB | Limite Streamlit Cloud |
| Types complexes | Tous | Sérialisés en JSON |

---

## 🛠️ Développement local

```bash
# Cloner
git clone https://github.com/samirelhassani1998/PARQUET_to_CSV.git
cd PARQUET_to_CSV

# Installer
pip install -r requirements.txt

# Configurer les secrets
cp .streamlit/secrets.template.toml .streamlit/secrets.toml
# Éditer secrets.toml avec votre mot de passe

# Lancer
streamlit run streamlit_app.py

# Tests (31 tests)
pip install pytest
pytest tests/ -v
```

---

## 📁 Structure du projet

```
PARQUET_to_CSV/
├── .streamlit/
│   ├── config.toml              # Config Streamlit (upload limit)
│   ├── secrets.template.toml    # Template pour secrets
│   └── secrets.toml             # Secrets locaux (gitignored)
├── app/
│   ├── auth.py                  # Authentification
│   └── services/
│       └── parquet_to_csv.py    # Conversion + fusion
├── pages/
│   └── 1_Parquet_to_CSV.py      # Page principale
├── scripts/
│   ├── build_windows.ps1        # Build Windows (PowerShell)
│   └── build_windows.cmd        # Build Windows (CMD)
├── tests/
│   ├── test_auth.py             # Tests auth (5)
│   └── test_parquet_to_csv.py   # Tests conversion (26)
├── streamlit_app.py             # Point d'entrée Streamlit
├── run_app.py                   # Lanceur pour .exe
├── ParquetToCSV.spec            # Config PyInstaller
├── requirements.txt             # streamlit, pyarrow, duckdb
└── README.md
```

---

## 📦 Technologies

| Technologie | Usage |
|-------------|-------|
| [Streamlit](https://streamlit.io/) | Interface web |
| [PyArrow](https://arrow.apache.org/docs/python/) | Lecture/écriture Parquet, CSV streaming |
| [DuckDB](https://duckdb.org/) | JOIN SQL performant |

---

## 🐛 Troubleshooting

| Erreur | Solution |
|--------|----------|
| "Cannot read Parquet" | Vérifiez que le fichier n'est pas corrompu |
| "Memory error" | Fichier trop gros → traiter en local |
| Caractères incorrects | Changez l'encodage (`latin-1`) |
| "Mot de passe incorrect" | Vérifiez les Secrets sur Streamlit Cloud |

---

## 🖥️ Build Windows (.exe)

### Prérequis machine de build

- Windows 10/11
- Python 3.9+ (testé avec 3.11)
- ~2 GB d'espace disque

### Commande de build

```powershell
# Option 1: PowerShell
.\scripts\build_windows.ps1

# Option 2: CMD
.\scripts\build_windows.cmd

# Avec nettoyage préalable
.\scripts\build_windows.ps1 -Clean
```

### Artefact produit

```
dist\ParquetToCSV\
├── ParquetToCSV.exe      # Exécutable principal
├── _internal\            # Dépendances Python
├── pages\                # Pages Streamlit
├── app\                  # Modules applicatifs
└── .streamlit\           # Config (sans secrets)
```

---

## 📦 Installation utilisateur final

### Étapes

1. **Copier** le dossier `dist\ParquetToCSV\` sur le PC cible
2. **Double-clic** sur `ParquetToCSV.exe`
3. Le navigateur s'ouvre automatiquement sur `http://localhost:8501`

### Configuration mot de passe (optionnel)

Par défaut, l'app fonctionne **sans authentification** si aucun secret n'est configuré.

Pour activer l'auth :

**Option 1 : Global utilisateur** (recommandé)
```
%USERPROFILE%\.streamlit\secrets.toml
```

**Option 2 : Par projet**
```
[dossier_ParquetToCSV]\.streamlit\secrets.toml
```

Contenu du fichier :
```toml
[auth]
required = true
password = "votre_mot_de_passe"
```

---

## 🔧 Troubleshooting Windows

| Problème | Solution |
|----------|----------|
| Port 8501 occupé | Fermez l'autre instance ou modifiez le port dans `run_app.py` |
| Antivirus bloque l'exe | Ajoutez une exception pour `ParquetToCSV.exe` |
| "DLL not found" pyarrow | Rébuilder avec `--clean` ou installer VC++ Redistributable |
| Fenêtre console se ferme vite | Lancez depuis CMD pour voir les erreurs |
| Navigateur ne s'ouvre pas | Ouvrez manuellement `http://localhost:8501` |

---

## 📄 Licence

MIT License - voir [LICENSE](LICENSE)
