# Parquet → CSV Converter

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://anefapi-ujclrot25dw4b77r2fg7w6.streamlit.app/)

Outil professionnel pour convertir des fichiers Parquet en CSV, déployé sur Streamlit Cloud.

## 🚀 Fonctionnalités

- **Upload multiple** : Convertissez un ou plusieurs fichiers Parquet en une seule opération
- **Conversion streaming** : Gestion optimisée de la mémoire pour les gros fichiers (utilisation de `iter_batches`)
- **Options flexibles** :
  - Séparateur personnalisable (`,`, `;`, `\t`, `|`)
  - Encodage de sortie (`utf-8`, `latin-1`, `cp1252`)
  - Inclusion/exclusion des en-têtes
  - Conversion des types complexes (list, struct, map) en JSON
- **Export ZIP** : Téléchargez plusieurs fichiers CSV dans une archive ZIP compressée
- **Aperçu des données** : Visualisez les 50 premières lignes avant conversion

## 📋 Utilisation

1. Accédez à l'application : [Parquet → CSV Converter](https://anefapi-ujclrot25dw4b77r2fg7w6.streamlit.app/)
2. Naviguez vers la page "Parquet → CSV" dans la barre latérale
3. Uploadez un ou plusieurs fichiers `.parquet`
4. Vérifiez l'aperçu et ajustez les options si nécessaire
5. Cliquez sur "Convertir en CSV"
6. Téléchargez le résultat (CSV unique ou archive ZIP)

## 🔐 Authentification

L'application est protégée par mot de passe. L'authentification persiste sur toutes les pages via `session_state`.

### Configuration sur Streamlit Cloud

1. Allez dans les paramètres de votre app sur [Streamlit Cloud](https://share.streamlit.io/)
2. Cliquez sur **Secrets** dans le menu
3. Ajoutez la configuration suivante :

```toml
[auth]
required = true
password = "votre_mot_de_passe_secret"
```

### Configuration locale

Pour le développement local, créez le fichier `.streamlit/secrets.toml` :

```toml
[auth]
required = true
password = "dev_password"
```

> ⚠️ **Important** : Ne commitez jamais `secrets.toml` dans Git ! Le fichier est déjà dans `.gitignore`.

### Désactiver l'authentification

Pour désactiver temporairement l'authentification, mettez `required = false` dans les secrets.

## ⚠️ Limites

| Paramètre | Limite | Note |
|-----------|--------|------|
| Taille max par fichier | 200 MB | Configurable dans `.streamlit/config.toml` |
| Mémoire disponible | ~1 GB | Limite Streamlit Cloud |
| Types supportés | Tous | Les types complexes sont sérialisés en JSON |

### Conseils pour les gros fichiers

- Préférez traiter les fichiers un par un pour économiser la mémoire
- Activez l'option "Convertir types complexes en JSON" pour éviter les erreurs
- Pour les fichiers > 200 MB, utilisez des outils CLI comme `pyarrow` en local

## 🛠️ Développement local

### Prérequis

- Python 3.8+
- pip

### Installation

```bash
# Cloner le repo
git clone https://github.com/votre-repo/PARQUET_to_CSV.git
cd PARQUET_to_CSV

# Installer les dépendances
pip install -r requirements.txt

# Lancer l'application
streamlit run streamlit_app.py
```

### Tests

```bash
# Installer pytest
pip install pytest

# Lancer les tests
pytest tests/ -v
```

## 📁 Structure du projet

```
PARQUET_to_CSV/
├── .streamlit/
│   ├── config.toml          # Configuration Streamlit
│   └── secrets.toml          # Secrets (local only, gitignored)
├── app/
│   ├── auth.py               # Authentification
│   └── services/
│       └── parquet_to_csv.py # Logique de conversion
├── pages/
│   └── 1_Parquet_to_CSV.py   # Page de conversion
├── tests/
│   └── test_parquet_to_csv.py # Tests unitaires
├── streamlit_app.py          # Point d'entrée
├── requirements.txt          # Dépendances
└── README.md
```

## 🐛 Troubleshooting

### "Cannot read Parquet file"
- Vérifiez que le fichier est un Parquet valide (non corrompu)
- Essayez d'ouvrir le fichier localement avec `pyarrow`

### "Memory error" ou crash
- Le fichier est trop volumineux pour Streamlit Cloud
- Solutions :
  - Découpez le fichier en parties plus petites
  - Utilisez l'outil en local avec plus de RAM

### Caractères spéciaux incorrects
- Changez l'encodage de sortie (`latin-1` pour les caractères européens)

## 📦 Technologies

- [Streamlit](https://streamlit.io/) - Interface web
- [PyArrow](https://arrow.apache.org/docs/python/) - Lecture/écriture Parquet et CSV

## 📄 Licence

MIT License - voir [LICENSE](LICENSE)
