# Projet NoSQL - MongoDB & Neo4j

Application Python / Streamlit d'exploration et d'interrogation de bases de données NoSQL (MongoDB et Neo4j) dans le cadre du Projet NoSQL-MangDB.

---

## Prérequis

- Python 3.10+
- Un accès aux instances cloud MongoDB et Neo4j (envoyée par mail)

---

## Installation

1. **Cloner le projet**

```bash
git clone https://github.com/Senga-hub/NoSql_Agnes_Nagarajah_Breteau_Manzanas
cd NoSql_Agnes_Nagarajah_Breteau_Manzanas
```

2. **Créer et activer un environnement virtuel** *(recommandé)*

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

3. **Installer les dépendances**

```bash
pip install -r requirements.txt
```

4. **Configurer les variables d'environnement**

Créer un fichier `.env` à la racine du projet qui ont été envoyée par mail :

```env
MONGO_URI=your_mongodb_uri
DB_NAME=entertainment
COLLECTION_NAME=films

NEO4J_URI=your_neo4j_uri
NEO4J_USER=your_neo4j_user
NEO4J_PASSWORD=your_neo4j_password
```

---

## Lancement

```bash
python -m streamlit run app.py
```

L'application sera accessible sur `http://localhost:8501`.

---

## Structure du projet

```
Projet Nosql/
├── app.py                  # Application principale Streamlit
├── config.py               # Chargement des variables d'environnement
├── database.py             # Connexions MongoDB et Neo4j
├── queries.py              # Requêtes MongoDB (questions 1 à 13)
├── neo4j_queries.py        # Requêtes Cypher Neo4j (questions 14 à 26)
├── transversal_queries.py  # Questions transversales (27 à 30)
├── mongo_to_neo4j.py       # Migration des données MongoDB vers Neo4j
├── requirements.txt        # Dépendances Python
└── .env                    # Variables d'environnement (non versionné)
```

---

## Fonctionnalités

### MongoDB
- Requêtes d'analyse sur la collection `films` (base `entertainment`)
- Statistiques : films par année, genres, revenus, corrélations, etc.
- Visualisations avec Matplotlib

### Neo4j
- Import des données depuis MongoDB
- Création de nœuds : `Film`, `Actor`, `Realisateur`
- Relations : `A_JOUE`, `INFLUENCE_PAR`, `CONCURRENT`
- Requêtes Cypher : chemin le plus court, communautés d'acteurs, recommandations

---

## Dépendances principales

| Bibliothèque | Usage |
|---|---|
| `streamlit` | Interface web |
| `pymongo` | Connexion MongoDB |
| `neo4j` | Connexion Neo4j |
| `pandas` | Manipulation des données |
| `matplotlib` | Visualisations |
| `numpy` | Calculs numériques |
| `python-dotenv` | Gestion des variables d'environnement |
