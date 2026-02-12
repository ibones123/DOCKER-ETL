# DOCKER-ETL

Projet DevOps / ETL simple qui récupère des données pays depuis l’API RestCountries puis les charge dans une base SQLite.  
Le projet peut être exécuté en local avec Python ou via Docker.

---

## Description du projet

Ce projet met en place une pipeline ETL classique :

- Extract : appel HTTP vers l’API RestCountries
- Transform : sélection et structuration des champs utiles (nom, code pays, région, population, capitale)
- Load : insertion ou mise à jour des données dans une base SQLite

L’objectif est d’avoir une base de données locale propre et exploitable pour de l’analyse, une API ou un futur dashboard.

---

## Architecture

### Back-end

Le back-end correspond au script Python qui :

- appelle l’API RestCountries
- transforme les données reçues
- initialise la base de données si nécessaire
- insère ou met à jour les enregistrements

Fichiers principaux :

- `etl.py` : contient la logique ETL ainsi que l’initialisation de la base
- `main.py` : point d’entrée qui déclenche l’exécution

La base de données SQLite contient une table `countries` avec :

- id (clé primaire auto-incrémentée)
- cca2 (code pays unique)
- name
- region
- population
- capital
- loaded_at (date/heure de chargement)

Une contrainte UNIQUE sur `cca2` empêche les doublons.

### Front-end

Il n’y a pas d’interface web dans ce projet.  
Les données peuvent être consultées :

- avec sqlite3 en ligne de commande
- avec un outil graphique comme DB Browser for SQLite
- ou via une future API / application (Streamlit, FastAPI, etc.)

---

## Lancer le projet en local

### 1. Prérequis

- Python 3.10 ou supérieur
- pip

### 2. Cloner le repository

```bash
git clone https://github.com/ibones123/DOCKER-ETL.git
cd DOCKER-ETL
### 3. Créer un environnement virtuel
python -m venv .venv
source .venv/bin/activate

### 4. Installer les dépendances
pip install -r requirements.txt

### 5. Lancer l’application
python main.py

La base SQLite sera créée automatiquement si elle n’existe pas.

## Lancer le projet avec Docker

### Build de l’image
docker build -t docker-etl .

### Lancer le conteneur
docker run --rm -v "$(pwd)/data:/data" docker-etl
