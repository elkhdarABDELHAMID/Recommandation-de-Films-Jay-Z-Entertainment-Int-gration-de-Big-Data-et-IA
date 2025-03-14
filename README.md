# Système de Recommandation de Films

Ce projet est un système de recommandation de films utilisant **Elasticsearch** pour la recherche et l'indexation des données, **Apache Spark** pour le traitement des données, et une API **Flask** pour interagir avec les utilisateurs. Il inclut également **Kibana** pour visualiser les données.

## Table des Matières
- [Description](#description)
- [Technologies Utilisées](#technologies-utilisées)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Utilisation](#utilisation)
- [Structure du Projet](#structure-du-projet)
- [Endpoints de l'API](#endpoints-de-lapi)
- [Exemple de Résultat](#exemple-de-résultat)
- [Contribution](#contribution)
- [Licence](#licence)

## Description
Ce système permet de :
- Indexer des données de films (comme les titres, genres, et dates de sortie) dans Elasticsearch en utilisant Spark.
- Rechercher des films via une API Flask.
- Visualiser les données (par exemple, la distribution des genres) avec Kibana.

Les données sont issues d'un fichier (`u.item`), qui contient des informations sur 1682 films.

## Technologies Utilisées
- **Apache Spark** : Pour le traitement et l'ingestion des données.
- **Elasticsearch** : Pour l'indexation et la recherche des films.
- **Flask** : Pour l'API RESTful.
- **Kibana** : Pour la visualisation des données.
- **Docker** : Pour la conteneurisation des services.

## Prérequis
- Docker et Docker Compose installés sur votre machine.
- Git pour cloner le projet.
- Python 3.8+ (optionnel, si vous travaillez hors Docker).
- Accès à Internet pour télécharger les images Docker.

## Installation
1. Clonez le dépôt :
   ```bash
   git clone https://github.com/tonusername/movie-recommendation-system.git
   cd movie-recommendation-system
