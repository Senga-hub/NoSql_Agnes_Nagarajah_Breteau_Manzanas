import math
from database import get_collection, get_db

collection = get_collection()
db = get_db()


# Fonctions utilitaires


def get_all_movies(limit=50):
    return list(collection.find({}, {"_id": 0}).limit(limit))


# Requêtes du sujet


# 1. Afficher l’année où le plus grand nombre de films ont été sortis
def q1_year_with_most_movies():
    pipeline = [
        {"$match": {"year": {"$ne": None}}},
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
        {"$limit": 1}
    ]
    return list(collection.aggregate(pipeline))

# 2. Quel est le nombre de films sortis après 1999
def q2_count_movies_after_1999():
    return collection.count_documents({"year": {"$gt": 1999}})

# 3. Quelle est la moyenne des votes des films sortis en 2007
def q3_average_votes_2007():
    pipeline = [
        {"$match": {"year": 2007, "Votes": {"$ne": None}}},
        {"$group": {"_id": None, "avg_votes": {"$avg": "$Votes"}}},
        {"$project": {"_id": 0, "avg_votes": 1}}
    ]
    return list(collection.aggregate(pipeline))

# 4. Histogramme : nombre de films par année
def q4_movies_per_year():
    pipeline = [
        {"$match": {"year": {"$ne": None}}},
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

# 5. Quelles sont les genres de films disponibles dans la base

def q5_all_genres():
    pipeline = [
        {
            "$project": {
                "genresArray": {
                    "$split": ["$genre", ","]
                }
            }
        },
        {"$unwind": "$genresArray"},
        {
            "$project": {
                "genre": {"$trim": {"input": "$genresArray"}}
            }
        },
        {"$group": {"_id": "$genre"}},
        {"$sort": {"_id": 1}}
    ]
    return [doc["_id"] for doc in collection.aggregate(pipeline)]

# 6. Quel est le film qui a généré le plus de revenu
def q6_movie_with_highest_revenue():
    pipeline = [
        {
            "$addFields": {
                "revenue_num": {
                    "$convert": {
                        "input": "$Revenue (Millions)",
                        "to": "double",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {
            "$match": {
                "revenue_num": {"$ne": None}
            }
        },
        {
            "$sort": {"revenue_num": -1}
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "year": 1,
                "Revenue (Millions)": "$revenue_num"
            }
        }
    ]
    return list(collection.aggregate(pipeline))

# 7. Quels sont les réalisateurs ayant réalisé plus de 5 films
def q7_directors_more_than_5_movies():
    pipeline = [
        {"$match": {"Director": {"$ne": None}}},
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},
        {"$sort": {"count": -1, "_id": 1}}
    ]
    return list(collection.aggregate(pipeline))

# 8. Quel est le genre de film qui rapporte en moyenne le plus de revenus
def q8_top_genre_by_avg_revenue():
    pipeline = [
        {
            "$match": {
                "genre": {"$ne": None},
                "Revenue (Millions)": {"$ne": None}
            }
        },
        {
            "$project": {
                "Revenue (Millions)": 1,
                "genresArray": {"$split": ["$genre", ","]}
            }
        },
        {"$unwind": "$genresArray"},
        {
            "$project": {
                "Revenue (Millions)": 1,
                "genre": {"$trim": {"input": "$genresArray"}}
            }
        },
        {
            "$group": {
                "_id": "$genre",
                "avg_revenue": {"$avg": "$Revenue (Millions)"},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"avg_revenue": -1}},
        {"$limit": 1}
    ]
    return list(collection.aggregate(pipeline))

# 9. Quels sont les 3 films les mieux notés pour chaque décennie

def q9_top_3_movies_by_decade():

    pipeline = [
        {
            "$addFields": {
                "metascore_num": {
                    "$convert": {
                        "input": "$Metascore",
                        "to": "double",
                        "onError": None,
                        "onNull": None
                    }
                },
                "year_num": {
                    "$convert": {
                        "input": "$year",
                        "to": "int",
                        "onError": None,
                        "onNull": None
                    }
                }
            }
        },
        {
            "$match": {
                "year_num": {"$ne": None},
                "metascore_num": {"$ne": None}
            }
        },
        {
            "$addFields": {
                "decade": {
                    "$subtract": ["$year_num", {"$mod": ["$year_num", 10]}]
                }
            }
        },
        {
            "$sort": {
                "decade": 1,
                "metascore_num": -1,
                "title": 1
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "movies": {
                    "$push": {
                        "title": "$title",
                        "year": "$year_num",
                        "Metascore": "$metascore_num"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "decade": "$_id",
                "top3": {"$slice": ["$movies", 3]}
            }
        },
        {
            "$sort": {"decade": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))

    rows = []

    for block in results:
        decade = block["decade"]
        for movie in block["top3"]:
            rows.append({
                "Décennie": f"{decade}s",
                "Titre": movie["title"],
                "Année": movie["year"],
                "Metascore": movie["Metascore"]
            })

    return rows
# 10. Quel est le film le plus long par genre
def q10_longest_movie_by_genre():
    pipeline = [
        {
            "$match": {
                "genre": {"$ne": None},
                "Runtime (Minutes)": {"$ne": None}
            }
        },
        {
            "$project": {
                "title": 1,
                "year": 1,
                "Runtime (Minutes)": 1,
                "genresArray": {"$split": ["$genre", ","]}
            }
        },
        {"$unwind": "$genresArray"},
        {
            "$project": {
                "title": 1,
                "year": 1,
                "Runtime (Minutes)": 1,
                "genre": {"$trim": {"input": "$genresArray"}}
            }
        },
        {"$sort": {"genre": 1, "Runtime (Minutes)": -1, "title": 1}},
        {
            "$group": {
                "_id": "$genre",
                "title": {"$first": "$title"},
                "year": {"$first": "$year"},
                "runtime": {"$first": "$Runtime (Minutes)"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "genre": "$_id",
                "title": 1,
                "year": 1,
                "runtime": 1
            }
        },
        {"$sort": {"genre": 1}}
    ]
    return list(collection.aggregate(pipeline))

# 11. Créer une vue MongoDB affichant uniquement les films ayant Metascore > 80
# et Revenue > 50 millions
def q11_create_view_high_score_high_revenue():
    view_name = "vue_films_elite"

    existing = db.list_collection_names()
    if view_name in existing:
        db.drop_collection(view_name)

    db.command({
        "create": view_name,
        "viewOn": "films",
        "pipeline": [
            {
                "$match": {
                    "Metascore": {"$gt": 80},
                    "Revenue (Millions)": {"$gt": 50}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "year": 1,
                    "Director": 1,
                    "Metascore": 1,
                    "Revenue (Millions)": 1
                }
            }
        ]
    })

    return list(db[view_name].find({}, {"_id": 0}))

# 12. Corrélation entre Runtime et Revenue (Pearson en Python)
def q12_runtime_revenue_correlation():

    docs = list(collection.find(
        {
            "Runtime (Minutes)": {"$ne": None},
            "Revenue (Millions)": {"$ne": None}
        },
        {
            "_id": 0,
            "title": 1,
            "Runtime (Minutes)": 1,
            "Revenue (Millions)": 1
        }
    ))

    x = []
    y = []

    # conversion sécurisée en float
    for doc in docs:
        try:
            runtime = float(doc["Runtime (Minutes)"])
            revenue = float(doc["Revenue (Millions)"])

            x.append(runtime)
            y.append(revenue)

        except:
            pass

    # vérifier qu'on a assez de données
    if len(x) < 2:
        return {
            "correlation": None,
            "sample_size": len(x),
            "interpretation": "Pas assez de données pour calculer la corrélation."
        }

    # moyenne
    mean_x = sum(x) / len(x)
    mean_y = sum(y) / len(y)

    # calcul du coefficient de Pearson
    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))

    denom_x = sum((xi - mean_x) ** 2 for xi in x) ** 0.5
    denom_y = sum((yi - mean_y) ** 2 for yi in y) ** 0.5

    if denom_x == 0 or denom_y == 0:
        corr = None
    else:
        corr = numerator / (denom_x * denom_y)

    # interprétation
    if corr is None:
        interpretation = "Corrélation impossible à calculer"
    elif corr > 0.7:
        interpretation = "Forte corrélation positive"
    elif corr > 0.3:
        interpretation = "Corrélation positive modérée"
    elif corr > 0:
        interpretation = "Faible corrélation positive"
    elif corr < -0.7:
        interpretation = "Forte corrélation négative"
    elif corr < -0.3:
        interpretation = "Corrélation négative modérée"
    elif corr < 0:
        interpretation = "Faible corrélation négative"
    else:
        interpretation = "Pas de corrélation"

    return {
        "correlation": corr,
        "sample_size": len(x),
        "interpretation": interpretation
    }
# 13. Évolution de la durée moyenne des films par décennie
def q13_avg_runtime_by_decade():
    pipeline = [
        {
            "$match": {
                "year": {"$ne": None},
                "Runtime (Minutes)": {"$ne": None}
            }
        },
        {
            "$addFields": {
                "decade": {
                    "$subtract": ["$year", {"$mod": ["$year", 10]}]
                }
            }
        },
        {
            "$group": {
                "_id": "$decade",
                "avg_runtime": {"$avg": "$Runtime (Minutes)"},
                "count": {"$sum": 1}
            }
        },
        {
            "$project": {
                "_id": 0,
                "decade": "$_id",
                "avg_runtime": 1,
                "count": 1
            }
        },
        {"$sort": {"decade": 1}}
    ]
    return list(collection.aggregate(pipeline))



