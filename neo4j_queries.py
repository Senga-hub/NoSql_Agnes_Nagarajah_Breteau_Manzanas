from database import get_neo4j_driver

driver = get_neo4j_driver()


def run_query(query, parameters=None):
    with driver.session() as session:
        result = session.run(query, parameters or {})
        return [record.data() for record in result]


# 14. Quel est l’acteur ayant joué dans le plus grand nombre de films ?
def q14_actor_most_films():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    RETURN a.name AS acteur, COUNT(f) AS nb_films
    ORDER BY nb_films DESC
    LIMIT 1
    """
    return run_query(query)


# 15. Quels sont les acteurs ayant joué dans des films où Anne Hathaway a également joué ?
def q15_actors_with_anne_hathaway():
    query = """
    MATCH (:Actor {name:"Anne Hathaway"})-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE a.name <> "Anne Hathaway"
    RETURN DISTINCT a.name AS acteur
    ORDER BY acteur
    """
    return run_query(query)


# 16. Quel est l’acteur ayant joué dans des films totalisant le plus de revenus ?
def q16_actor_highest_total_revenue():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    WHERE f.Revenue IS NOT NULL
    RETURN a.name AS acteur, SUM(f.Revenue) AS revenu_total
    ORDER BY revenu_total DESC
    LIMIT 1
    """
    return run_query(query)


# 17. Quelle est la moyenne des votes ?
def q17_average_votes():
    query = """
    MATCH (f:Film)
    WHERE f.Votes IS NOT NULL
    RETURN AVG(f.Votes) AS moyenne_votes
    """
    return run_query(query)


# 18. Quel est le genre le plus représenté dans la base de données ?
def q18_most_represented_genre():
    query = """
    MATCH (f:Film)-[:A_GENRE]->(g:Genre)
    RETURN g.name AS genre, COUNT(f) AS nb_films
    ORDER BY nb_films DESC
    LIMIT 1
    """
    return run_query(query)


# 19. Quels sont les films dans lesquels les acteurs ayant joué avec vous ont également joué ?
def q19_movies_of_coactors(your_name="Paul B"):
    query = """
    MATCH (:Actor {name:$your_name})-[:A_JOUE]->(:Film)<-[:A_JOUE]-(coactor:Actor)
    MATCH (coactor)-[:A_JOUE]->(otherFilm:Film)
    RETURN DISTINCT coactor.name AS acteur, otherFilm.title AS film
    ORDER BY acteur, film
    """
    return run_query(query, {"your_name": your_name})


# 20. Quel réalisateur a travaillé avec le plus grand nombre d’acteurs distincts ?
def q20_director_most_distinct_actors():
    query = """
    MATCH (d:Director)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    RETURN d.name AS realisateur, COUNT(DISTINCT a) AS nb_acteurs_distincts
    ORDER BY nb_acteurs_distincts DESC
    LIMIT 1
    """
    return run_query(query)


# 21. Quels sont les films les plus connectés ?
def q21_most_connected_films(limit=10):
    query = """
    MATCH (f1:Film)<-[:A_JOUE]-(a:Actor)-[:A_JOUE]->(f2:Film)
    WHERE f1 <> f2
    RETURN f1.title AS film, COUNT(DISTINCT f2) AS nb_films_connectes
    ORDER BY nb_films_connectes DESC
    LIMIT $limit
    """
    return run_query(query, {"limit": limit})


# 22. Trouver les 5 acteurs ayant joué avec le plus de réalisateurs différents
def q22_top_5_actors_most_directors():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_REALISE]-(d:Director)
    RETURN a.name AS acteur, COUNT(DISTINCT d) AS nb_realisateurs
    ORDER BY nb_realisateurs DESC
    LIMIT 5
    """
    return run_query(query)


# 23. Recommander un film à un acteur selon les genres des films où il a déjà joué
def q23_recommend_film_to_actor(actor_name="Paul B", limit=5):
    query = """
    MATCH (a:Actor {name:$actor_name})-[:A_JOUE]->(:Film)-[:A_GENRE]->(g:Genre)
    WITH a, COLLECT(DISTINCT g.name) AS genres_aimes

    MATCH (recFilm:Film)-[:A_GENRE]->(g:Genre)
    WHERE g.name IN genres_aimes
      AND NOT (a)-[:A_JOUE]->(recFilm)
    RETURN recFilm.title AS film_recommande, COUNT(DISTINCT g) AS score_genres_communs
    ORDER BY score_genres_communs DESC, film_recommande
    LIMIT $limit
    """
    return run_query(query, {"actor_name": actor_name, "limit": limit})


# 24. Créer une relation INFLUENCE_PAR entre réalisateurs selon les genres en commun
def q24_create_director_influence():
    query = """
    MATCH (d1:Director)-[:A_REALISE]->(:Film)-[:A_GENRE]->(g:Genre)
    MATCH (d2:Director)-[:A_REALISE]->(:Film)-[:A_GENRE]->(g)
    WHERE d1 <> d2
    WITH d1, d2, COUNT(DISTINCT g) AS genres_communs
    WHERE genres_communs >= 2
    MERGE (d1)-[r:INFLUENCE_PAR]->(d2)
    SET r.genres_communs = genres_communs
    RETURN d1.name AS realisateur1, d2.name AS realisateur2, r.genres_communs AS genres_communs
    ORDER BY genres_communs DESC
    """
    return run_query(query)


def q24_show_director_influence():
    query = """
    MATCH (d1:Director)-[r:INFLUENCE_PAR]->(d2:Director)
    RETURN d1.name AS realisateur1, d2.name AS realisateur2, r.genres_communs AS genres_communs
    ORDER BY genres_communs DESC, realisateur1, realisateur2
    """
    return run_query(query)


# 25. Quel est le chemin le plus court entre deux acteurs donnés ?
def q25_shortest_path_between_actors(actor1="Tom Hanks", actor2="Scarlett Johansson"):
    query = """
    MATCH p = shortestPath((a1:Actor {name:$actor1})-[*]-(a2:Actor {name:$actor2}))
    RETURN p
    """
    return run_query(query, {"actor1": actor1, "actor2": actor2})


# Version plus lisible de la 25 : on renvoie juste la longueur
def q25_shortest_path_length(actor1="Tom Hanks", actor2="Scarlett Johansson"):
    query = """
    MATCH p = shortestPath((a1:Actor {name:$actor1})-[*]-(a2:Actor {name:$actor2}))
    RETURN length(p) AS longueur_chemin
    """
    return run_query(query, {"actor1": actor1, "actor2": actor2})



# 26. Communautés d'acteurs avec Louvain


def q26_create_coactor_relationships():
    """
    Crée des relations CO_ACTOR entre deux acteurs
    s'ils ont joué dans le même film.
    """
    query = """
    MATCH (a1:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a2:Actor)
    WHERE a1 <> a2
    MERGE (a1)-[:CO_ACTOR]-(a2)
    """
    return run_query(query)


def q26_drop_actor_graph_if_exists():
    """
    Supprime le graphe projeté actorGraph s'il existe déjà,
    pour éviter l'erreur 'graph already exists'.
    """
    query = """
    CALL gds.graph.exists('actorGraph')
    YIELD exists
    WITH exists
    WHERE exists = true
    CALL gds.graph.drop('actorGraph')
    YIELD graphName
    RETURN graphName
    """
    return run_query(query)


def q26_project_actor_graph():
    """
    Crée une projection GDS nommée actorGraph
    à partir des noeuds Actor et des relations CO_ACTOR.
    """
    query = """
    CALL gds.graph.project(
        'actorGraph',
        'Actor',
        {
            CO_ACTOR: {
                type: 'CO_ACTOR',
                orientation: 'UNDIRECTED'
            }
        }
    )
    YIELD graphName, nodeCount, relationshipCount
    RETURN graphName, nodeCount, relationshipCount
    """
    return run_query(query)


def q26_louvain_communities():
    """
    Lance l'algorithme Louvain sur actorGraph
    et retourne chaque acteur avec son communityId.
    """
    query = """
    CALL gds.louvain.stream('actorGraph')
    YIELD nodeId, communityId
    RETURN gds.util.asNode(nodeId).name AS acteur,
           communityId
    ORDER BY communityId, acteur
    """
    return run_query(query)


def q26_run_full_analysis():
    """
    Exécute toute la pipeline de la question 26 :
    1. créer les relations CO_ACTOR
    2. supprimer actorGraph s'il existe
    3. recréer actorGraph
    4. lancer Louvain
    """
    q26_create_coactor_relationships()
    q26_drop_actor_graph_if_exists()
    q26_project_actor_graph()
    return q26_louvain_communities()

if __name__ == "__main__":
    print("Q14:", q14_actor_most_films())
    print("Q15:", q15_actors_with_anne_hathaway())
    print("Q16:", q16_actor_highest_total_revenue())
    print("Q17:", q17_average_votes())
    print("Q18:", q18_most_represented_genre())
    print("Q19:", q19_movies_of_coactors("Paul B"))
    print("Q20:", q20_director_most_distinct_actors())
    print("Q21:", q21_most_connected_films())
    print("Q22:", q22_top_5_actors_most_directors())
    print("Q23:", q23_recommend_film_to_actor("Paul B"))
    print("Q24 create:", q24_create_director_influence())
    print("Q24 show:", q24_show_director_influence())
    print("Q25 length:", q25_shortest_path_length("Tom Hanks", "Scarlett Johansson"))