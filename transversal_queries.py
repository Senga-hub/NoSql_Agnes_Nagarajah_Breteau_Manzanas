from database import get_neo4j_driver

driver = get_neo4j_driver()



# Fonction utilitaire pour exécuter une requête Cypher


def run_query(query, parameters=None):

    with driver.session() as session:

        result = session.run(query, parameters or {})

        return [record.data() for record in result]



# 27. Films ayant des genres en commun mais
#     réalisés par des réalisateurs différents


def q27_similar_genre_different_director():

    query = """
    MATCH (f1:Film)-[:A_GENRE]->(g:Genre)<-[:A_GENRE]-(f2:Film)

    MATCH (f1)<-[:A_REALISE]-(d1:Director)
    MATCH (f2)<-[:A_REALISE]-(d2:Director)

    WHERE f1 <> f2
    AND d1 <> d2

    RETURN f1.title AS film1,
           f2.title AS film2,
           g.name AS genre_commun,
           d1.name AS realisateur1,
           d2.name AS realisateur2

    
    """

    return run_query(query)



# 28. Recommander des films selon les préférences
#     d'un acteur donné


def q28_recommend_films(actor):

    query = """
    MATCH (a:Actor {name:$actor})-[:A_JOUE]->(:Film)-[:A_GENRE]->(g:Genre)

    WITH a, COLLECT(DISTINCT g.name) AS genres_aimes

    MATCH (f:Film)-[:A_GENRE]->(g:Genre)

    WHERE g.name IN genres_aimes
    AND NOT (a)-[:A_JOUE]->(f)

    RETURN f.title AS film_recommande,
           COUNT(g) AS score_genres_communs

    ORDER BY score_genres_communs DESC
    
    """

    return run_query(query, {"actor": actor})



# 29. Créer une relation de concurrence entre
#     réalisateurs ayant réalisé des films
#     similaires la même année


def q29_create_director_competition():

    query = """
    MATCH (d1:Director)-[:A_REALISE]->(f1:Film)-[:A_GENRE]->(g:Genre)
    MATCH (d2:Director)-[:A_REALISE]->(f2:Film)-[:A_GENRE]->(g)

    WHERE f1.year = f2.year
    AND d1 <> d2

    MERGE (d1)-[r:CONCURRENCE]->(d2)

    SET r.genre = g.name,
        r.year = f1.year

    RETURN d1.name AS realisateur1,
           d2.name AS realisateur2,
           g.name AS genre,
           f1.year AS annee

    
    """

    return run_query(query)



# 30. Collaborations fréquentes entre acteurs
#     et réalisateurs + analyse du succès


def q30_top_actor_director_collaborations():

    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_REALISE]-(d:Director)

    RETURN d.name AS realisateur,
           a.name AS acteur,
           COUNT(f) AS collaborations,
           AVG(f.Revenue) AS revenu_moyen,
           AVG(f.Votes) AS votes_moyens

    ORDER BY collaborations DESC
    
    """

    return run_query(query)