from database import get_collection, get_neo4j_driver

collection = get_collection()
driver = get_neo4j_driver()

PROJECT_MEMBERS = [
    "Paul B",
    "Kevin N",
    "Esteban M",
    "Tytouan A"
]

CHOSEN_FILM_TITLE = "The Departed"


def to_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except:
        return None


def to_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except:
        return None


def split_csv_field(value):
    if not value or not isinstance(value, str):
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def clear_neo4j():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


def create_constraints():
    queries = [
        "CREATE CONSTRAINT film_id IF NOT EXISTS FOR (f:Film) REQUIRE f.id IS UNIQUE",
        "CREATE CONSTRAINT actor_name IF NOT EXISTS FOR (a:Actor) REQUIRE a.name IS UNIQUE",
        "CREATE CONSTRAINT director_name IF NOT EXISTS FOR (d:Director) REQUIRE d.name IS UNIQUE",
        "CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE"
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)


def import_movies():
    movies = list(collection.find({}))

    with driver.session() as session:
        for movie in movies:
            movie_id = str(movie.get("id", movie.get("_id", "")))
            title = movie.get("title")
            year = to_int(movie.get("year"))
            votes = to_int(movie.get("Votes"))
            revenue = to_float(movie.get("Revenue (Millions)"))
            rating = movie.get("rating")
            director = movie.get("Director")
            actors = split_csv_field(movie.get("Actors"))
            genres = split_csv_field(movie.get("genre"))

            # Film
            session.run("""
                MERGE (f:Film {id: $id})
                SET f.title = $title,
                    f.year = $year,
                    f.Votes = $votes,
                    f.Revenue = $revenue,
                    f.rating = $rating,
                    f.director = $director
            """, {
                "id": movie_id,
                "title": title,
                "year": year,
                "votes": votes,
                "revenue": revenue,
                "rating": rating,
                "director": director
            })

            # Directeur
            if director:
                session.run("""
                    MERGE (d:Director {name: $name})
                    WITH d
                    MATCH (f:Film {id: $film_id})
                    MERGE (d)-[:A_REALISE]->(f)
                """, {"name": director, "film_id": movie_id})

            # Acteurs
            for actor in actors:
                session.run("""
                    MERGE (a:Actor {name: $name})
                    WITH a
                    MATCH (f:Film {id: $film_id})
                    MERGE (a)-[:A_JOUE]->(f)
                """, {"name": actor, "film_id": movie_id})

            # Genres
            for genre in genres:
                session.run("""
                    MERGE (g:Genre {name: $name})
                    WITH g
                    MATCH (f:Film {id: $film_id})
                    MERGE (f)-[:A_GENRE]->(g)
                """, {"name": genre, "film_id": movie_id})


def attach_project_members():
    with driver.session() as session:
        for member in PROJECT_MEMBERS:
            session.run("""
                MERGE (a:Actor {name: $name})
            """, {"name": member})

            session.run("""
                MATCH (a:Actor {name: $name})
                MATCH (f:Film {title: $title})
                MERGE (a)-[:A_JOUE]->(f)
            """, {"name": member, "title": CHOSEN_FILM_TITLE})


if __name__ == "__main__":
    clear_neo4j()
    create_constraints()
    import_movies()
    attach_project_members()
    print("Import MongoDB -> Neo4j terminé.")