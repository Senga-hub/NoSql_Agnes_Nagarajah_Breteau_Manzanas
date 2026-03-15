import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# MongoDB 
from queries import (
    get_all_movies,
    q1_year_with_most_movies,
    q2_count_movies_after_1999,
    q3_average_votes_2007,
    q4_movies_per_year,
    q5_all_genres,
    q6_movie_with_highest_revenue,
    q7_directors_more_than_5_movies,
    q8_top_genre_by_avg_revenue,
    q9_top_3_movies_by_decade,
    q10_longest_movie_by_genre,
    q11_create_view_high_score_high_revenue,
    q12_runtime_revenue_correlation,
    q13_avg_runtime_by_decade
)

# Neo4j 
from neo4j_queries import *
from transversal_queries import *

st.set_page_config(page_title="Projet NoSQL", layout="wide")
st.title("Projet NoSQL - MongoDB & Neo4j")


def clean_dataframe(df):
    df = df.copy()

    numeric_columns = [
        "year",
        "Votes",
        "Metascore",
        "Runtime (Minutes)",
        "Revenue (Millions)",
        "count",
        "avg_runtime",
        "avg_revenue",
        "runtime",
        "nb_films",
        "revenu_total",
        "moyenne_votes",
        "nb_acteurs_distincts",
        "nb_films_connectes",
        "nb_realisateurs",
        "score_genres_communs",
        "genres_communs",
        "longueur_chemin"
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df



# Sidebar Navigation


st.sidebar.title("Navigation")

# Choix de la catégorie
base_choice = st.sidebar.selectbox(
    "Choisir une catégorie",
    [
        "MongoDB",
        "Neo4j",
        "Analyse Transversale"
    ]
)


# MongoDB


if base_choice == "MongoDB":

    question_choice = st.sidebar.selectbox(
        "Questions MongoDB",
        [
            "Accueil",
            "Question 1",
            "Question 2",
            "Question 3",
            "Question 4",
            "Question 5",
            "Question 6",
            "Question 7",
            "Question 8",
            "Question 9",
            "Question 10",
            "Question 11",
            "Question 12",
            "Question 13"
        ]
    )


# Neo4j


elif base_choice == "Neo4j":

    question_choice = st.sidebar.selectbox(
        "Questions Neo4j",
        [
            "Question 14",
            "Question 15",
            "Question 16",
            "Question 17",
            "Question 18",
            "Question 19",
            "Question 20",
            "Question 21",
            "Question 22",
            "Question 23",
            "Question 24",
            "Question 25",
            "Question 26"
        ]
    )


# Analyse Transversale


elif base_choice == "Analyse Transversale":

    question_choice = st.sidebar.selectbox(
        "Questions Transversales",
        [
            "Question 27",
            "Question 28",
            "Question 29",
            "Question 30"
        ]
    )

if base_choice == "MongoDB":

    if question_choice == "Accueil":
        st.subheader("Aperçu des films MongoDB")
        data = get_all_movies(30)
        df = pd.DataFrame(data)
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 1":
        st.subheader("1. Année avec le plus de films")
        result = q1_year_with_most_movies()
        if result:
            st.success(f"Année : {result[0]['_id']} | Films : {result[0]['count']}")

    elif question_choice == "Question 2":
        st.subheader("2. Nombre de films après 1999")
        result = q2_count_movies_after_1999()
        st.metric("Nombre de films", result)

    elif question_choice == "Question 3":
        st.subheader("3. Moyenne des votes en 2007")
        result = q3_average_votes_2007()
        if result:
            st.metric("Moyenne des votes", round(result[0]["avg_votes"], 2))

    elif question_choice == "Question 4":
        st.subheader("4. Histogramme du nombre de films par année")
        result = q4_movies_per_year()
        df = pd.DataFrame(result)

        if not df.empty:
            df = df.rename(columns={"_id": "year"})
            df = clean_dataframe(df)
            st.dataframe(df, width="stretch")

            fig, ax = plt.subplots()
            ax.bar(df["year"], df["count"])
            ax.set_xlabel("Année")
            ax.set_ylabel("Nombre de films")
            st.pyplot(fig)
        else:
            st.warning("Aucune donnée.")

    elif question_choice == "Question 5":
        st.subheader("5. Genres disponibles")
        result = q5_all_genres()
        st.write(result)

    elif question_choice == "Question 6":
        st.subheader("6. Film ayant généré le plus de revenus")
        result = q6_movie_with_highest_revenue()
        if result:
            film = result[0]
            st.success(
                f"{film['title']} ({film['year']}) - {film['Revenue (Millions)']} millions"
            )
        else:
            st.warning("Aucun film avec un revenu valide.")

    elif question_choice == "Question 7":
        st.subheader("7. Réalisateurs avec plus de 5 films")
        result = q7_directors_more_than_5_movies()
        df = pd.DataFrame(result).rename(columns={"_id": "Director"})
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 8":
        st.subheader("8. Genre rapportant le plus")
        result = q8_top_genre_by_avg_revenue()
        if result:
            st.success(
                f"{result[0]['_id']} - revenu moyen {round(result[0]['avg_revenue'], 2)} millions"
            )

    elif question_choice == "Question 9":
        st.subheader("9. Top 3 films par décennie")
        result = q9_top_3_movies_by_decade()
        df = pd.DataFrame(result)
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 10":
        st.subheader("10. Film le plus long par genre")
        result = q10_longest_movie_by_genre()
        df = pd.DataFrame(result)
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 11":
        st.subheader("11. Vue MongoDB")
        result = q11_create_view_high_score_high_revenue()
        df = pd.DataFrame(result)
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 12":
        st.subheader("12. Corrélation durée / revenu")
        result = q12_runtime_revenue_correlation()
        if result["correlation"] is not None:
            st.metric("Corrélation", round(result["correlation"], 4))
        st.write(result["interpretation"])
        st.write(f"Taille de l'échantillon : {result['sample_size']}")

    elif question_choice == "Question 13":
        st.subheader("13. Évolution durée moyenne")
        result = q13_avg_runtime_by_decade()
        df = pd.DataFrame(result)
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

        if not df.empty:
            fig, ax = plt.subplots()
            ax.plot(df["decade"], df["avg_runtime"], marker="o")
            ax.set_xlabel("Décennie")
            ax.set_ylabel("Durée moyenne")
            st.pyplot(fig)


# Neo4j


elif base_choice == "Neo4j":

    if question_choice == "Question 14":
        st.subheader("14. Acteur avec le plus de films")
        df = pd.DataFrame(q14_actor_most_films())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 15":
        st.subheader("15. Acteurs ayant joué avec Anne Hathaway")
        df = pd.DataFrame(q15_actors_with_anne_hathaway())
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 16":
        st.subheader("16. Acteur avec le plus de revenus cumulés")
        df = pd.DataFrame(q16_actor_highest_total_revenue())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 17":
        st.subheader("17. Moyenne des votes")
        df = pd.DataFrame(q17_average_votes())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 18":
        st.subheader("18. Genre le plus représenté")
        df = pd.DataFrame(q18_most_represented_genre())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 19":
        st.subheader("19. Films joués par les acteurs ayant joué avec vous")
        your_name = st.text_input("Votre nom dans Neo4j", value="Paul B")
        df = pd.DataFrame(q19_movies_of_coactors(your_name))
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 20":
        st.subheader("20. Réalisateur avec le plus d'acteurs distincts")
        df = pd.DataFrame(q20_director_most_distinct_actors())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 21":
        st.subheader("21. Films les plus connectés")
        df = pd.DataFrame(q21_most_connected_films())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 22":
        st.subheader("22. Top 5 acteurs ayant travaillé avec le plus de réalisateurs")
        df = pd.DataFrame(q22_top_5_actors_most_directors())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 23":
        st.subheader("23. Recommandation de films")
        actor_name = st.text_input("Nom de l'acteur", value="Paul B")
        df = pd.DataFrame(q23_recommend_film_to_actor(actor_name))
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 24":
        st.subheader("24. Influence entre réalisateurs")
        if st.button("Créer / mettre à jour les relations INFLUENCE_PAR"):
            q24_create_director_influence()
            st.success("Relations créées ou mises à jour.")
        df = pd.DataFrame(q24_show_director_influence())
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 25":
        st.subheader("25. Plus court chemin entre deux acteurs")
        actor1 = st.text_input("Acteur 1", value="Tom Hanks")
        actor2 = st.text_input("Acteur 2", value="Scarlett Johansson")
        df = pd.DataFrame(q25_shortest_path_length(actor1, actor2))
        df = clean_dataframe(df)
        st.dataframe(df, width="stretch")

    elif question_choice == "Question 26":

        st.subheader("26. Communautés d'acteurs")

        if st.button("Lancer l'analyse Louvain"):

         try:
            result = q26_run_full_analysis()
            df = pd.DataFrame(result)

            st.success("Analyse terminée avec succès.")
            st.dataframe(df, width="stretch")

         except Exception as e:
            st.error("Erreur pendant l'analyse des communautés.")
            st.code(str(e))



elif base_choice == "Analyse Transversale":

    if question_choice == "Question 27":

        df = pd.DataFrame(q27_similar_genre_different_director())

        st.dataframe(df, width="stretch")


    elif question_choice == "Question 28":

        actor = st.text_input("Acteur", "Leonardo DiCaprio")

        df = pd.DataFrame(q28_recommend_films(actor))

        st.dataframe(df, width="stretch")


    elif question_choice == "Question 29":

        if st.button("Créer les relations de concurrence"):

            df = pd.DataFrame(q29_create_director_competition())

            st.dataframe(df, width="stretch")


    elif question_choice == "Question 30":

        df = pd.DataFrame(q30_top_actor_director_collaborations())

        st.dataframe(df, width="stretch")