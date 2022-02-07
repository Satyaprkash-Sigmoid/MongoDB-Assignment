from pymongo import MongoClient
from bson import ObjectId
import json

try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error in Connect")

db = connection['Mflix']
comments = db['comments']
users = db['users']
movies = db['movies']
theaters = db['theaters']
sessions = db['sessions']


# Question 1 : Find top 10 users who made the maximum number of comments
def top_ten_user_max_comment(n):
    output = comments.aggregate([{"$group": {"_id": {"name": "$name"}, "total_comments": {"$sum": 1}}},
                                 {"$sort": {"total_comments": -1}},
                                 {"$limit": n}])
    for doc in output:
        print(doc)


# top_ten_user_max_comment(10)  # Calling Function to print top 10 user who made maximum Comments

#  Question 2 : Find top 10 movies with most comments
def top_ten_movies_having_most_comment(n):
    output = comments.aggregate([{"$group": {"_id": {"name": "$movie_id"}, "total_comments": {"$sum": 1}}},
                                 {"$sort": {"total_comments": -1}},
                                 {"$limit": n}])
    for doc in output:
        print(doc)


# top_ten_movies_having_most_comment(10)  # calling Function to print top 10 movies having most comments


# Question - b.1 - Find top `N` movies - with the highest IMDB rating
def n_movies_highest_imdb_rating(n):
    output = movies.aggregate([{"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
                               {"$sort": {"imdb.rating": -1}}, {"$limit": n}])
    for doc in output:
        print(doc)


# n_movies_highest_imdb_rating(5)  # calling function to print highest imdb rating movies


# Question - b.1.4 with title matching a given pattern sorted by highest tomatoes ratings

def movies_with_matching_string(n, string_match):
    pipeline = [{"$addFields": {"tomatoes_Rating": "$tomatoes.viewer.rating", "result": {
        "$cond": {"if": {"$regexMatch": {"input": "$title", "regex": string_match}}, "then": "yes", "else": "no"}}}},
                {"$project": {"_id": 0, "title": 1, "tomatoes_Rating": 1, "result": 1}},
                {"$match": {"result": {"$eq": "yes"}}},
                {"$sort": {"tomatoes_Rating": -1}},
                {"$limit": n}]
    output = list(db.movies.aggregate(pipeline))
    for doc in output:
        print(doc)


# movies_with_matching_string(5, 'my')  # Invoking function


# Find top `N` actors : Question B.3.1 who starred in the maximum number of movies

def top_n_actor_starred_in_max_Movies(n):
    output = movies.aggregate([{"$unwind": "$cast"},
                               {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
                               {"$project": {"cast": 1, "count": 1}},
                               {"$sort": {"count": -1}},
                               {"$limit": n}, ])
    for doc in output:
        print(doc)


# top_n_actor_starred_in_max_Movies(6)  # function call


# Find top `N` Directors : Question B.2.1 - who created the maximum number of movies

def top_n_director_create_max_Movies(n):
    output = movies.aggregate([{"$unwind": "$directors"},
                               {"$group": {"_id": {"dir_name": "$directors"}, "Movie_count": {"$sum": 1}}},
                               {"$project": {"dir_name": 1, "Movie_count": 1}},
                               {"$sort": {"Movie_count": -1}},
                               {"$limit": n}, ])
    for doc in output:
        print(doc)


# top_n_director_create_max_Movies(8)  # calling 8 directors crated max movies


# Question : B.2.2 - who created the maximum number of movies in a given year
def top_n_director_max_movie_in_year(n, year):
    output = movies.aggregate(
        [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}}}},
         {"$unwind": "$directors"},
         {"$match": {"yr": {"$eq": year}}},
         {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"director_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


# top_n_director_max_movie_in_year(7, '1983')  # function call


# Question : B.3.2 -Actors who created the maximum number of movies in a given year
def top_n_Actor_max_movie_in_year(n, year):
    output = movies.aggregate(
        [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}}}},
         {"$unwind": "$cast"},
         {"$match": {"yr": {"$eq": year}}},
         {"$group": {"_id": {"actor_name": "$cast"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])

    for i in output:
        print(i)


# top_n_Actor_max_movie_in_year(5, "1983")  # function call


# Question :B.1.2 - Find top `N` movies - with the highest IMDB rating in a given year
def movie_with_highest_IMDB_rating_in_year(n, year):
    output = movies.aggregate(
        [{"$addFields": {"yr": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$year"}},
                         "rating": {"$getField": {"field": {"$literal": "$numberDouble"}, "input": "$imdb.rating"}}}},
         {"$match": {"yr": {"$eq": year}}},
         {"$project": {"_id": 0, "title": 1, "yr": 1, "rating": 1}},
         {"$sort": {"rating": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


# movie_with_highest_IMDB_rating_in_year(8, "1990")


# Question :B.2.3 - Find top `N` directors  - who created the maximum number of movies for a given genre
def top_n_directors_with_highest_movie_given_genre(n, genres):
    output = movies.aggregate(
        [{"$unwind": "$directors"},
         {"$match": {"genres": {"$eq": genres}}},
         {"$group": {"_id": {"director_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"director_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


# top_n_directors_with_highest_movie_given_genre(6, "Short")  # Function call


# Question :B.3.3 - Find top `N` Actor  - who created the maximum number of movies for a given genre
def top_n_actor_with_highest_movie_given_genre(n, genres):
    output = movies.aggregate(
        [{"$unwind": "$cast"},
         {"$match": {"genres": {"$eq": genres}}},
         {"$group": {"_id": {"actor_name": "$directors"}, "count": {"$sum": 1}}},
         {"$project": {"actor_name": 1, "count": 1}},
         {"$sort": {"count": -1}},
         {"$limit": n}])
    for i in output:
        print(i)


# top_n_actor_with_highest_movie_given_genre(6, "Comedy")  # function call


# Question : C.1 -theatre collection - Top 10 cities with the maximum number of theatres

def city_with_max_theaters(n):
    output = theaters.aggregate([{"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
                                 {"$project": {"location.address.city": 1, "count": 1}},
                                 {"$sort": {"count": -1}},
                                 {"$limit": n}])

    for i in output:
        print(i)


# city_with_max_theaters(10)  # Function call


# Question - C.2 theatre collection -  top 10 theatres nearby given coordinates
def theaters_near_given_coordinates(lat, lng):
    output = db.theaters.aggregate([{"$geoNear": {"near": {"type": "Point", "coordinates": [-91.24, 43.85]},
                                                  "maxDistance": 10000000, "distanceField": "distance"}},
                                    {"$project": {"location.addess.city": 1, "_id": 0, "location.geo.coordinates": 1}},
                                    {"$limit": 10}])
    for i in output:
        print(i)


# theaters_near_given_coordinates(-91.24, 43.85)  # function call


# Question 4.1 c - comments collection : iven a year find the total number of comments created each month in that year
def comment_for_each_month_for_year(year):
    output = comments.aggregate(
        [{"$project": {"_id": 0, "date": {"$toDate": {"$convert": {"input": "$date", "to": "long"}}}}},
         {"$group": {"_id": {"year": {"$year": "$date"}, "month": {"$month": "$date"}}, "total_comment": {"$sum": 1}}},
         {"$match": {"_id.year": {"$eq": year}}},
         {"$sort": {"_id.month": 1}}
         ])
    for i in output:
        print(i)


# comment_for_each_month_for_year(1978)  # function call


# Question B 4 - Find top `N` movies for each genre with the highest IMDB rating

def N_Top_movie_for_each_genre(n):
    output = movies.aggregate([{"$unwind": "$genres"}, {"$sort": {"imdb.rating": -1}},
                               {"$group": {"_id": "$genres", "title": {"$push": "$title"},
                                           "rating": {"$push": {"$getField": {"field": {"$literal": "$numberDouble"},
                                                                              "input": "$imdb.rating"}}}}},
                               {"$project": {"_id": 1, "Movies": {"$slice": ['$title', 0, n]},
                                             "ratings": {"$slice": ["$rating", 0, n]}}}])
    for i in output:
        print(i)


# N_Top_movie_for_each_genre(3)  # Function call


# Question B . 1 . 3 . with the highest IMDB rating with number of votes > 1000
def N_movies_having_votes_gt_thousand(n):
    output = db.movies.aggregate(
        [{"$addFields": {"vote": {"$getField": {"field": {"$literal": "$numberInt"}, "input": "$imdb.votes"}}}},
         {"$match": {"$expr": {"$gt": [{"$toInt": "$vote"}, 1000]}}},
         {"$sort": {"imdb.rating": -1}},
         {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "vote": 1}},
         {"$limit": n}])
    for i in output:
        print(i)


N_movies_having_votes_gt_thousand(7)  # function call
