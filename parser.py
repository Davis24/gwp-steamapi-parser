# standard library imports
import csv
import datetime as dt
import json
import os
import statistics
import time
import pprint
import re
from shutil import copyfile

# third-party imports
import numpy as np
import pandas as pd
import requests
import mysql.connector
from getpass import getpass
from mysql.connector import connect, Error

from bs4 import BeautifulSoup



#Setup Variables
game_id_list = [line.rstrip('\n') for line in open('curator_game_list.txt')]

# Back up file on each run
timestr = time.strftime("%Y%m%d-%H%M%S")
copyfile('curator_game_list.txt', (str(timestr) + '_curator_game_list'))

game_id_list = list(map(int, game_id_list))

curator_base_url = "https://store.steampowered.com/curator/" 

# Curators Lists
#
# 6864182-Hella-Yuri
# 25144145-Gay-Gotta-Game
# 28742289-Gay-Interest-Gaming


curator = "28742289-Gay-Interest-Gaming" #TODO: Read this from file


game_base_url = "https://store.steampowered.com/api/appdetails?appids="

new_games = {}
  
def append_curator_id(id):
    f = open('curator_game_list.txt', "a")
    f.write(f"{id}\n")
    f.close()

# Function to convert   
def listToString(s):  
    
    # initialize an empty string 
    str1 = " " 
    str1 = ', '.join(s)
    # return string   
    return (str1) 
        
 
url = curator_base_url + curator + "/ajaxgetfilteredrecommendations/?query&start=1&count=1"

print("Curator URL:" + url)

response = requests.get(url)

print(response)

json_response = response.json()
total_game_count = json_response['total_count']   
start_game_count = 1


## Loop through all the games within the curator
## Assign new games to a dictionary 
for x in range(start_game_count, total_game_count):
    url = curator_base_url + curator + "/ajaxgetfilteredrecommendations/?query&start=" + str(x) + "&count=1"

    response = requests.get(url)
    json_response = response.json()
    json_data = json_response['results_html']
    soup = BeautifulSoup(json_data, 'html.parser')  

    attributes_dict = soup.find('a').attrs
    divs_dict = soup.select('div.recommendation_desc')

    if int(attributes_dict['data-ds-appid']) not in game_id_list:
        append_curator_id(attributes_dict['data-ds-appid'])

        new_games[int(attributes_dict['data-ds-appid'])] = {}
        new_games[int(attributes_dict['data-ds-appid'])]['curator_description'] = str(divs_dict[0])
    else:
        print("Skipping: ID already within list.")

    time.sleep(5)

print("Beginning to parse raw data into hash.")
## Loop through the new games and get the information
for key in new_games:
    url = game_base_url + str(key)
    print(url)
    response = requests.get(url)
    json_response = response.json()
    json_data = json_response[str(key)]['data']
    #print(json_data)

    new_games[key]['title'] = json_data['name']
    new_games[key]['slug'] = re.sub(r'\W+', '-', new_games[key]['title'])
    new_games[key]['product_description'] = json_data['detailed_description']
    new_games[key]['developers'] = json_data['developers']
    new_games[key]['publishers'] = json_data['publishers'] 
    new_games[key]['platforms'] =  json_data['platforms']
    new_games[key]['categories'] =  json_data['categories']
    new_games[key]['series_id'] = 1
    new_games[key]['genres'] = None
    if('genres' in json_data.keys()):
        new_games[key]['genres'] =  json_data['genres']
    

    new_games[key]['release_date'] =  json_data['release_date']['date']
    new_games[key]['release_date'] = "test"
    try:
        new_games[key]['release_date'] = dt.datetime.strptime(new_games[key]['release_date'], '%b %d, %Y').strftime('%Y/%m/%d')
    except(ValueError, TypeError):
        new_games[key]['release_date'] = "1800/1/1"

    

    new_games[key]['full_list_of_tags'] = [item['description'] for item in new_games[key]['categories']]
    if('genres' in json_data.keys()):
        new_games[key]['full_list_of_tags'].extend(item['description'] for item in new_games[key]['genres'])
    new_games[key]['full_list_of_tags'] = listToString(new_games[key]['full_list_of_tags'])

    time.sleep(5)

pprint.pprint(new_games)

##Loop through the final dictionary format the data and insert it into DB

try:
    with connect(
        host="192.168.10.10",
        user=input("Enter username: "),
        password=getpass("Enter password: "),
        database="gamewithprideadmin"
    ) as connection:
        print("Connection to DB Made.")
        cursor = connection.cursor()

        """ sql_select_Query = "select * from games"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        print("Total number of rows in games is: ", cursor.rowcount) """

        #WorkFlow
        # Create Game
        # Iterate over Developers and Publishers, if they exist in Developers or Publishers get ID and then insert.
        # If they don't exist create them -> get ID and then create entry on joint table.


        #Create Game
        for key in new_games:
            sql_insert_query = "INSERT INTO games (title, slug, product_description, lgbt_description, series_id, rating, release_date, ready_to_publish, published_date, api_tags) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            #print(type (new_games[key]['title']))
            #print(type(new_games[key]['slug']))
            #print(type(new_games[key]['product_description']))
            #print(type(new_games[key]['curator_description']))
            #print(type(new_games[key]['release_date']))
            #print(type(new_games[key]['full_list_of_tags']))
            
            values = (new_games[key]['title'], new_games[key]['slug'], new_games[key]['product_description'], new_games[key]['curator_description'], 1, 'not rated', new_games[key]['release_date'], False, None, new_games[key]['full_list_of_tags'])
            cursor.execute(sql_insert_query, values)
            connection.commit()

            print(new_games[key]['title'] + "has been successfully inserted.")

            game_id = cursor.lastrowid

            ####
            #
            # Inserting DEVELOPERS from game into database.
            #
            ####
            for name in new_games[key]['developers']:
                #Check if developer exists
                sql_select_query = "SELECT * FROM developers WHERE name LIKE %s"
                cursor.execute(sql_select_query, (name,))
                results = cursor.fetchone()

                developer_id = 0


                if cursor.rowcount < 0:
                    print("New developer found, adding to DB.")
                    sql_insert_query = "INSERT INTO developers (name) VALUES (%s)"
                    cursor.execute(sql_insert_query, (name,))
                    connection.commit()

                    developer_id = cursor.lastrowid
                else:
                    print("Existing developer found.")
                    developer_id = results[0]

                #Insert into join table
                sql_insert_query = "INSERT INTO developer_game (developer_id, game_id) VALUES(%s, %s)"
                values = (developer_id, game_id)
                cursor.execute(sql_insert_query, values)
                connection.commit()

                print("Successfully inserted developer_game into DB.")

            ####
            #
            # Inserting PUBLISHERS from game into database.
            #
            ####
            for name in new_games[key]['publishers']: 
                sql_select_query = "SELECT id FROM publishers WHERE name=%s"
                cursor.execute(sql_select_query, (name,))
                results = cursor.fetchone()

                publisher_id = 0

                if cursor.rowcount < 0:
                    print("New publisher found, adding to DB.")
                    sql_insert_query = "INSERT into publishers (name) VALUES (%s)"
                    cursor.execute(sql_insert_query, (name,))
                    connection.commit()

                    publisher_id = cursor.lastrowid
                else:
                    print("Existing publisher found.")
                    publisher_id = results[0]

                #Insert into join table
                sql_insert_query = "INSERT INTO game_publisher (game_id, publisher_id) VALUES(%s, %s)"
                values = (game_id, publisher_id)
                cursor.execute(sql_insert_query, values)
                connection.commit()

                print("Successfully inserted publisher into DB.")
                print("----")
    
except Error as e:
    print(e)  
finally:
    if (connection.is_connected()):
        connection.close()
        cursor.close()
        print("MySQL connection is closed") 


def get_request(url, parameters=None):
    """Return json-formatted response of a get request using optional parameters.
    
    Parameters
    ----------
    url : string
    parameters : {'parameter': 'value'}
        parameters to pass as part of get request
    
    Returns
    -------
    json_data
        json-formatted response (dict-like)
    """

