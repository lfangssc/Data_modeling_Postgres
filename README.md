# Data_modeling_Postgres
### Summary of the project
The purpose of this project is to build a database for Sparkify to analyze the played songs. Sparkify is specially insterested in which song was played, how long it was played, and who played this song. According to this, I created a star schema with five tables, which are one fact table 'songplays' and four dimention tables 'songs', 'users', 'artists', and 'time'. These five tables are linked with foreign keys 'songs.song_id', 'users.user_id','artists.artist_id', and 'time.ts'


### How to run the Python scripts
First, set up the schema by creating tables in the database
* Go to the Untitled.ipynb and run the second cell.

Second, run the etl.py to format the data in the data folder then insert into the tables in the database
* Go to the Untitled.ipynb and run the third cell.

Finally, run the test.ipynb to verify the tables
### An explanation of the files in the repository
1. The data folder contains all the song and log data files. There are subfolders of 'log_data' and 'song_data'
2. The create_tables.py are for reseting the database. It first drop all the existing tables then create the new tables. 
3. The sql_queries.py has all the database query commands, including create tables, insert into tables and select from tables. The queries were imported by etl.py and create_tables.py
4. The test.ipynb was used to test whether the tables was successfully inserted. 
5. The etl.py and etl.ipynb are essentially the same.
6. The untitled.ipynb has three cells.
 1. The first cell is to display the data in a log_data file
 2. The second cell is to run the create_tables.py. It resets the database, i.e. drop all the existing tables and create new tables. It was used to modify the schema.
 3. The third cell is to run the elt.py. It reads data from the data folder and insert into tables in the databases.
