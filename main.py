import json
#import numpy
#import scipy
#import pandas

from sqlalchemy import create_engine, Table, Column, MetaData, select, text, func
from sqlalchemy import Date, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import relationship, backref, mapper, sessionmaker

# ########################################################################
# ################### DOCUMENTATION LINKs ################################
# ########################################################################
"""
SQLite Docs
http://www.sqlitetutorial.net/sqlite-select/

SQLAlchemy Docs
https://docs.sqlalchemy.org/en/latest/

Python3 Docs
https://docs.python.org/3/library/index.html
"""

# ########################################################################
# ###################### DATABASE SETUP ##################################
# ########################################################################

# db path & name
dbPath = 'database.sqlite'
# creates engine, set echo to True for debug log
engine = create_engine('sqlite:///%s' % dbPath, echo=False)
Base = declarative_base(engine)


def loadSession():
    """
    Loads a sqlite database located in <dbPath>
    and returns a SQL Alchemy session
    """
    #metadata = MetaData(engine)
    metadata = Base.metadata

    Session = sessionmaker(bind=engine)
    session = Session()
    return session


# ########################################################################
# ##################### BACKGROUND CONTEXT ###############################
# ########################################################################
"""
 Sample Database "database.sqlite" information (see db_schema.png):

 employees table stores employees data such as employee id, last name, first name, etc. It also has a field named ReportsTo to specify who reports to whom.

 customers table stores customers data.

 invoices & invoice_items tables: these two tables store invoice data. The invoices table stores invoice header data and the invoice_items table stores the invoice line items data.

 artists table stores artists data. It is a simple table that contains only artist id and name.

 albums table stores data about a list of tracks. Each album belongs to one artist. However, one artist may have multiple albums.

 media_types table stores media types such as MPEG audio file, ACC audio file, etc.
 genres table stores music types such as rock, jazz, metal, etc.

 tracks table store the data of songs. Each track belongs to one album.

 playlists & playlist_track tables: playlists table store data about playlists. Each playlist contains a list of tracks. Each track may belong to multiple playlists. 
 
 The relationship between the playlists table and tracks table is many-to-many. The playlist_track table is used to reflect this relationship.
 """

# ########################################################################
# ################## MAPPED DATABASE TABLES ##############################
# ########################################################################

# Mapped Python classes to db tables (SQLAlchemy)


class Artists(Base):
    """
    Artists represents 'artist' table
    """
    __tablename__ = 'artist'
    #__table_args__ = {'autoload': True}

    ArtistId = Column(Integer, primary_key=True)
    name = Column(String)
    albums = relationship('Albums')  # custom


class Albums(Base):
    """
    Albums represents 'album' table
    """
    __tablename__ = 'album'
    #__table_args__ = {'autoload': True}

    AlbumId = Column(Integer, primary_key=True)
    Title = Column(String)
    ArtistId = Column(Integer, ForeignKey("artist.ArtistId"))


class Customers(Base):
    """
    Customers Table
    Automapped, no explicit defintion is provided as given
    """
    __tablename__ = 'Customer'
    __table_args__ = {'autoload': True}  # required when not explicitly defined


# CODE HERE AND MODIFY ABOVE IF NEEDED


# ########################################################################
# ########################### DB SESSION #################################
# ########################################################################

# Metadata in loadSession is generated from declative base that the mapped objects use.
session = loadSession()

# ########################################################################
# ########################## TASK ZERO ###################################
# ########################################################################
"""
Task 0: 
Read SQLAlchemy documenation and create python classes that are mapped to database tables (above). It is up to you whether you want to explicitly define all the columns & relationships. Artists, Albums and Customers are defined. Please note that Artists and Albums are explicitly defined with a relationship in Aritsts that points related field (reverse relationship) albums. 

"""


# ########################################################################
# ########################## TASK ONE ####################################
# ########################################################################

print("\n\t########### \n\t# TASK 1  #\n\t###########\n")
"""
Task 1: 
Verify data is correctly mapped / queried by using the count_mapped_objects
function. 
"""


def count_mapped_objects(class_name, verbose=False, limit=None):
    # get results
    result = session.query(class_name).all()
    # print count / length
    print("> Queried %s number of rows from %s" % (len(result),
                                                   str(class_name.__name__)))
    # print rows in results if verbose is true
    if verbose:
        for i, row in enumerate(result):
            fields = {}
            # get relevent fields and ignore internal fields
            for k, v in row.__dict__.items():
                if k.startswith('_'):
                    continue
                fields[k] = v
            # stop if limit is set to a number
            if limit is not None and i >= limit:
                break
            print(">>> Row", i + 1, fields)


#count_mapped_objects(Artists, verbose=True, limit=2)
#count_mapped_objects(Albums)
#count_mapped_objects(Customers)

# CODE HERE

# ########################################################################
# ############################### TASK TWO ###############################
# ########################################################################

print("\n\t########### \n\t# TASK 2  #\n\t###########\n")
"""
Task 2:
Cross check by validating with raw SQL query (see below).
"""


def check_count(table_name):
    # sql query
    query = 'SELECT Count(*) FROM %s' % table_name
    # execute query
    result = session.execute(text(query))
    # print results
    for row in result:
        print('> Counted %s number of rows in %s' % (row[0], table_name))


#check_count('artist')
#check_count('album')
#check_count('customer')

# CODE HERE

# ########################################################################
# ############################### TASK THREE #############################
# ########################################################################

print("\n\t########### \n\t# TASK 3  #\n\t###########\n")
"""
Task 3:
Count the number of albums each artist / group has released using 
the python and sqlalchemy and print the top 5 (name, number of albums)
"""


def task_3_sql_version():
    # WRITE THE QUERY
    query = ""

    if query:
        result = session.execute(text(query))
    else:
        result = []

    #print("> Top 5 artists with most albums (SQL)")
    # PRINT RESULTS


def task_3_python_version():
    artists = session.query(Artists).all()  # all artists
    albums = session.query(Albums).all()  # all albums

    #print("> Top 5 artists with most albums (Python)")
    # CODE HERE

    # PRINT RESULTS


task_3_sql_version()
task_3_python_version()

# ########################################################################
# ############################# TASK FOUR ################################
# ########################################################################

print("\n\t########### \n\t# TASK 4  #\n\t###########\n")


class Country(object):
    """
  name      -> Name of country
  """
    # CODE HERE


class State(object):
    """
  name      -> Name of state
  country   -> Country object
  """
    # CODE HERE


class Locality(object):
    """
  name      -> Name of locality
  zip_code  -> Zip code
  state     -> State object
  """
    # CODE HERE


class Address(object):
    """
  name      -> Street address
  locality  -> Locality object
  """
    # CODE HERE


"""
Task 4:
There are addresses (Address, City, State, Country, Zip) in two tables:
Customers and Invoices. Create python objects based on the address model defined above, then extract all addresses from the database, store them in the python objects, and append them to the list below. Please avoid having duplicates. E.g. Only 1 country object should exist for USA. Think about how to handle missing data.
"""

# e.g.
'''
country_usa = Country(name="USA")
state_maine = State(name="Maine", country=country_usa)
locality_portland = Locality(name="Portland", state=state_maine, zip_code='04102')
address = Address(name="1 Washington Ave", locality=locality_portland)
'''
# or
'''
locality_waterville = Locality(name="Waterville", state=state_maine, zip_code='04901') # use existing state
address = Address(name="400 Mayflower Drive", locality=locality_waterville)
'''
# or
'''
state_massachusetts = State(name="Massachusetts", country=country_usa) # use existing country
locality_boston = Locality(name="Waterville", state=state_massachusetts, zip_code='02109')
address = Address(name="10 Hannover Street", locality=locality_boston)
'''

all_addresses = []

# CODE HERE

# ########################################################################
# ################################ TASK FIVE #############################
# ########################################################################

print("\n\t########### \n\t# TASK 5  #\n\t###########\n")
"""
Task 5:
Using the addresses gathered in Task 4 or by running new queries, create a new function
that gets addresses with missing components and appends them to a list. A missing component might 
be a zip code, state, or country. Please print out the count.
"""

missing_addresses = []

# CODE HERE

# ########################################################################
# ################################ TASK SIX ##############################
# ########################################################################

print("\n\t########### \n\t# TASK 6  #\n\t###########\n")
"""
Task 6:
Similar to Task 3, using raw sql queries and sqlalchemy, create two function that obtain the most (top 5) succesful albums in terms of highest sales. 
"""

# CODE HERE

# ########################################################################
# ################################ TASK SEVEN ############################
# ########################################################################

print("\n\t########### \n\t# TASK 7  #\n\t###########\n")
"""
Task 7:
If possible, create a dictionary (hashmap) of artists with their tracks and number of sales count.
"""
artist_top_tracks = {}

# e.g.
'''
artist_top_tracks['Tan Vampire'] = {}               # adds tan vampires 
artist_top_tracks['Tan Vampire']['Digital Rot'] = 5 # adds song and sets count
'''

# CODE HERE

# ########################################################################
# ################################ TASK EIGHT ############################
# ########################################################################

print("\n\t########### \n\t# TASK 8  #\n\t###########\n")
"""
Task 8:
Do something new with SQLAlchemy or obtain some insight about the data that's interesting. 
"""

# CODE HERE


# ########################################################################
# ################################ FINAL TASK ############################
# ########################################################################

print("\n\t########### \n\t# THANKS  #\n\t###########\n")
"""
Task 9:
Add any comments below related to the project, this can include feedback, questions, or anything else. Thanks for taking the time to complete this. 
"""
'''

# COMMENTS HERE


'''
