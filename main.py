
import json
# import numpy
# import scipy
# import pandas

from sqlalchemy import create_engine, Table, Column, MetaData, select, text, func
from sqlalchemy import Date, Integer, String, ForeignKey
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
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
    # metadata = MetaData(engine)
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
'''
class Artists(Base):
    """
    Artists represents 'artist' table
    """
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
'''
# CODE HERE AND MODIFY ABOVE IF NEEDED

# automap all existing db tables
Base = automap_base(Base)
Base.prepare(engine, reflect=True)

# some more convenient class names than Base.classes.*
Artists = Base.classes.Artist
Albums = Base.classes.Album
Customers = Base.classes.Customer
Employees = Base.classes.Employee
Invoices = Base.classes.Invoice
InvoiceLines = Base.classes.InvoiceLine
Tracks = Base.classes.Track
Playlists = Base.classes.Playlist

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


# count_mapped_objects(Artists, verbose=True, limit=2)
# count_mapped_objects(Albums)
# count_mapped_objects(Customers)

# CODE HERE
# ignore warning 'Dialect sqlite+pysqlite doesn't support Decimal objects...'
# that's okay for our purposes.
import warnings
warnings.filterwarnings('ignore')
for cls in Base.classes:
    count_mapped_objects(cls)
warnings.filterwarnings('default')

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


# check_count('artist')
# check_count('album')
# check_count('customer')

# CODE HERE
for cls in Base.classes:
    table_name = cls.__table__.name
    check_count(table_name)

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
    query = "select artist.name, count(*) from artist " \
            "inner join album on album.artistid = artist.artistid " \
            "group by artist.name " \
            "order by count(*) desc limit 5 "

    if query:
        result = session.execute(text(query))
    else:
        result = []

    print("> Top 5 artists with most albums (SQL)")
    # PRINT RESULTS
    for row in result:
        print(row)


def task_3_python_version():
    artists = session.query(Artists).all()  # all artists
    # albums = session.query(Albums).all()  # not needed, albums loaded via artist relationship

    print("> Top 5 artists with most albums (Python)")
    # CODE HERE
    lst = []  # list of tuples: (artist_name, album_count)
    for artist in artists:
        lst.append((artist.Name, len(artist.album_collection)))
    lst.sort(reverse=True, key=lambda t: t[1])

    # PRINT RESULTS
    for i in range(5):
        print(lst[i])


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
    _countries = {}  # dict of country_name : Country instance

    # override __new__ to return an existing Country instance if available
    # (to avoid duplicates)
    def __new__(cls, country):
        if country in Country._countries:
            return Country._countries[country]
        else:
            return super(Country, cls).__new__(cls)

    def __init__(self, country):
        self.name = country
        if country not in Country._countries:
            Country._countries[country] = self


class State(object):
    """
  name      -> Name of state
  country   -> Country object
  """
    # CODE HERE
    _states = {}  # dict of state_name : State instance

    # override __new__ to return an existing State instance if available
    # (to avoid duplicates)
    def __new__(cls, state, country):
        if state in State._states:
            return State._states[state]
        else:
            return super(State, cls).__new__(cls)

    def __init__(self, state, country):
        self.name = state
        if state not in State._states:
            State._states[state] = self
        self.country = Country(country)


class Locality(object):
    """
  name      -> Name of locality
  zip_code  -> Zip code
  state     -> State object
  """
    # CODE HERE
    _localities = {}  # dict of locality_name : Locality instance

    # override __new__ to return an existing Locality instance if available
    # (to avoid duplicates)
    def __new__(cls, city, zip_code, state, country):
        if city in Locality._localities:
            return Locality._localities[city]
        else:
            return super(Locality, cls).__new__(cls)

    def __init__(self, city, zip_code, state, country):
        self.name = city
        if city not in Locality._localities:
            Locality._localities[city] = self
        self.zip_code = zip_code
        self.state = State(state, country)


class Address(object):
    """
  name      -> Street address
  locality  -> Locality object
  """

    # CODE HERE
    def __init__(self, street, city, zip_code, state, country):
        self.name = street
        self.locality = Locality(city, zip_code, state, country)


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
missing_addresses = []


# CODE HERE

def extract_addresses_from_customers(all_addresses, missing_addresses):
    customers = session.query(Customers).all()  # all customers
    for row in customers:
        missing_data_flag = True if (None in (row.Address,
                                              row.City,
                                              row.PostalCode,
                                              row.State,
                                              row.Country)) else False
        addr = Address(row.Address, row.City, row.PostalCode, row.State, row.Country)
        all_addresses.append(addr)
        if missing_data_flag: missing_addresses.append(addr)
    return ((all_addresses, missing_addresses))


def extract_addresses_from_invoices(all_addresses, missing_addresses):
    invoices = session.query(Invoices).all()  # all invoices
    for row in invoices:
        missing_data_flag = True if (None in (row.BillingAddress,
                                              row.BillingCity,
                                              row.BillingPostalCode,
                                              row.BillingState,
                                              row.BillingCountry)) else False
        addr = Address(row.BillingAddress,
                       row.BillingCity,
                       row.BillingPostalCode,
                       row.BillingState,
                       row.BillingCountry)
        all_addresses.append(addr)
        if missing_data_flag: missing_addresses.append(addr)
    return ((all_addresses, missing_addresses))

all_addresses, missing_addresses = extract_addresses_from_customers(all_addresses, missing_addresses)
all_addresses, missing_addresses = extract_addresses_from_invoices(all_addresses, missing_addresses)

print("all_addresses length is: %d" % len(all_addresses))

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

# missing_addresses = [] moved to above & populated during task 4

# CODE HERE (see task 4)
print("missing_addresses length is %d" % len(missing_addresses))

# ########################################################################
# ################################ TASK SIX ##############################
# ########################################################################

print("\n\t########### \n\t# TASK 6  #\n\t###########\n")
"""
Task 6:
Similar to Task 3, using raw sql queries and sqlalchemy, create two function that obtain the most (top 5) succesful albums in terms of highest sales. 
"""


def task_6_sql_version():
    query = "select a.title, sum(i.unitprice) from InvoiceLine i " \
            "inner join Track t on t.trackid = i.trackid " \
            "inner join Album a on a.albumid = t.albumid " \
            "group by a.title " \
            "order by sum(i.unitprice) desc limit 5 "

    if query:
        result = session.execute(text(query))
    else:
        result = []

    print("> Top 5 albums with most sales (SQL)")
    # PRINT RESULTS
    for row in result:
        print(row[0], "%0.2f" % row[1])


def task_6_python_version():
    albums = session.query(Albums).all()  # all albums

    print("> Top 5 albums with most sales (Python)")
    # CODE HERE
    album_sales = []
    for album in albums:
        sales = 0.0
        album_tracks = album.track_collection
        for track in album_tracks:
            for item in track.invoiceline_collection:
                sales += float(item.UnitPrice)
        album_sales.append((album.Title, sales))
    album_sales.sort(reverse=True, key=lambda t: t[1])

    # PRINT RESULTS
    for i in range(5):
        print(album_sales[i][0], "%0.2f" % album_sales[i][1])


task_6_sql_version()
task_6_python_version()

# CODE HERE

# ########################################################################
# ################################ TASK SEVEN ############################
# ########################################################################

print("\n\t########### \n\t# TASK 7  #\n\t###########\n")
"""
Task 7:
If possible, create a dictionary (hashmap) of artists with their tracks and number of sales count.
"""
# artist_top_tracks = {}

# e.g.
'''
artist_top_tracks['Tan Vampire'] = {}               # adds tan vampires 
artist_top_tracks['Tan Vampire']['Digital Rot'] = 5 # adds song and sets count
'''


# CODE HERE
def build_artist_top_tracks_dict():
    artists = session.query(Artists).all()  # all artists & relationships
    artist_top_tracks = {}
    for artist in artists:
        artist_top_tracks[artist.Name] = {}
        for album in artist.album_collection:
            for track in album.track_collection:
                artist_top_tracks[artist.Name][track.Name] = len(track.invoiceline_collection)
    return artist_top_tracks


artist_top_tracks = build_artist_top_tracks_dict()
print("number of items in artist_top_tracks is %d" % len(artist_top_tracks))
print("the first 5 items are:")
for i, (k, v) in enumerate(artist_top_tracks.items()):
    if i > 4: break
    print(k, '\n', v, '\n')

########################################################################
# ################################ TASK EIGHT ############################
# ########################################################################

print("\n\t########### \n\t# TASK 8  #\n\t###########\n")
"""
Task 8:
Explore PlaylistTrack many-to-many relationship by counting tracks per playlist.
"""
# automap didn't create an object for the PlaylistTrack many-to-many table.
# This is per sqlalchemy doc. so,  how are tracks in a playlist accessed?
# or playlists including a given track? try using the relationship created by
# automap to count the tracks in each playlist...

# CODE HERE
print("Explore PlaylistTrack many-to-many relationship by counting tracks per playlist.")
print('> The first five playlists & their track counts.')
playlists = session.query(Playlists).all()
for i, playlist in enumerate(playlists):
    if i > 4: break
    print('playlist ', playlist.Name, ' has ', len(playlist.track_collection), ' tracks.')

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
As a SQLAlchemy rookie, I spent most of my time perusing the online doc.
Hopefully using reflection/automap wasn't "cheating" on task 0 !

'''
