# About

This repo was born from two motivations. 
1. To see if a proof of concept id system could be useful to our research. In particular can it be simple enough to play around with while still allowing us to test PETS ideas.
2. To see if I could actually get it to work from a techical point of view. 

# Setup

We call psql from python, therefore I assume that people have psql installed, and are able to access it etc. 
The first thing to do will be to change the parameters of the database in db_initialise.py (that is, change u (user), p (password), h (host), o (port), d (database)).

The second thing to do is run the following to set up a virtual environment.

```
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

The last thing to do is create the tables in the database. Open psql and run the create table commands in db_tables.sql 

There are three files to take note of:
1. db_initialise.py: this initialises the connection with the db
2. db_operations.py: here we define anything that interacts directly with the database
3. id_system_functionality.py: this is where the functionality is run from.

# Walkthrough

For the walk through we will only need to look at id_system_functionality.py. For ease of documentation (mainly my own ease) I have provided a walkthrough of the basic functionalities. This can be found at the bottom of id_system_functionality.py (line 325 and down). Hopefully this is all self explainatory. 

# Extras

Note that I call some jsno validators to ensure we have correct inputs and to try to keep track of some types. If you are not familiar with this you can just comment out any line that runs the validate method when you play about with things. Otherwise things are likely to break if you start adding fields to records etc.

# Next steps

As I have said, the motivations for this are broad and not strongly binding. If we think it can be extended to a fully fledged prof of concept that we can use to prototype things in then great. If not then it was an interesting day and a half for me. 



