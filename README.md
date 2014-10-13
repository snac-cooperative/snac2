snac2
=====

Python match/merge code for SNAC

Installing Match/Merge Code
---------------------------

Once Cheshire and CheshirePy have been built, set up, and installed, and VIAF has been correctly indexed by cheshire (instructions available at the [Cheshire repository](https://github.com/snac/cheshire)), the snac match-merge code can be installed and executed.

1. Checkout the snac2 code from this repository using `git clone https://github.com/snac/snac2.git`.  All the following paths will be relative to the root directory of that repository.
2. Set up a postgres database (steps omitted).  We will assume the database `snac_test` has been set up with user `snac_user` and password `snac_password`.
3. Edit the configuration files:
	* Copy `snac2/config/db.py.tpl` to `snac2/config/db.py`.  Update `snac2/config/db.py` to include the user, database, and password as follows:  
	```
	DB_NAME = "snac_test"
	DB_USER = "snac_user"
	DB_PASS = "snac_password"
	```
	* Update `snac2/config/app.py` to include the cheshire config file, log locations, any data directories (those containing EAC-CPF xml files), and merged output directory as follows:  
	```
	VIAF_CONFIG = "/full/path/to/config.viaf"
	log = '/full/path/to/logfile.log'
	data_shortname = '/full/path/to/data/directory'
	# any number of shortname = path are allowed
	merged = '/full/path/to/merged/output/directory'
	```
4. Install the snac code using `sudo python setup.py develop`.  This will install the necessary python packages that are needed by the snac code.
	* Change permissions on the package manager file used in this step to avoid command-line warnings on future python commands.  This can be done with the command `chmod go-w ~/.python-eggs`.
5. Initialize the database using `python snac2/scripts/init_db.py`.
6. Test that everything works by running `python shell.py`, which should enter you into the following shell:  
    ```
    === Welcome to SNAC Merge Tool Shell ===
    using database postgresql+psycopg2://username:@/snac-test
    	 
    	In [1]: 
    ```
To exit, press `Control+D` and answer the prompt.  If there are no errors, you are ready to run the snac code.
7. Update the static names in the python code:
    * Edit `snac2/scripts/match.py` and change the line `viaf.config_cheshire(db="viaf")` so that `db` equals the name of your Cheshire database, aka the text inside the `FILETAG` Cheshire config.


Running Match/Merge Code
------------------------

Now you are ready to run the code.

* Loading Data
	* Refer to the `snac2/config/app.py` file for the `data_shortname` variables set above.
	* Load data into the database using `python snac2/scripts/load.py data_shortname`.  This will read through the EAC-CPF records and import them into the database.
* Matching Data to VIAF
	* Matching is performed using the command `python snac2/scripts/match.py` with the following arguments:
		* Type arguments: `-p` for Persons, `-c` for Corporate Bodies, or `-f` for Families.  Note: only one at a time should be used.
		* `-s STARTS_AT` record to start at (optional)
		* `-e ENDS_AT` record to end at (optional)
    * The following command will match all three types of records:  
        ```
        python snac2/scripts/match.py -p && python snac2/scripts/match.py -c && python snac2/scripts/match.py -f
        ```
* Merging Record Groups
    * Merging is performed in two steps
        1. Merge: this step creates the matched records from the record groups and assigns unique ARK ids.  For testing purposes, it is very important to run without the `-r` command line argument.  Without it, the script will ask for temporary ARKs.  The real merge command can be run as  
            ```
            python snac2/scripts/merge.py -r -m
            ```
        while the command that should be used for testing is  
            ```
            python snac2/scripts/merge.py -m
            ```
        2. Assemble: this step exports the matched records out to XML files.  It is run as  
            ```
            python snac2/scripts/merge.py -a
            ```
