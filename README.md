**Organiation of the project:**

* src/
  * python code for the expansion strategies, Similarity Metrics, input & output of the databases etc.
* Evaluation/Evaluation.db
  * Database for results and setup parameters  
* out/DoopProgramAnalysis
  * contains the computed mappings for a database pair, the renamed databases,
      the merged database, the computed analyses results, and their unravelling
  * files are organised by db_config_id (for each database-pair) and 
  mapping_id (a unique key for a set of parameters found in the Evaluation.db)

* Evaluation/
  * contains the Run-Setups to generate the results for each Experiment 
  * also contains functionalities for plotting and analysing the data

* Datalog-Programs/
  * contains the merged & not merged Datalog programs for the Control Flow Graph analysis (CFG) and 
  the Pointer Analysis (PA) 
  * separate: to compute on one database
  * merged: to compute on the merged database with one extra column of identifiers


Utilisation:
* Use your own file structure in Python/Libraries/PathLib.py
* If you want to generate own Databases from jars: install Doop (https://github.com/plast-lab/doop)
* Install the Nemo reasoning language (https://github.com/knowsys/nemo/) for the program analyses
