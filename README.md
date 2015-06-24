# ONS Twitter Pilot - BigData team #


### What is this repository for? ###

#### Quick summary
This repository holds all the Python code that is used by the ONS to analyse geo-located twitter data.
The code is written in Python 3 and relies heavily on pymongo, GDAL and joblib libraries to run efficiently.
First the csv file of tweets are imported into a mongodb database, while cleaning and pre-processing is carried
out. This includes a closest address lookup from another mongodb database, that was built using the address register.
A naive DBScan algorithm is then carried out on the dataset to find locations for users with dense usage patterns.
These could indicate home, work or other regular places. We're most interested in the dominant clusters of users
which are defined as the most populous cluster that has a residential address.
#### Structure
* The main scripts are located in the root library and are numbered 1.1,1.2 etc. These scripts run the functions located
  in the `ons_twitter` folder.
* There are separate files for data formats, data importing and clustering.
* The `in_development` folder holds small ad-hoc scripts that were used to extract information from the clustered
  dataset, such as daily volumes, location statistics and usage distributions.
    

### How do I get set up? ###

* Summary of set up
    The code is written using Python 3.4 and relies on a MongoDB server to hold the data after the initial import.
* Configuration

* Dependencies
    The following list of non-standard Python modules are used:
        - Joblib
        - pymongo
        - GDAL
* Database configuration
    ...
* How to run tests
    ...
* Deployment instructions
    - How to install GDAL on windows:
    Download GDAL from [Unofficial Windows Libraries](http://www.lfd.uci.edu/~gohlke/pythonlibs/#gdal)  
    Then add pip to Windows PATH by doing Win+Pause -> Advanced System Settings  
     -> Environment Variables... -> PATH (edit) -> add ";C:\Python34\Scripts"
     Then from the terminal (Win+R -> cmd) run "pip install package_name.whl"

### Contribution guidelines ###
...
* Writing tests
* Code review
* Other guidelines

### Contributors
* Ben Clapperton
* Bence Komarniczky
* Nigel Swier

### Who do I talk to? ###

* Project originator: Bence Komarniczky |  niczky12@gmail.com
* Project lead: Nigel Swier | nigel.swier@ons.gov.uk
* Or someone at the BigData team: onsbigdata@gmail.com