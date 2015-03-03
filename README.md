# ONS Twitter Pilot - BigData team #


### What is this repository for? ###

* Quick summary
    This repository holds all the Python code that is used by the ONS to analyse geo-located twitter data.
* Version


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

### Who do I talk to? ###

* Repo admin: Bence Komarniczky | bence.komarniczky@ons.gov.uk or niczky12@gmail.com
* You can also contact: Ben Clapperton | ben.clapperton@ons.gov.uk
* Or someone at the BigData team: onsbigdata@gmail.com