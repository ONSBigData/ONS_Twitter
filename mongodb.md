# Instructions for MongoDB servers

### Start address_base server

### Inserting address_base
* run `1.1_ImprtAddressBase.py`
* go into mongo with `mongo --host myhost`
* select database `use twitter`
* optional step: find the min/max value of coordinates:  
`db.address.aggregate({$group: {"_id": "min_max",  
                                "min_value": {$min: "$coordinates"},  
                                "max_value": {$max: "$coordinates"}}})`
* add geo_indexing:  
    