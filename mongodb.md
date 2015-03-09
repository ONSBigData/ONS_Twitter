# Instructions for MongoDB servers

### Start address_base server

### Inserting address_base
* run `1.1_ImprtAddressBase.py`
* go into mongo with `mongo --host myhost`
* select database `use twitter`
* optional step: find the min/max value of coordinates:  
`db.address.aggregate({$unwind: "$coordinates"},{$group: {"_id": "min_max",  
                                "min_value": {$min: "$coordinates"},  
                                "max_value": {$max: "$coordinates"}}})`
* add geo_indexing:  
    min: 8001,  max: 1216407
    `db.address.ensureIndex({"coordinates": "2d"}, {min: -100000, max:1400000})`
    
    
    
for( var x=0; x<; x+=6 ) {
    var prefix = String.fromCharCode(x) + String.fromCharCode(y);
    db.runCommand( { split : "twitter.tweets" , middle : { user_id : x } } );
  }