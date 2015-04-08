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
  
  
# update system
sudo apt-get update; sudo apt-get upgrade -y;


# install mongodb
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10;
echo "deb http://repo.mongodb.org/apt/ubuntu "$(lsb_release -sc)"/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.0.list;
sudo apt-get update;
sudo apt-get install -y mongodb-org;

# create folder for logs
sudo mkdir /mongo_logs

# connect to nas
sudo apt-get install -y nfs-common;
sudo mkdir -p /nas/data; sudo mount -t nfs -o rw,nfsvers=3 192.168.0.10:/volume1/DATA /nas/data;

sudo nano /etc/fstab
insert:
	192.168.0.10:/volume1/DATA /nas/data nfs defaults 0 0
	Ctrl+O to save and then Ctrl+X to exit
sudo apt-get update

# set readahead
sudo blockdev --setra 32 /dev/vdb

# attach volumes
sudo mkdir -pv /mongovolume
cat /proc/partitions; sudo mkfs.ext4 /dev/vdb;
sudo mount /dev/vdb /mongovolume;
sudo mkdir -v /mongovolume/mongodata/db;

sudo chown -R mongodb:mongodb /mongovolume/mongodata;
sudo chown -R mongodb:mongodb /mongo_logs

sudo echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
sudo echo 'never' | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
sudo blockdev --setra 32 /dev/vdb

sudo mount /dev/vdb /mongovolume;

mongorestore --port 30001 --numInsertionWorkersPerCollection 4 /nas/data/Twitter\ Data/mongo_address/
mongorestore --port 30000 --numInsertionWorkersPerCollection 8 /nas/data/Twitter\ Data/MongoTweetsApril/