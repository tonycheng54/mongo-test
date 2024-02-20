docker-compose up

REM docker rm -f mongo1
REM docker rm -f mongo2
REM docker rm -f mongoarb
REM REM docker run -d --rm --name mongo1   -p 27017:27017 --volume D:\Project_Code\agr\mongo_python\env_start\config\mongo1\mongod.conf:/etc/mongod.conf mongo:dep bash -c 'mongod -f /etc/mongod.config'
REM REM docker run -d --rm --name mongo2   -p 27018:27017 --volume D:\Project_Code\agr\mongo_python\env_start\config\mongo2\mongod.conf:/etc/mongod.conf mongo:dep
REM REM docker run -d --rm --name mongoarb -p 27019:27017 --volume D:\Project_Code\agr\mongo_python\env_start\config\mongoarb\mongod.conf:/etc/mongod.conf mongo:dep

REM REM { _id: "rs0", members: [ {_id: 0, host: "172.17.0.2:27017"}, {_id: 1, host: "172.17.0.3:27017"}]}

REM docker run -it --rm --name mongo1  -p 27017:27017 --network bridge mongo:dep /usr/bin/mongod --replSet rs0
REM docker run -d --rm --name mongo2   -p 27018:27017 --network bridge --replSetName='rs0' mongo:dep
REM docker run -d --rm --name mongoarb -p 27019:27017 --network bridge --replSetName='rs0' mongo:dep
REM pause

REM docker run -d --rm --name mongo1   -p 27017:27017 --volume D:\Project_Code\agr\mongo_python\env_start\config\mongo1\mongod.conf:/etc/mongod.conf mongo:dep "mongod -f /etc/mongod.conf"
REM docker run -it --rm --name mongo1   -p 27017:27017 --volume D:\Project_Code\agr\mongo_python\env_start\config\mongo1\mongod.conf:/etc/mongod.conf mongo:dep bash "mongod -f /etc/mongod.conf"