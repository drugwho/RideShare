For Users:
	- Build docker-compose.yml
	- Bring up docker-compose.yml

For Rides:
    - Build docker-compose.yml
    - Bring up docker-compose.yml

For Orchestrator:
	- Navigate to proj_mk_2
	- Remove database.db (if it doesn't exist, skip this step)
	- Run user_db_stuff.py
	- Run ride_db_stuff.py
	- Build docker-compose.yml
	- Bring up docker-compose.yml
	  (Occasionally, an error shows up, saying port 5672 is 
	   already in use. In that case, run the following:
        	  netstat -pna | grep 5672
		  sudo kill process_id_from_previous_step)
