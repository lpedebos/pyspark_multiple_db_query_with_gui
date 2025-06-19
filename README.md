This project is a fork from the original project "https://github.com/lpedebos/pyspark_multiple_db_query" but with a added gui to better usage on remote server.

Now all the steps can be done on the interface.

## Steps to use

1 - Download the code;\
2 - Install the dependencies (including java);\
2.1 - Export $JAVA_HOME;
3 - Run "streamlit run streamlit_app.py". This will open a local server. you can access it through the browser on port 8502;\
4 - Login with default credentials;\
5 - Use the lateral menu to go through every step.

---------------------------

## Docker option

Inside the "app" folder:\
1 - run the command "docker build -t pyspark_multiple_db ." --> this will build a docker image named "pyspark_multiple_db";\
2 - run the command "docker run -p 8501:8501 --network="host" pyspark_multiple_db" --> this will run the docker image;\
2.1 - you can access the application on your favorite browser in the address "localhost:8501";\
2.2 - the '--network="host"' parameter will make possible for your docker container to access databases that are running on you localhost, but not inside the container. It's noit mandatory and, perhaps, you will need to remove it.

---------------------------

## To do

1 - add other databases support;\
2 - add database configuration screen; ✔️ \
3 - add multi-thread for database queries (right now the query on databases is sequential).
