docker run -it --name sm-nightly-upload --env-file social-media-extractor.env -v socialmediaextractor_db-data:/usr/src/app/persistent_data sm-nightly-upload
docker rm $(docker ps -a -q -f 'name=sm-nightly-upload' -f 'exited=0')
