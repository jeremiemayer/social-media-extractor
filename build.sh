set -x # -e Stop if we had an error / -x Print the commads as you run them

LIVE_VERSION_TAG=latest
FALLBACK_VERSION_TAG=previous
IMAGE_NAME=social-media-extractor
IMAGE_PATH=./deploy/base_dockerfile

export DOCKER_IMAGE_VERSION=${LIVE_VERSION_TAG}

# Fetch from github
#git pull

# Use the current live version as fallback
docker rmi ${IMAGE_NAME}:${FALLBACK_VERSION_TAG}
docker tag ${IMAGE_NAME}:${LIVE_VERSION_TAG} ${IMAGE_NAME}:${FALLBACK_VERSION_TAG}

# Bulid the image
docker build -f ${IMAGE_PATH} -t ${IMAGE_NAME}:${LIVE_VERSION_TAG} .

docker-compose up -d



