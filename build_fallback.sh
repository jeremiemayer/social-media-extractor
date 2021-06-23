set -x # -e Stop if we had an error / -x Print the commads as you run them

LIVE_VERSION_TAG=latest
FALLBACK_VERSION_TAG=previous
IMAGE_NAME=echo

export DOCKER_IMAGE_VERSION=${FALLBACK_VERSION_TAG}

docker-compose up -d



