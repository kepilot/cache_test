## Docker
Si no has incluido tu usuario al grupo docker puedes añadirlo con el comando 
sudo usermod -aG docker ${USER} si no tendras que ejecutar los comando con sudo
```
 export DOCKER_BUILDKIT=1
 docker login 
 docker build . -f Dockerfile --rm -t fastapi:1.0
 docker create --name fastapi_test -p 5000:5000  -p 6379:6379 --rm -it fastapi:1.0
 docker start fastapi_test
 
```

## Test funcionality
curl --location 'http://localhost:5000/test/async/aiocache/redis'