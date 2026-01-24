## Creating protobuf files with "protobuf_gen-dockerfile" "buf.gen.yaml" "buf.yaml":

```sh
docker-compose --profile tools run --rm protobuf_gen
```
## Build the program

```sh
docker-compose build
```
and up the Docker container

```sh
docker-compose up
```

## Schedule a task on local `localhost:8081/schedule` with a JSON body:

- `"command"` command to imaginary execute
- `"must_be_execute_at"` when task must be execute
- `"scheduled_at"` representing the current time

To check task: `localhost:8081/status?task_id=<task-id>`


