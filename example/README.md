Brief introduction of examples:

* Counter: A integer which can be added to by a given number in each request.
* Atomic: : A integer which supports exchange and compare_exchange operation.
* Block: A single block device supports randomly read/write concurrently.

# Build steps

```shell
example=counter|atomic|block
cd $example && cmake . && make
```

# Run Server

```sh
bash run_server.sh
```

* Default number of servers in the group is `3`,  changed by `--server_num` 
* Servers run on `./runtime/` which is resued,  add --clean to cleanup storage

# Run Client

```sh
bash run_client.sh
```

* Default concurrency of client is 1, changed by `--thread_num` 
* If server_num of run_server.sh has been changed, specify run_client to make it consistent.
* Add `--log_each_request` if you want detailed information of each request.

