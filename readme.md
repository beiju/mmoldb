MMOLDB
======

MMOLDB is a (to-be-)queryable database for [MMOLB][mmolb]. It's currently 
hosted [here][mmoldb].

Running it yourself
-------------------

There are two options to run MMOLDB. One is through devcontainers using the 
file at `.devcontainers/devcontainer.json`. I do my development using 
[RustRover][rustrover]'s devcontainer support. I don't really understand how
devcontainers work myself so I won't give any more information than that.

The other option, which I can provide more support for, is using docker 
compose directly.

Note: It was previously necessary to add `-f docker-compose-prod.yml` to all
docker compose commands. That is no longer necessary, and in fact if you do
that you will get an error. You don't need any `-f` any more. (If for some
reason you really want one, now use `-f docker-prod/docker-compose.yml`.)

### A note on memory limits

MMOLDB is a data-intensive app and uses lots of RAM. It was written for 
systems with a minimum of 16GB of RAM+swap. Docker Desktop imposes strict
memory and swap limits based on your hardware, and users running MMOLDB
with Docker Desktop are likely to encounter Out-Of-Memory (OOM) errors.
These manifest as your containers crashing with exit code 137.

If you get OOM errors using Docker Desktop, try these steps in order of 
preference:

1. Switch to Docker CE (aka Docker Engine, aka "just plain Docker") if it's
   supported by your OS. Docker CE doesn't have memory limits by default.
2. [Increase Docker Desktop's memory and/or swap limit][docker-desktop-limits].
   Allowing up to 16GB of swap should be sufficient to avoid OOM errors, but
   will be slower than if you allowed Docker to use more memory. 
3. Reduce the amount of parallelism in MMOLDB's ingest process. Uncomment the
   `ingest_parallelism` setting in `Rocket.toml` and set it to a lower value
   than its default, which is the number of CPU cores that Docker is configured
   to use. Lower values for `ingest_parallelism` will result in slower ingests.

### If Jetbrains breaks the devcontainer again

Jetbrains' devcontainer implementation appears to be particularly fragile. 
These steps have allowed me to work around "Cannot build docker compose image"
problems in the past:

1. Comment out the entire `build` section in `.devcontainer/docker-compose.yml`
2. In its place, add an `image: {the-image}` definition. `{the-image}` is the
   image name used in the `FROM` declaration of .devcontainer/Dockerfile`.
3. Start the devcontainer. If this fix is successful, you should be able to 
   successfully build and enter the container but you'll get cargo errors as 
   soon as it opens.
4. Run all the `RUN` commands from the Dockerfile manually
5. Undo your changes to `.devcontainer/docker-compose.yml`

First-run Setup
---------------

1. Install [docker-compose][docker-compose]. The installation instructions 
   for docker-compose will also install docker for you.
2. Create the file `.db_admin_password` at the root of this repo, with a secure
   password as the file's contents. 

Running
-------

1. From the root `mmoldb` directory, start the database container: 
   `docker compose up -d db`. This command will run the database container in 
   the background (remove `-d` if you want it to run in the foreground). It 
   will build the container first if necessary.
2. Once the database is up (you can verify that it's up by running 
   `docker compose logs db` and looking for "database system is ready to accept 
   connections"), run the app: `docker compose up -d app`. As before, it will
   be built if necessary and `-d` makes it run in the background.
3. Visit localhost:42424 to see the MMOLDB status page.

Updating
--------

Updating typically requires rebuilding your database, which is currently a 
manual process:

1. `git pull` to fetch the changes
2. `docker compose down` to stop the running containers. Do _not_ add the `-v` 
   flag as was previously recommended -- that will remove the HTTP cache as 
   well as the database itself, and will make your database rebuild 
   significantly slower.
3. `docker compose build` to rebuild the container. If you don't do this you 
   won't see the changes.
4. `docker volume rm mmoldb_postgres-data` to remove the database volume.
5. Run the app again using the "Running" instructions.

Debug
-----

Aside from running in a devcontainer as described earlier, you can get some 
debug information by adding the line `log_level = "normal"` to the `[default]`
section of `Rocket.toml`. Log levels higher than `"normal"` will print debug
information from the libraries mmoldb uses, which is rarely useful. Note that
if you change `Rocket.toml` you will have to rebuild the container before the
change takes effect.

Exiting/Resetting
-----------------

Docker will continue running the db and app containers until you quit them. 
Depending on the docker configuration, it may even relaunch them after a 
reboot. To stop running them, run 
`docker compose -f docker-compose-prod.yml down`.

If you want to stop running the containers *and* delete the database, add the
`-v` flag:
`docker compose -f docker-compose-prod.yml down -v`

mmoldb is not designed to be a primary source for MMOLB data, so it should 
always be safe to delete the database. All that's required to rebuild it is
time and the availability of the actual primary sources.

Contributing
------------

Contributions are very welcome! There are guides on how to contribute specific
things in the `contributing` folder. Contributions that contribute additional
guides are also welcome.

### Desired contributions

Some contributions that are particularly desired are:

1. Improve the startup performance and memory usage of the HTTP cache used by
   `src/ingest/chron.rs`. This may involve replacing the key-value store 
   library.
2. Add another config file that allows overriding config values from 
   `Rocket.toml` and isn't checked into git. We're already customizing the
   Rocket configuration in `get_figment_with_constructed_db_url`.

[mmolb]: https://mmolb.com/
[mmoldb]: https://mmoldb.beiju.me/
[rustrover]: https://www.jetbrains.com/rust/
[docker-compose]: https://docs.docker.com/compose/
[docker-desktop-limits]: https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources