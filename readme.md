MMOLDB
======

MMOLDB is a (to-be-)queryable database for [MMOLB][mmolb]. It's currently 
hosted [here][mmoldb].

Running it yourself
-------------------

There are several ways to run MMOLB. They are listed in order of preference for
a beginner user. Advanced users should read the options and decide which is 
most applicable to them.

### Option 1: Pure docker-compose

This is recommended for users who do not intend to submit any pull requests, 
and just want to run a local copy; or for users who do intend to submit a
pull request but are not experienced in development or Docker.

#### Initial setup for the pure docker-compose setup

1. Install [docker-compose][docker-compose]. The installation instructions
   for docker-compose will also install docker for you. Docker CE (aka Docker
   Engine or Just Plain Docker) is recommended if your system supports it, but
   many systems do not. Docker Desktop will also work.
2. Check out this repo using git. If you're not planning to contribute changes,
   run `git clone https://github.com/beiju/mmoldb.git`. If you are planning to
   contribute, thank you! And please fork this repo and replace the URL in the 
   `git clone` command with the URL to your fork.
3. In the root directory of the repo you just checked out, create the file 
   `.db_admin_password`, with a secure password as the file's contents. Ensure 
   your `.db_admin_password` does not have any newlines except (optionally) at 
   the end of the file. One way to do this is to run the command 
   `openssl rand -base64 512 | tr -d '\n' > .db_admin_password` in the root 
   directory for this repo. (The `openssl` part generates the random password,
   but formats it with newlines; the `tr` removes the newlines; then the `>` 
   part directs the output to the correct file).

#### Running MMOLDB for the pure docker-compose setup

1. From the directory where you checked out the repo, run 
   `docker compose up -d`. The `-d` instructs docker-compose to run in the
   background. If this is your first time running `docker compose up` the
   container will be automatically built for you.
2. (Optional) Monitor progress using `docker compose logs -f`. The `-f`
   instructs docker-compose to keep watching for new logs; omit it if you
   only want to see a one-time snapshot of recent logs.
3. (Optional) Visit http://localhost:42424 to see the status page.

#### Updating MMOLDB for the pure docker-compose setup

1. `git pull` to fetch the changes
2. `docker compose down` to stop the running containers.
3. `docker compose build` to rebuild the container. If you don't do this you
   won't see the changes.
4. Optional: To delete all derived data (i.e. to have the database rebuild 
   itself from the cache), first ensure the `db` container is running but
   not the `ingest` container (`docker compose up db -d`). Then run 
   `docker compose exec -T db psql -U postgres -d mmoldb < ./delete_derived.sql`.
   This may fail if there are pending migrations (if you've pulled a new 
   version but not yet run it). If this happens, run the app using the 
   "Running" instructions, then quit it and redo this step. 
5. Run the app again using the "Running" instructions. Certain updates will
   rebuild some or all of the database even if you didn't delete the derived
   data.

### Option 2: Devcontainer

This is recommended for experienced developers who plan to contribute to
MMOLDB. It's more convenient for people who already use an IDE which supports
devcontainers.

#### Initial setup for the devcontainer setup

1. Install your preferred devcontainer-supporting IDE. I use RustRover and can
   only provide support for that setup, but if you use a different IDE and can
   support it yourself that's fine too.
2. Check out this repo using git. If you're not planning to contribute changes,
   run `git clone https://github.com/beiju/mmoldb.git`. If you are planning to
   contribute, thank you! And please fork this repo and replace the URL with
   the URL to your fork.
3. Copy `.env.example` to `.env`. This is required for the `diesel` command. 
   You can optionally set any desired settings in `.env`.

#### Running MMOLDB for the devcontainer setup

1. Open the devcontainer from `.devcontainer/devcontainer.json`. This should
   start the db container and the app devcontainer.
2. Within the app devcontainer, run the `mmmodb-app` target within the 
   `mmoldb-app` package. This starts the web front-end.
3. Within the app devcontainer, run the `mmmodb-ingest` target within the 
   `mmoldb-ingest` package. This starts the ingest task.

#### Updating MMOLDB for the pure docker-compose setup

Updating typically requires rebuilding your database, which is currently a
manual process:

1. `git pull` to fetch the changes
2. Stop both `mmoldb-app` and `mmoldb-ingest`.
3. Optional: To delete all derived data (i.e. to have the database rebuild
   itself from the cache), run the `./delete_derived.sql` on the `mmoldb` 
   database using your preferred SQL client.
   This may fail if there are pending migrations (if you've pulled a new
   version but not yet run it). If this happens, run the app using the
   "Running MMOLDB for the devcontainer setup" instructions, then quit it 
   and redo this step.
4. Run the app again using the "Running MMOLDB for the devcontainer setup" 
   instructions.

### Option 3: Double-buffered docker-compose

This setup can achieve much less downtime than the others, at the cost of 
increased disk space and increased manual work for setup and updates. It's how
the public version of MMOLDB is managed. You need some familiarity with docker
and .env files to follow these instructions.

#### Initial setup for the double-buffered docker compose setup

1. Install Docker following the instructions from step 1 of option 1.
2. Check out this repo _twice_ using git, in two different locations. These
   two checkouts must have different computed project names -- you can check
   their project names by running `docker compose config` and looking for the
   top-level `name`. By default the project name is the name of the containing 
   directory, but it can be overridden by the `COMPOSE_PROJECT_NAME` variable 
   in your .env file. Speaking of which,
3. In both checkouts, create a `.env` file. (It doesn't matter whether you copy 
   `.env.example` or create one from scratch). In the `.env` file, set the 
   following:
   - `MMOLDB_DOCKER_SUBNET`: Set one checkout to `172.142.0.0/17` and the other
     to `172.142.128.0/17`. This is because the production postgres container
     is configured to only accept connections on `172.142.0.0/16` for security
     reasons. These settings give each checkout half that range.
   - `MMOLDB_MAP_APP_PORT`: For each checkout set this to any unused, 
     non-protected port (e.g. 24201 on one checkout and 24202 on the other).
   - `MMOLDB_MAP_DB_PORT`: For each checkout set this to any unused,
     non-protected port (e.g. 24211 on one checkout and 24212 on the other).

#### Running MMOLDB for the double-buffered docker compose setup

1. Choose one of your checkouts to be the front buffer. It doesn't matter
   which, and it will swap every time you update.
2. (Optional) Change the ports on your chosen front-buffer checkout to the 
   "production" configuration. This is only required if you want a consistent
   port for the active web and postgres connections. If you're happy to switch
   which ports you're using every time you update, you can keep your 
   configuration unchanged.
3. From within the directory of your chosen checkout, follow the "Running 
   MMOLDB for the pure docker-compose setup" instructions.

#### Updating MMOLDB for the double-buffered docker compose setup

1. Your back buffer is whichever of your checkouts is not currently the front
   buffer. Ensure your back buffer is configured to use different ports from
   your front buffer (which should be currently running).
2. From within your back buffer's directory, follow the instructions from 
   "Updating MMOLDB for the pure docker-compose setup". During this step and 
   the next, your front buffer and back buffer will be running at the same 
   time.
3. Wait for the first ingest to complete. This can take some time. You can
   monitor progress using `docker compose logs`.
4. Once the first ingest is complete, swap your buffers. You may choose to
   keep the two checkouts' ports constant, in which case you swap your buffers 
   simply by changing which port you connect to. Or you may choose to keep your 
   front buffer's ports constant, in which case you must:
   1. Change the ports in your previous front buffer's `.env` file to the 
      non-active ports.
   2. Change the ports in your previous back buffer's `.env` file to the active 
      ports.
   3. Shut down your back buffer by running `docker compose down` in its 
      directory.
   4. Shut down your previous front buffer by running `docker compose down` in 
      your previous front buffer's directory. Active database queries will be
      aborted and your database will not be accessible until the next step is
      completed.
   5. Bring up your new front buffer (which is your previous back buffer) by
      running `docker compose up -d` in its directory. Once that finishes you
      should be able to connect to the new database and web app.

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
3. ~~Reduce the amount of parallelism in MMOLDB's ingest process. Uncomment the
   `ingest_parallelism` setting in `Rocket.toml` and set it to a lower value
   than its default, which is the number of CPU cores that Docker is configured
   to use. Lower values for `ingest_parallelism` will result in slower ingests.~~
   Currently unavailable.

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
reboot. To stop running them, run `docker compose down`.

mmoldb is not designed to be a primary source for MMOLB data, so it should 
always be safe to delete the database. All that's required to rebuild it is
time and the availability of the actual primary sources.

Contributing
------------

Contributions are very welcome! There are guides on how to contribute specific
things in the `contributing` folder. Contributions that contribute additional
guides are also welcome.

[mmolb]: https://mmolb.com/
[mmoldb]: https://mmoldb.beiju.me/
[rustrover]: https://www.jetbrains.com/rust/
[docker-compose]: https://docs.docker.com/compose/
[docker-desktop-limits]: https://docs.docker.com/desktop/settings-and-maintenance/settings/#resources