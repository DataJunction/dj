# DataJunction Demo

This repo includes a DJ demo setup that launches and connects all of the DJ services as well as a jupyter lab server with example notebooks.

Clone this repository as well as the repositories for the other services.

```sh
git clone https://github.com/DataJunction/dj-demo.git
git clone https://github.com/DataJunction/dj.git
git clone https://github.com/DataJunction/dj-ui.git
git clone https://github.com/DataJunction/djqs.git
git clone https://github.com/DataJunction/djrs.git
git clone https://github.com/DataJunction/djms.git  # Optional
```

Change into the `dj-demo` directory and start up the docker environment.

```sh
cd dj-demo
docker compose up
```

To run the demo notebook, go to [http://localhost:8888/lab/](http://localhost:8888/lab/) and access it under
`/notebooks/Modeling the Roads Example Database.ipynb`. In the same directory, you can find other example notebooks.

**note:** The service urls are configured in the `.env` file. You can modify them to connect to remote services.
