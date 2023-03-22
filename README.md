# dj-demo

To set up and run the demo:

1. Check out the repositories for all DJ services:
   - [DJ Core](https://github.com/DataJunction/dj): `git clone https://github.com/DataJunction/dj.git`
   - [DJ Query Service](https://github.com/DataJunction/djqs): `git clone https://github.com/DataJunction/djqs.git`
   - (optional) [DJ Materialization Service](https://github.com/DataJunction/djms): `git clone https://github.com/DataJunction/djms.git`
2. Configure their locations in `.env`. The default setup assumes that they're in the immediate parent directory.
3. Bring up all default services using `docker compose up`. This will also run a Jupyter Lab instance with access to the DJ services.
4. To run the demo notebook, go to http://localhost:8888/lab/ and access it under `/notebooks/Modeling the Roads Example Database.ipynb`.
