# Preparing environment

The easiest way is to setup everything with `uv`
```sh
uv tool install maturin
```

/conecta/conecta-python
```sh
uv sync
```

When doing changes in rust code, to see them reflected in the final python build run:
/conecta/conecta-python
```shell
maturin develop
```

Building production
```sh
maturin build
```

