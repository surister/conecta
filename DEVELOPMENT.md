# Project.

This is the tree of the project:

```
├── benchmarks          - Project to benchmark conecta and other libraries.
├── conecta-core        - Core parts of conecta.
├── conecta-python      - Python bindings (where we use conecta-core code to create Python-ready methods).
├── conecta-docs        - Documentation website.
└── scripts             - Random scripts.
```

# Preparing environment

```shell
git clone git@github.com:surister/conecta.git
```

The easiest way is to set up everything with `uv`
```sh
uv tool install maturin
```


```sh
# Path: /conecta/conecta-python
uv sync
```

When doing changes in rust code, to see them reflected in the final python build run:

```shell
# Path: /conecta/conecta-python
maturin develop
```

To install a release build (for performance testing) in the development environment:
```shell
# Path: /conecta/conecta-python
maturin develop -r
```

if installing tools is not permitted in your environment:

```shell
# Path: /conecta/conecta-python
uv run --with maturin maturin develop --uv
```

Building production
```sh
# Path: /conecta/conecta-python
maturin build
```

or 

```
uv run maturin build
```

