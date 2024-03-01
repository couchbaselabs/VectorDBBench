ENV := env

all:
	export PYENV_ROOT="$$HOME/.pyenv" && \
	export PATH="$$PYENV_ROOT/bin:$$PATH" && \
	eval "$$(pyenv init --path)" && \
	pyenv local 3.11.8 && \
	virtualenv --quiet --python python3.11 ${ENV}
	# ${ENV}/bin/pip install ./vectordb-bench[couchbase]
	${ENV}/bin/pip install -e ".[couchbase]"

clean:
	rm -rf ${ENV} vectordb_bench.egg-info
	find . -name '*.pyc' -o -name '*.pyo' -o -name __pycache__ | xargs rm -fr

# env/bin/init_bench
# env/bin/cmd/run
unittest:
	PYTHONPATH=`pwd` python3 -m pytest tests/test_dataset.py::TestDataSet::test_download_small -svv
