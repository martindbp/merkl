install:
	python setup.py install

test:
	python -m unittest discover -s merkl/tests -p 'test_*.py'