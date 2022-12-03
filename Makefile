install:
	rm -rf build
	python setup.py install

test:
	python -m unittest discover -s merkl/tests -p 'test_*.py'

readme:
	cd docs && ./compile_readme

release:
	python setup.py sdist
	VERSION=$$(cat setup.py | grep "version" | grep -Eo "[0-9]\.[0-9]\.?[0-9]?") ; \
	twine upload dist/merkl-$$VERSION.tar.gz
