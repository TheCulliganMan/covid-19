pull:
	python

format:
	isort -rc . && black .

notebook:
	poetry run jupyter notebook