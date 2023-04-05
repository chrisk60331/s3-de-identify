.DEFAULT_GOAL := build

format:
	isort --profile black .
	black -l79 .

test:
	pytest .

build:
	pip3 install -r requirements.txt
