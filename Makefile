all: test

clean:
	@rm -f .flake .coverage

.flake:
	flake8-py3 .
	mypy async_sched.py
	touch .flake

isort:
	isort -rc

install:
	@sudo pip3 install -r requirements.txt

test: .flake
	@coverage3 run -m unittest -v test_async_sched
	@coverage3 report
	@coverage3 html
	@echo "# Open file://`pwd`/htmlcov/index.html"
