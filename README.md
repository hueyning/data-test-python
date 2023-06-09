# Konfir Data Test

## The challenge

The aim of this exercise is to demonstrate your problem-solving skill and understanding of Python.

### Instructions

- Fork this repository
- Complete the exercises in `exercises/pyspark/` folder
- Each exercise has instructions in the file

### Additional terms

- We are big on TDD, so we expect you to complete this test using this approach.
- Credit will be given for approach, correctness, clean code, and testing.
- You can take as long as you like to complete the exercise, but for an indicative timescale we expect a senior
  developer can accomplish this in an hour.
- Please do not use any other libraries than those already included in the `Pipfile`.
- You may of course use any resources you like to assist you with specific techniques, syntax etc - but please do not
  just copy code.
- Please don't share this exercise with anyone else :)

### Instructions for running the tests

With pipenv:

- Run `pipenv install --dev` to install dependencies
- Run `pipenv run pytest` to run the tests

### Running Exercises

```py -m pipenv run python .\exercises\pyspark\01_data_transformation\data_transformation.py```

```py -m pipenv run python .\exercises\pyspark\02_data_aggregation\data_aggregation.py```

```py -m pipenv run python .\exercises\pyspark\03_data_join\data_join.py```

TODO:
- abstract out mock data to conf test - needs to be set as pyfixture
- implement I/O tests (if have time, need to figure out how to create temp dirs)
- safety checks (if have time, check if input data is valid/invalid test)
- safety checks 2 (if have time, check if input data has 0 row cnt, implies upstream error)