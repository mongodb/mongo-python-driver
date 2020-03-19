import copy
import json
import re
import sys
import traceback

from pymongo import MongoClient
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
from bson.py3compat import iteritems


def camel_to_snake(camel):
    # Regex to convert CamelCase to snake_case.
    snake = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake).lower()


def prepare_operation(operation_spec):
    target_name = operation_spec["object"]
    cmd_name = camel_to_snake(operation_spec["name"])
    arguments = operation_spec["arguments"]
    for arg_name in list(arguments):
        if arg_name == "sort":
            sort_dict = arguments[arg_name]
            arguments[arg_name] = list(iteritems(sort_dict))
    return target_name, cmd_name, arguments, operation_spec.get('result')


def run_operation(objects, prepared_operation):
    target_name, cmd_name, arguments, expected_result = prepared_operation

    if cmd_name.lower().startswith('insert'):
        # PyMongo's insert* methods mutate the inserted document, so we
        # duplicate it to avoid the DuplicateKeyError.
        arguments = copy.deepcopy(arguments)

    target = objects[target_name]
    cmd = getattr(target, cmd_name)
    result = cmd(**dict(arguments))

    if expected_result is not None:
        if isinstance(result, Cursor) or isinstance(result, CommandCursor):
            result = list(result)
        assert result == expected_result


def workload_runner(srv_address, workload_spec):
    # Do not modify connection string and do not add option of your own.
    client = MongoClient(srv_address)

    # Create test entities.
    database = client.get_database(workload_spec["database"])
    collection = database.get_collection(workload_spec["collection"])
    objects = {"database": database, "collection": collection}

    # Run operations
    operations = workload_spec["operations"]
    num_failures = 0
    num_errors = 0

    ops = [prepare_operation(op) for op in operations]
    try:
        while True:
            try:
                for op in ops:
                    run_operation(objects, op)
            except AssertionError:
                traceback.print_exc(file=sys.stdout)
                num_failures += 1
            except Exception:  # Don't catch Keyboard Interrupt here or you can never exit
                traceback.print_exc(file=sys.stdout)
                num_errors += 1
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt. Exiting gracefully.")
        print(
            json.dumps(
                {"numErrors": num_errors, "numFailures": num_failures}),
            file=sys.stderr)


if __name__ == '__main__':
    srv_address, workload_ptr = sys.argv[1], sys.argv[2]
    try:
        workload_spec = json.loads(workload_ptr)
    except json.decoder.JSONDecodeError:
        # We also support passing in a raw test YAML file to this
        # script to make it easy to run the script in debug mode.
        # PyYAML is imported locally to avoid ImportErrors on EVG.
        import yaml
        with open(workload_ptr, 'r') as fp:
            testspec = yaml.load(fp, Loader=yaml.FullLoader)
            workload_spec = testspec['driverWorkload']

    workload_runner(srv_address, workload_spec)
