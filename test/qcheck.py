import random

gen_target = 100
examples = 5

def gen_range(start, stop):
    return lambda: random.randint(start, stop)

def char():
    return chr(random.randint(0, 255))

def gen_string(gen_length):
    return lambda: "".join(gen_list(char, gen_length))

def gen_list(generator, length):
    return [generator() for _ in range(length())]


def isnt(predicate):
    return lambda x: not predicate(x)


def check(predicate, generator):
    counter_examples = []
    for i in range(gen_target):
        case = generator()
        if not predicate(case):
            counter_examples.append(repr(case))
    return counter_examples

def check_unittest(test, predicate, generator):
    counter_examples = check(predicate, generator)
    if counter_examples:
        failures = len(counter_examples)
        message = "\n".join(["    -> %s" % f for f in counter_examples[:examples]])
        message = "found %d counter examples, displaying first %d:\n%s" % (failures,
                                                                          min(failures, examples),
                                                                          message)
        test.fail(message)
