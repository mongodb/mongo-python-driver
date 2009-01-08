import random
import sys
import traceback
import datetime
import re

from objectid import ObjectId
from dbref import DBRef

gen_target = 100
examples = 5

def lift(value):
    return lambda: value

def choose_lifted(list):
    return lambda: random.choice(list)

def choose(list):
    return lambda: random.choice(list)()

def gen_range(start, stop):
    return lambda: random.randint(start, stop)

def gen_int():
    return lambda: random.randint(-sys.maxint-1, sys.maxint)

def gen_float():
    return lambda: (random.random() - 0.5) * sys.maxint

def gen_boolean():
    return lambda: random.choice([True, False])

def gen_printable_char():
    return lambda: chr(random.randint(32, 126))

def gen_printable_string(gen_length):
    return lambda: "".join(gen_list(gen_printable_char(), gen_length)())

def gen_char(set=None):
    return lambda: chr(random.randint(0, 255))

def gen_string(gen_length):
    return lambda: "".join(gen_list(gen_char(), gen_length)())

def gen_unichar():
    return lambda: unichr(random.randint(1, 0xFFFF))

def gen_unicode(gen_length):
    return lambda: u"".join(gen_list(gen_unichar(), gen_length)())

def gen_list(generator, gen_length):
    return lambda: [generator() for _ in range(gen_length())]

# This is a little Mongo specific...
def gen_datetime():
    return lambda: datetime.datetime(random.randint(1970, 2037),
                                     random.randint(1, 12),
                                     random.randint(1, 28),
                                     random.randint(0, 23),
                                     random.randint(0, 59),
                                     random.randint(0, 59),
                                     random.randint(0, 999) * 1000)

def gen_dict(gen_key, gen_value, gen_length):
    def a_dict(gen_key, gen_value, length):
        result = {}
        for _ in range(length):
            result[gen_key()] = gen_value()
        return result
    return lambda: a_dict(gen_key, gen_value, gen_length())

def gen_regexp(gen_length):
    # TODO our patterns only consist of one letter.
    # this is because of a bug in CPython's regex equality testing, which I haven't
    # quite tracked down, so I'm just ignoring it...
    pattern = lambda: u"".join(gen_list(choose_lifted(u"a"), gen_length)())
    def gen_flags():
        flags = 0
        if random.random() > 0.5:
            flags = flags | re.IGNORECASE
        if random.random() > 0.5:
            flags = flags | re.MULTILINE
        return flags
    return lambda: re.compile(pattern(), gen_flags())

def gen_objectid():
    return lambda: ObjectId()

def gen_dbref():
    collection = gen_unicode(gen_range(0, 20))
    return lambda: DBRef(collection(), gen_objectid()())

def gen_mongo_value(depth):
    choices = [gen_unicode(gen_range(0, 50)),
               gen_string(gen_range(0, 1000)),
               gen_int(),
               gen_float(),
               gen_boolean(),
               gen_datetime(),
               gen_regexp(gen_range(0, 20)),
               gen_objectid(),
               gen_dbref(),
               lift(None),]
    if depth > 0:
        choices.append(gen_mongo_list(depth))
        choices.append(gen_mongo_dict(depth))
    return choose(choices)

def gen_mongo_list(depth):
    return gen_list(gen_mongo_value(depth - 1), gen_range(0, 10))

def gen_mongo_dict(depth):
    return gen_dict(gen_unicode(gen_range(0, 20)), gen_mongo_value(depth - 1), gen_range(0, 10))


def isnt(predicate):
    return lambda x: not predicate(x)


def check(predicate, generator):
    counter_examples = []
    for _ in range(gen_target):
        case = generator()
        try:
            if not predicate(case):
                counter_examples.append(repr(case))
        except:
            counter_examples.append("%r : %s" % (case, traceback.format_exc()))
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
