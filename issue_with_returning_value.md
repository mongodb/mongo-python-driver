## Problem description

The *insert* method is written with some errors that cause my interpreter with Pylance in VSCode complain about unreachable code after the *insert* method.
The reason is most likely the following:
```
This appears to be a bug in the pymongo library. The Collection class does not define a method named insert.
Rather, it defines insert_one and insert_many. It does define a __getattr__ method, which is annotated with a return type of Collection,
and it defines a __call__ method which is annotated with a return type of NoReturn.

That means db.order_dishes.insert evaluates to type Collection, and db.order_dishes.insert("") evaluates to type NoReturn, which means that
the call will never return, and any code after that point will be unreachable.
```


## Environment data
- Language Server version: Pylance language server 2022.11.30 (pyright 04a10583)
- OS and version: macOS 12.5, Apple M1
- Python version (& distribution if applicable, e.g. Anaconda): CPython 3.10.2


## Code Exmaple
```python
import pymongo


def connect_to_db():
    client = pymongo.MongoClient(["11.222.33.4:55555"], username="username", password="password")
    return client.app


# Create connection
db = connect_to_db()

# Insert data
db.order_dishes.insert("Some data to insert to MongoDB")  # ! <-- This is Point

print("Interpreter see this code is unreachable already :'(")
print(int("4" + "2"))
```


## Expected behaviour
Code should not be marked as unreachable


## Actual behaviour
Code is marked as unreachable


## Links
- https://github.com/microsoft/pylance-release/issues/3672

## Screen
<img width="936" alt="image" src="https://user-images.githubusercontent.com/78344396/203837530-53885275-1ca7-450d-9355-d8223741d28f.png">


