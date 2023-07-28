try:
    from greenletio.green import socket as socket
    from greenletio.green import ssl as ssl
    from greenletio.green import threading as threading
    from greenletio.green import time as time
except ImportError:
    import socket
    import ssl
    import threading
    import time
