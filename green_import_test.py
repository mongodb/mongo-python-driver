from threading import Thread

from green_test import test_database

# from greenletio import patch_blocking
#
# with patch_blocking():
#     from threading import Thread

sync_thread = Thread(target=test_database)

sync_thread.start()
sync_thread.join()
