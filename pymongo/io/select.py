try:
    import asyncio

    from greenletio import await_
    from greenletio.patcher import copy_globals

    def select(read_list, write_list, error_list, timeout=None):
        return wait_many(read_list, write_list, timeout) + ([],)

    def wait_many(read_list, write_list, timeout=None):
        readers = []
        writers = []
        event = asyncio.Event()

        def _reader_callback(fd):
            if fd not in readers:  # pragma: no branch
                readers.append(fd)
            event.set()

        def _writer_callback(fd):
            if fd not in writers:  # pragma: no branch
                writers.append(fd)
            event.set()

        for fd in read_list:
            asyncio.get_event_loop().add_reader(fd, _reader_callback, fd)
        for fd in write_list:
            asyncio.get_event_loop().add_writer(fd, _writer_callback, fd)
        try:
            await_(asyncio.wait_for(event.wait(), timeout))
        except asyncio.TimeoutError:  # pragma: no cover
            pass
        for fd in read_list:
            asyncio.get_event_loop().remove_reader(fd)
        for fd in write_list:
            asyncio.get_event_loop().remove_writer(fd)
        return readers, writers

except ImportError:
    import select
