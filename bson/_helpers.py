


def setstate_slots(self, state):
    for slot, value in state.items():
        setattr(self, slot, value)

def mangle_name(n, prefix):
    return prefix + n

def getstate_slots(self):
    prefix = self.__class__.__name__
    if prefix not in ["Timestamp", "DBRef"]:
        prefix = ""
    else:
        prefix = "_"+prefix
    return {mangle_name(s, prefix): getattr(self, mangle_name(s, prefix)) for
            s in
            self.__slots__ if
            hasattr(self, mangle_name(s, prefix))}