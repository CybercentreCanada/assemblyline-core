

class MockFactory:
    def __init__(self, mock_type):
        self.type = mock_type
        self.mocks = {}

    def __call__(self, name, *args):
        if name not in self.mocks:
            self.mocks[name] = self.type(name, *args)
        return self.mocks[name]

    def __getitem__(self, name):
        return self.mocks[name]

    def __len__(self):
        return len(self.mocks)

    def flush(self):
        self.mocks.clear()


class TrueCountTimes:
    """A helper object that replaces a boolean.

    After being read a fixed number of times this object switches to false.
    """
    def __init__(self, count):
        self.counter = count

    def __bool__(self):
        self.counter -= 1
        return self.counter >= 0


class ToggleTrue:
    """A helper object that replaces a boolean.

    After every read the value switches from true to false. First call is true.
    """
    def __init__(self):
        self.next = True

    def __bool__(self):
        self.next = not self.next
        return not self.next
