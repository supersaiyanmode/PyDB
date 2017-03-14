
class SmallUniqueKeyStore(object):
    def __init__(self, io, cls, initialize=False):
        self.io = io
        self.cls = cls

        if initialize:
           self.write_header()
        else:
            self.read_header()

    def insert(self, item):
        cur_data = [item]

        cur_data.append(item)
        cur_data.sort()
        self.io.seek(0)
        for item in cur_data:
            self.io.write(int_to_bytes(item), size=4)



