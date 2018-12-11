from mrjob.job import MRJob
from mrjob.step import MRStep

class User(MRJob):
    def mapper(self, _, value):
        list_value = value.split('|')
        yield list_value[1], 1

    def reducer(self, key, value):
        yield key , sum(value)

if __name__ == "__main__":
    User.run();