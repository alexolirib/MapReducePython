from mrjob.job import MRJob
from mrjob.step import MRStep

class Heroes(MRJob):

    def mapper(self, _, line):
        data_space = line.split(' ')

        key = data_space[0]
        value = len(data_space) - 2
        yield key, value
    
    def reducer(self, hero_id, friends):        
        yield None, (sum(friends), hero_id)

    def reducer_sort(self, _, values):
        valuesList = list(values)
        sortedValues = sorted(valuesList)
        for sortedValue in sortedValues:
            yield sortedValue[1],  sortedValue[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(
                reducer=self.reducer_sort)
        ]


if __name__ == '__main__':
    Heroes.run();