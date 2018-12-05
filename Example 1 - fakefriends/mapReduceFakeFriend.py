from mrjob.job import MRJob
from mrjob.step import MRStep

class FakeFriend(MRJob):

    def mapper(self, _, line):
        data_virgula = line.split(',')
        key = data_virgula[2]
        value = data_virgula[3]
        yield key, int(value)

    def reducer(self, age, values):
        #values(só posso utilizar uma vez) vem como cursor  com os amigos
        #converto em um vetor
        friends = list(values)

        total = sum(friends)
        qtdElementos = len(friends)

        avg = total/qtdElementos

        yield age, avg


if __name__ == '__main__':
    FakeFriend.run()    