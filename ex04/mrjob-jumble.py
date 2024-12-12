from mrjob.job import MRJob
from mrjob.step import MRStep

#Jike Lu, jikelu
class MRJumbleSolver(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        if line.strip():
            word = line.strip().split()[0]
            is_jumbled = line.strip().endswith('?')

            sorted_word = ''.join(sorted(word.rstrip('?')))

            yield (sorted_word, (word, is_jumbled))

    def reducer(self, key, values):
        jumbled_words = []
        dictionary_words = []

        for word, is_jumbled in values:
            if is_jumbled:
                jumbled_words.append(word)
            else:
                dictionary_words.append(word)

        if jumbled_words:
            for jumble in jumbled_words:
                matches = [dw for dw in dictionary_words]
                if matches:
                    yield (jumble, matches)

if __name__ == '__main__':
    MRJumbleSolver.run()
